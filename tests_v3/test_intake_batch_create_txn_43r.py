"""Batch-create atomicity for grouped intake PHASE 2 (PR#43 review finding 3).

The plan-then-create restructure made a mid-cycle crash leave nothing behind
ONLY for the in-memory store: ``ClusterStore.create_task`` and
``PostgresStore.create_task`` each commit ONE transaction per task, so a crash
between two PHASE-2 writes strands a partial batch — exactly the half-
published state the restructuring was for, and the hosted main runs Postgres.

Fix pinned here: ``create_tasks_batch(plans)`` on ALL THREE stores opens ONE
transaction, inserts + promotes every bundle task, commits once; any
exception rolls back every row. The grouped cycle's PHASE 2 must be ONE batch
call and must not touch the dedup map until the batch committed.

TDDD-1 RED proof (measured on 91dace1 before the impl, same venv):
  7 failed, 1 skipped — every leg dies with
    AttributeError: 'ClusterState'/'ClusterStore'/'PostgresClusterStore'
    object has no attribute 'create_tasks_batch'
  (the API this PR's fix adds). Mutation proof: adding a per-plan
  ``conn.commit()`` inside the SQLite batch loop (reverting to the old
  per-task transaction boundaries) re-fails
  test_sqlite_batch_crash_mid_transaction_leaves_nothing_on_disk with
  'mid-batch crash left partial rows on disk' — the exact defect.
"""

import asyncio
import os
import sqlite3
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore
from hermes_cluster.routers import intake as intake_mod
from hermes_cluster.models import TaskStatus

from test_intake_grouping_762 import (  # noqa: E402
    _issue_json, _poller, _grouping_policy)


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    yield


def _plan(issue_ids, task_id=None, lane_key="p#main"):
    """One PHASE-2 plan in the batch API's call shape (the kwargs
    create_task already takes)."""
    tid = task_id or "task_" + "_".join(i.replace("/", "z").replace("#", "y")
                                        for i in issue_ids)
    return dict(
        task_id=tid,
        title=f"[grouped] {lane_key} ({len(issue_ids)} issues)",
        requires=["tooling"],
        priority=3,
        lane_key=lane_key,
        role="author",
        description="bundle brief",
        issues=list(issue_ids),
    )


# ---------------------------------------------------------------------------
# (b) In-memory — same test shape as the SQLite leg, stays green.
# ---------------------------------------------------------------------------

def test_in_memory_batch_writes_all_tasks_promoted():
    st = ClusterState()
    plans = [_plan(["a/x#1"]), _plan(["b/y#2"], lane_key="b#main")]
    tasks = st.create_tasks_batch(plans)
    assert [t.id for t in tasks] == [p["task_id"] for p in plans]
    assert all(t.status == TaskStatus.ready for t in tasks)
    assert tasks[1].issues == ["b/y#2"] and tasks[1].lane_key == "b#main"


def test_in_memory_batch_crash_mid_batch_leaves_zero_tasks():
    """An N-th insert raising must leave ZERO tasks — the in-memory analogue
    of the SQLite transaction leg. The batch publishes only after every plan
    is built, so a mid-batch raise cannot half-publish by construction."""
    st = ClusterState()
    boom = RuntimeError("injected: insert 3 exploded")
    plans = [_plan(["a/x#1"]), _plan(["b/y#2"]), _poison_plan("task_boom", boom)]
    with pytest.raises(RuntimeError):
        st.create_tasks_batch(plans)
    assert not st.get_all_tasks(), (
        "mid-batch raise left tasks behind: "
        f"{[(t.id, t.status) for t in st.get_all_tasks()]}")


class _poison_plan(dict):
    """A plan dict that explodes when its row gets rendered — mirrors a crash
    while assembling the N-th row, AFTER the earlier rows were built."""

    def __init__(self, task_id, exc):
        super().__init__(task_id=task_id, title="", requires=["tooling"],
                         priority=3, lane_key="", role="author",
                         description="", issues=[])
        self._exc = exc

    def __getitem__(self, key):
        if key in ("title", "description", "issues"):
            raise self._exc
        return super().__getitem__(key)


# ---------------------------------------------------------------------------
# (a) SQLite — fault injection INSIDE the transaction: a store whose N-th
#     insert raises leaves ZERO rows ON DISK. Reverting the implementation to
#     per-task commits turns this test RED (partial rows remain).
# ---------------------------------------------------------------------------

def test_sqlite_batch_crash_mid_transaction_leaves_nothing_on_disk(tmp_path):
    db = str(tmp_path / "cluster.db")
    store = ClusterStore(db_path=db)
    try:
        plans = [_plan(["a/x#1"]), _plan(["b/y#2"]), _plan(["c/z#3"])]

        boom = sqlite3.OperationalError("injected: disk on fire")

        class _FaultyConn:
            """Proxy over the real connection: the N-th INSERT raises BEFORE
            reaching sqlite. (sqlite3.Connection.execute is a read-only C
            attribute — it cannot be monkeypatched directly.)"""
            def __init__(self, real):
                object.__setattr__(self, "_real", real)

            def execute(self, sql, params=()):
                if "c/z#3" in repr(params):   # the THIRD insert explodes
                    raise boom
                return self._real.execute(sql, params)

            def __getattr__(self, name):
                return getattr(object.__getattribute__(self, "_real"), name)

        store._conn = _FaultyConn(store._conn)
        with pytest.raises(sqlite3.OperationalError):
            store.create_tasks_batch(plans)
        store._conn = object.__getattribute__(store._conn, "_real")

        # Read from a FRESH handle on the same file: the on-disk truth, not
        # the open connection's uncommitted view. Per-task commits leave the
        # first two rows here — that is the mutation this pin exists for.
        probe = ClusterStore(db_path=db)
        try:
            rows = probe.get_all_tasks()
            assert not rows, (
                f"mid-batch crash left partial rows on disk: "
                f"{[(t.id, t.status) for t in rows]}")
        finally:
            probe.close()
    finally:
        store.close()


def test_sqlite_batch_success_writes_all_promoted(tmp_path):
    """Happy leg: every plan becomes a ready task, field-for-field what
    create_task would produce — the batch changes the commit boundary only,
    not the per-row semantics."""
    db = str(tmp_path / "cluster.db")
    store = ClusterStore(db_path=db)
    try:
        plans = [_plan(["a/x#1", "a/x#2"], lane_key="a#main"),
                 _plan(["b/y#3"], lane_key="b#main")]
        tasks = store.create_tasks_batch(plans)
        assert [t.id for t in tasks] == [p["task_id"] for p in plans]
        assert all(t.status == TaskStatus.ready for t in tasks)
        on_disk = store.get_all_tasks()
        assert sorted(t.id for t in on_disk) == sorted(p["task_id"] for p in plans)
        for t, p in zip(tasks, plans):
            assert t.lane_key == p["lane_key"] and t.issues == p["issues"]
            assert t.description == p["description"] and t.role == "author"
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Wiring: the grouped cycle's PHASE 2 must be ONE batch call, and a crashing
# batch must leave no dedup entries pointing at vanished tasks.
# ---------------------------------------------------------------------------

def _two_repo_poller(st):
    routes = {
        "/groups/invora/issues": [
            _issue_json(463, "a", "invora/invora-flutter", 275),
            _issue_json(501, "b", "invora/invora-backend", 276),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: [],
    }
    cfg = {"enabled": True, "endpoint": "https://gitlab.test",
           "scopes": [{"type": "group", "path": "invora", "enabled": True}],
           "dedup_scope": "full", "grouping": _grouping_policy()}
    return _poller(st, routes, cfg)


def test_grouped_cycle_phase2_is_one_batch_call(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterStore(db_path=":memory:")
    poller = _two_repo_poller(st)

    calls = []
    real_batch = st.create_tasks_batch

    def spy_batch(plans):
        calls.append([p["task_id"] for p in plans])
        return real_batch(plans)

    monkeypatch.setattr(st, "create_tasks_batch", spy_batch)
    res = asyncio.run(poller.poll_once())
    assert res["created"], "two repos must bundle into two lane tasks"
    assert len(calls) == 1, (
        f"PHASE 2 must be ONE batch call, got {len(calls)} write(s)")
    assert len(calls[0]) == 2
    assert len([t for t in st.get_all_tasks() if t.issues]) == 2
    st.close()


def test_grouped_cycle_batch_crash_alarms_and_keeps_dedup_clean(clean_env, monkeypatch):
    """The batch raising (models the store crashing mid-commit): the poller
    must ALARM (last_errors, the PR's own instrument rule), create nothing,
    and — the load-bearing half — write NO dedup entries: a pid mapped to a
    task that doesn't exist makes the issue intake-invisible forever."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterStore(db_path=":memory:")
    poller = _two_repo_poller(st)

    boom = sqlite3.OperationalError("injected: crash mid PHASE 2")

    def batch_then_crash(plans):
        raise boom

    monkeypatch.setattr(st, "create_tasks_batch", batch_then_crash)
    res = asyncio.run(poller.poll_once())          # must NOT propagate
    assert res["created"] == []
    assert "grouped_cycle" in poller.last_errors, (
        "a crashing batch must alarm like any other grouped-cycle failure")
    assert not [t for t in st.get_all_tasks() if getattr(t, "issues", None)], (
        "a crashing batch must create no bundle tasks")
    assert not intake_mod._issue_dedup_to_task_id, (
        f"crashed batch left dedup entries: "
        f"{intake_mod._issue_dedup_to_task_id!r}")
    # The issues are NOT in _seen_keys either — the next cycle must retry.
    assert "invora/invora-flutter#463" not in poller._seen_keys
    st.close()


# ---------------------------------------------------------------------------
# (c) Postgres — this node cannot reach PG (the live leg skips without a DSN;
#     CI's python-tests job runs postgres:16). The claim travels on TWO legs:
#     the structural pin below (same single-_txn shape as the SQLite store)
#     and the CI-gated atomicity test.
# ---------------------------------------------------------------------------

def test_postgres_batch_shares_the_single_txn_shape():
    """Static pin without a server: PG create_tasks_batch MUST open exactly
    ONE _txn() with the create loop INSIDE its body, executing through the
    transaction connection — the same structure the SQLite store pins
    behaviourally. (Statements on the pool instead of the txn connection
    auto-commit on checkout and the transaction would be decorative.)"""
    import inspect
    from hermes_cluster.state.postgres_store import PostgresClusterStore

    src = inspect.getsource(PostgresClusterStore.create_tasks_batch)
    assert "async with self._txn() as conn:" in src, (
        "PG batch must open ONE transaction (parity with the SQLite _tx shape)")
    assert src.count("async with self._txn()") == 1, (
        "exactly one _txn per batch call — per-plan transactions are the "
        "defect this API exists to remove")
    assert src.index("async with self._txn") < src.index("for plan in plans"), (
        "the create loop must sit INSIDE the single _txn block")
    assert "conn.execute" in src


def test_postgres_batch_atomicity(pg_dsn):
    """Live leg — runs in CI (postgres:16 service); skips here without
    HERMES_CLUSTER_PG_TEST_DSN, which is WHY the structural pin exists.
    Fault injection without monkeypatching the (Cython) asyncpg Connection:
    the SECOND plan's title is None — a server-side NOT NULL violation that
    lands AFTER the first INSERT executed on the transaction connection. A
    per-task-commit implementation (the old create_task shape, which the
    mutation check pins on SQLite) would have already committed row one;
    the single-_txn batch must roll everything back. Then the happy batch
    promotes everything."""
    import asyncpg.exceptions as pgexc
    from hermes_cluster.state.postgres_store import SyncPostgresStore

    store = SyncPostgresStore(dsn=pg_dsn)
    try:
        store.truncate_all()
        good = _plan(["a/x#1"])
        bad = _plan(["b/y#2"])
        bad["title"] = None          # NOT NULL violation, mid-loop
        with pytest.raises(pgexc.NotNullViolationError):
            store.create_tasks_batch([good, bad])

        rows = store.get_all_tasks()
        assert not rows, (
            f"mid-batch crash left partial rows in PG: "
            f"{[(t.id, t.status) for t in rows]}")

        plans = [_plan(["a/x#1"]), _plan(["b/y#2"]), _plan(["c/z#3"])]
        tasks = store.create_tasks_batch(plans)
        assert len(tasks) == 3
        assert all(t.status == TaskStatus.ready for t in tasks)
        on_disk = store.get_all_tasks()
        assert sorted(t.id for t in on_disk) == sorted(p["task_id"] for p in plans)
    finally:
        store.close()
