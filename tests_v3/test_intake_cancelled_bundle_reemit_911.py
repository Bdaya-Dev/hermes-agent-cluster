"""#911 — the intake grouper re-emits CANCELLED bundles. Measured twice live.

The specimen (hosted main, signed GET /api/v1/tasks, bayader-flutter#env/dev):

  12:18:53 task_8fb3b9e6107b0701  failed    issues=[#168,#216,#243,#244,#245]
  12:19:47 task_72dc7592c9e7a92d  cancelled issues=[#168,#216,#243,#244,#245]
           reason: "LFP-1 consolidation: #168/#243/#244 are IN FLIGHT as
                    task_5b4bea562ce82bc5; remainder (#216,#245) re-queued
                    as a follow-up delivery into the SAME MR"
  12:22:27 task_264f278634de4ee2  cancelled issues=[#168,#216,#243,#244,#245]
           (the re-emission of the one cancelled 2.5 min ago, cancelled
            again by the SAME operator action)
  13:12:11 task_65a16f253d92afcb  running   issues=[#168,#216,#244,#245]  <== 3rd re-emission

Every one of those five issues is STILL OPEN on GitLab (state=opened,
measured 2026-09-15), so the GitLab-side filters admit them all — the only
store-side veto is the cancel itself. Today _grouping_view's terminal rule
("FAILED / CANCELLED tasks release their issues") treats that intentional
cancel exactly like a transient failure, and the next cycle re-mints a
bundle for work the cancel declared already in flight. The consolidation is
undone and the lane re-spawns: LFP-1's saving leaks here the same way #909
pinned it for node placement — this time at the issue-membership level.

Fix shape (pinned by these tests): the cancel endpoint gains an explicit
re-queue INTENT that rides the task ROW (store-backed, restart-safe, the
same DEDUP-FIRST discipline as every other grouping guard):

  POST /tasks/{id}/cancel  {"reason": ..., "requeue": true|false}

  * requeue=true  — "reschedule this work": the cancelled row's issues are
    RELEASED and the next cycle re-groups them (the old behavior, made
    opt-IN). Disk-full drains / dead-node recoveries pass true.
  * requeue absent/false — an intentional cancel HOLDS its membership: the
    issues are not re-bundled by the grouper while the cancel stands. The
    2026-09-15 consolidations pass nothing — their members stay
    un-re-emitted and the 13:12 re-spawn cannot happen.
  * FAILED tasks keep releasing their issues unchanged: the split is
    INTENT, not terminality.
  * A held cancel is never permanently stranded: flipping the intent later
    (the setter is the operator's re-admission verb) or closing the issue
    on GitLab both evict it from the backlog.

RED on origin/main@c0833b8: no store exposes set_task_cancel_requeue (the
getattr pin fails AssertionError-shaped), _grouping_view releases cancelled
membership unconditionally, Task carries no cancel_requeue field, and
CancelTaskRequest drops `requeue` from the body silently (the #905
field-drop disease, pinned at the schema where a round-trip cannot see it).
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore
from hermes_cluster.routers import tasks as tasks_mod
from hermes_cluster.routers.intake import _grouping_view
from hermes_cluster.models import TaskStatus


BOTH_STORES = [ClusterState, ClusterStore]


def make_store(cls):
    return cls(":memory:") if cls is ClusterStore else cls()


def _bundle(store, task_id, lane="repo#main", issues=("p#1", "p#2"),
            status=TaskStatus.running):
    store.create_task(task_id, f"bundle {lane}", ["tooling"],
                      lane_key=lane, issues=list(issues))
    store.trigger_pending_tasks()
    store.set_task_status(task_id, status)
    return store.get_task(task_id)


def _requeue_setter(store):
    """getattr-pin (skill standard): on main this fails as 'seam missing'
    AssertionError, never AttributeError at import."""
    fn = getattr(store, "set_task_cancel_requeue", None)
    assert fn is not None, (
        "tasks must carry a persisted cancel-requeue intent "
        "(set_task_cancel_requeue) — without it the grouper cannot tell an "
        "intentional cancel from a transient one (#911)")
    return fn


# ---------------------------------------------------------------------------
# 1. _grouping_view: an intentional cancel holds its membership.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cls", BOTH_STORES)
def test_cancelled_bundle_blocks_its_issues_by_default(cls):
    st = make_store(cls)
    _bundle(st, "tA", issues=("p#1", "p#2"), status=TaskStatus.ready)
    _requeue_setter(st)("tA", False)          # what a bodyless /cancel records
    st.set_task_status("tA", TaskStatus.cancelled,
                       fail_reason="duplicate of running task")
    # base defect: cancelled releases -> the measured 13:12 re-emission
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" in blocked and "p#2" in blocked, (
        "a cancelled bundle with a recorded HOLD intent must not release "
        "its membership — the measured bayader-flutter cancel re-spawned "
        "4 of 5 members 50 minutes later")


@pytest.mark.parametrize("cls", BOTH_STORES)
def test_cancel_requeue_true_releases_membership(cls):
    st = make_store(cls)
    _bundle(st, "tB", issues=("p#1", "p#2"), status=TaskStatus.ready)
    _requeue_setter(st)("tB", True)
    st.set_task_status("tB", TaskStatus.cancelled,
                       fail_reason="disk full — reschedule elsewhere")
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" not in blocked and "p#2" not in blocked, (
        "requeue=true is the explicit 'reschedule this work' intent — the "
        "pre-#911 release behavior, made opt-IN")


@pytest.mark.parametrize("cls", BOTH_STORES)
def test_failed_bundle_still_releases(cls):
    """By-design control (green on main too): a FAILED sitting fixed nothing
    and is transient — its issues re-group. The fix must not strand."""
    st = make_store(cls)
    _bundle(st, "tF", issues=("p#1", "p#2"), status=TaskStatus.failed)
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" not in blocked and "p#2" not in blocked


@pytest.mark.parametrize("cls", BOTH_STORES)
def test_completed_bundle_still_blocks(cls):
    """By-design control: completed holds (already true on main)."""
    st = make_store(cls)
    _bundle(st, "tC", issues=("p#9",), status=TaskStatus.completed)
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#9" in blocked


@pytest.mark.parametrize("cls", BOTH_STORES)
def test_held_and_released_cancels_coexist(cls):
    """Two cancels, opposite intents: membership is held PER TASK — never
    by lane and never globally."""
    st = make_store(cls)
    _bundle(st, "tHold", issues=("p#1",), status=TaskStatus.ready)
    _bundle(st, "tRelease", issues=("p#2",), status=TaskStatus.ready)
    _requeue_setter(st)("tHold", False)
    _requeue_setter(st)("tRelease", True)
    for tid in ("tHold", "tRelease"):
        st.set_task_status(tid, TaskStatus.cancelled)
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" in blocked
    assert "p#2" not in blocked


def test_intent_survives_store_round_trip():
    """The cancel is the ACT; the intent is what main must remember across a
    restart — the whole DEDUP-FIRST store-backed discipline."""
    st = ClusterStore(":memory:")
    _bundle(st, "tD", issues=("p#7",), status=TaskStatus.ready)
    _requeue_setter(st)("tD", False)
    st.set_task_status("tD", TaskStatus.cancelled, fail_reason="consolidated")
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#7" in blocked
    row = st._conn.execute(
        "SELECT cancel_requeue FROM tasks WHERE id='tD'").fetchone()
    assert row is not None and int(row["cancel_requeue"]) == 0, (
        "tasks.cancel_requeue must be a persisted column (0 = hold, "
        "1 = release); a memory-only flag forgets the operator's intent")


def test_legacy_null_intent_releases(cls=ClusterStore):
    """Backfill policy: rows cancelled BEFORE this migration (or flipped
    store-direct without the setter) have no stored intent (NULL). NULL
    RELEASES — today's behavior — so history is not rewritten: the
    measured disk-full drains of 2026-09-14 keep re-grouping exactly as
    they did. Only cancels through the intent-aware path (the /cancel
    handler, which ALWAYS records — absent flag = explicit hold, the
    consolidation shape) can hold membership back."""
    st = make_store(cls)
    _bundle(st, "tOld", issues=("p#5",), status=TaskStatus.cancelled)
    # never touched the setter => column NULL
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#5" not in blocked, (
        "a NULL (pre-migration) cancel must keep today's release behavior — "
        "flipping history retroactively would strand the disk-full drains "
        "of last week")


# ---------------------------------------------------------------------------
# 2. The cancel endpoint writes the intent.
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    from hermes_cluster.app import create_app
    app = create_app()
    with TestClient(app) as c:
        yield c


def _seed_bundle(client, issues=("p#1", "p#2"), lane="repo#main"):
    st = tasks_mod._state  # the app factory wired routers with this store
    tid = "task_" + "%016x" % len(st.get_all_tasks())
    st.create_task(tid, "bundle", ["tooling"], lane_key=lane,
                   issues=list(issues))
    st.trigger_pending_tasks()
    return st, tid


def test_bodyless_cancel_holds_membership(client):
    st, tid = _seed_bundle(client)
    r = client.post(f"/api/v1/tasks/{tid}/cancel",
                    json={"reason": "duplicate of running task"})
    assert r.status_code == 200, r.text
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" in blocked and "p#2" in blocked, (
        "a consolidation cancel holds its membership by default (#911)")


def test_cancel_with_requeue_true_releases(client):
    st, tid = _seed_bundle(client)
    r = client.post(f"/api/v1/tasks/{tid}/cancel",
                    json={"reason": "node drained — reschedule elsewhere",
                          "requeue": True})
    assert r.status_code == 200, r.text
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" not in blocked and "p#2" not in blocked


def test_cancel_requeue_field_is_not_dropped():
    """The #905 disease pinned at the schema: SubmitTaskRequest silently
    dropped depends_on; CancelTaskRequest must not silently drop `requeue`
    — a flagged vs bodyless cancel that land identically is the same class
    of bug, and a router round-trip cannot observe a silent drop."""
    from hermes_cluster.models import CancelTaskRequest
    req = CancelTaskRequest.model_validate({"reason": "x", "requeue": True})
    assert getattr(req, "requeue", None) is True, (
        "CancelTaskRequest carries no requeue field — the body's value is "
        "dropped by Pydantic before the handler ever sees it")


def test_cancel_endpoint_records_flag_explicitly(client):
    """Endpoint-level: an explicit requeue:false cancel must hold — same as
    bodyless — proving the handler reads the field, not just its absence."""
    st, tid = _seed_bundle(client)
    r = client.post(f"/api/v1/tasks/{tid}/cancel",
                    json={"reason": "folded into the same MR",
                          "requeue": False})
    assert r.status_code == 200, r.text
    _view, blocked, _legacy = _grouping_view(st)
    assert "p#1" in blocked
