"""#905 — POST /api/v1/tasks silently dropped depends_on.

Measured 2026-09-14 on the hosted main: submitting a verdict-gated landing
task with `"depends_on": ["task_559e323aa3ce198c"]` returned 200, but the
created task carried `depends_on: []` and went straight to `ready` — so the
landing ran immediately, found no verdict, STOPped, and the PR stranded.

Root cause: `SubmitTaskRequest` has no `depends_on` field, so Pydantic
silently dropped it, and every backend's `create_task` INSERTs a hardcoded
`'[]'` and then unconditionally promotes pending -> ready ("create_task
always creates with empty depends_on, so promote immediately"). The
dependency ENGINE (trigger_pending_tasks, _trigger_downstream, set_dependencies
demotion) was fully built — only the create handler never fed it.

TDDD-1 fence, per the issue's own acceptance text: "create A, create B with
depends_on=[A] -> B not dispatchable until A completes (RED today)". Plus the
store-leg twins so the fix cannot be backend-skewed: the hosted main runs the
Postgres store and SQLite is the local default, and the drop lived in BOTH.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import TaskStatus


@pytest.fixture
def client():
    app = create_app(cluster_id="test-cluster", node_id="test-node", node_role="main")
    return TestClient(app)


def _submit(client, title, **extra):
    resp = client.post("/api/v1/tasks", json={"title": title, "requires": [], **extra})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _register_worker(client):
    """An online capable node: if a task ever reaches `ready`, the scheduler
    WILL dispatch it — so 'still not dispatched' is a real claim, not an
    artifact of an empty cluster."""
    resp = client.post("/api/v1/nodes/join", json={
        "node_name": "worker-905",
        "capabilities": ["coding"],
    })
    assert resp.status_code == 200
    return resp.json()["node_id"]


# ---------------------------------------------------------------------------
# The API leg (the issue's named tests_v3 case) — in-memory ClusterState.
# ---------------------------------------------------------------------------

def test_create_with_depends_on_holds_task_not_ready(client):
    _register_worker(client)
    reviewer = _submit(client, "reviewer task A")
    assert reviewer["status"] == "ready"  # control: no deps -> promotes as before

    landing = _submit(client, "landing task B", depends_on=[reviewer["id"]])
    # RED today: status == "ready" and depends_on == [] — the field was dropped.
    assert landing["status"] == "pending", (
        "landing task jumped to ready despite an unmet dependency (#905)"
    )
    assert landing["depends_on"] == [reviewer["id"]]

    # A full schedule kick must not dispatch it either.
    client.post("/api/v1/schedule/trigger", json={})
    held = client.get(f"/api/v1/tasks/{landing['id']}").json()
    assert held["status"] == "pending"
    assert held["assigned_to"] is None
    assert held["depends_on"] == [reviewer["id"]]  # exposed on GET, per the issue

    # The reviewer completes -> the landing becomes dispatchable.
    done = client.post(f"/api/v1/tasks/{reviewer['id']}/complete", json={})
    assert done.status_code == 200
    freed = client.get(f"/api/v1/tasks/{landing['id']}").json()
    assert freed["status"] in ("ready", "assigned", "running"), (
        "dependency met but landing never promoted (or the scheduler never "
        f"saw it): {freed['status']}"
    )


def test_create_rejects_unknown_dependency_id(client):
    resp = client.post("/api/v1/tasks", json={
        "title": "task with a dangling dep",
        "requires": [],
        "depends_on": ["task_0000000000000000"],
    })
    assert resp.status_code == 422, (
        "unknown dependency ids must fail loud — a silently-unsatisfiable dep "
        "is a task that never runs (#905: validate ids exist)"
    )


def test_create_without_depends_on_still_promotes(client):
    t = _submit(client, "no deps at all")
    assert t["status"] == "ready"
    assert t["depends_on"] == []


# ---------------------------------------------------------------------------
# Store legs: the drop lived in create_task itself in BOTH PERSISTENT
# backends (hardcoded '[]' INSERT + unconditional pending->ready promote).
# The hosted main runs Postgres; SQLite is the local default. Keep them from
# drifting apart. In-memory ClusterState leaves creation pending and lets the
# router's trigger kick it — that path is covered by the API leg above.
# ---------------------------------------------------------------------------

def _store_depends_on_cycle(store):
    # The persistent stores promote dep-free rows inside create_task.
    a = store.create_task("task_a_905", "reviewer A", [])
    assert a.status == TaskStatus.ready

    b = store.create_task("task_b_905", "landing B", [], depends_on=["task_a_905"])
    assert b.status == TaskStatus.pending, (
        "create_task promoted a dependency-held task to ready (#905)"
    )
    assert b.depends_on == ["task_a_905"]
    # kick: must stay held
    assert store.trigger_pending_tasks() == 0
    assert store.get_task("task_b_905").status == TaskStatus.pending

    store.set_task_status("task_a_905", TaskStatus.completed)
    assert store.trigger_pending_tasks() == 1
    assert store.get_task("task_b_905").status == TaskStatus.ready
    # get_dependents feeds the completion cascade; prove the stored field is
    # real JSON, not just the returned model.
    assert store.get_dependents("task_a_905") == ["task_b_905"]


def test_sqlite_store_create_task_honors_depends_on(tmp_path):
    from hermes_cluster.state.cluster_store import ClusterStore

    store = ClusterStore(db_path=str(tmp_path / "t905.db"))
    try:
        _store_depends_on_cycle(store)
    finally:
        store.close()


def test_postgres_store_create_task_honors_depends_on(pg_store):
    _store_depends_on_cycle(pg_store)


# ---------------------------------------------------------------------------
# Batch leg: the grouped-intake PHASE-2 writer had the SAME drop as
# create_task in both persistent stores (hardcoded '[]' INSERT + promote of
# every batch id). The in-memory twin already honored per-plan depends_on —
# the stores must not skew from it.
# ---------------------------------------------------------------------------

def _store_batch_cycle(store):
    plans = [
        dict(task_id="task_r905", title="reviewer R", requires=[]),
        dict(task_id="task_l905", title="landing L", requires=[],
             depends_on=["task_r905"]),
    ]
    created = store.create_tasks_batch(plans)
    by_id = {t.id: t for t in created}
    assert by_id["task_r905"].status == TaskStatus.ready
    assert by_id["task_l905"].status == TaskStatus.pending
    assert by_id["task_l905"].depends_on == ["task_r905"]
    assert store.trigger_pending_tasks() == 0
    store.set_task_status("task_r905", TaskStatus.completed)
    assert store.trigger_pending_tasks() == 1
    assert store.get_task("task_l905").status == TaskStatus.ready


def test_sqlite_store_batch_honors_depends_on(tmp_path):
    from hermes_cluster.state.cluster_store import ClusterStore

    store = ClusterStore(db_path=str(tmp_path / "t905b.db"))
    try:
        _store_batch_cycle(store)
    finally:
        store.close()


def test_postgres_store_batch_honors_depends_on(pg_store):
    _store_batch_cycle(pg_store)


# ---------------------------------------------------------------------------
# The landing-task contract (#902's mechanical fix is what #905 gates): the
# reviewer hand-off brief tells the reviewer to SUBMIT the landing task — with
# the field that #902's own text says must be honored. A brief that names
# `depends_on` while the create handler drops it is a half-landed contract.
# ---------------------------------------------------------------------------

def test_github_landing_dispatch_carries_depends_on():
    """The #902 landing-task text in reviewer_handoff_brief must bind the
    landing to the reviewed PR via the SAME submit call that carries
    `depends_on` — and the create handler must keep what it sends."""
    from hermes_cluster.core.intake_grouping import (
        BundlePlan,
        reviewer_handoff_brief,
    )

    bundle = BundlePlan(
        lane_key="infra!pr-308",
        iids=[902],
        project_paths=["Bdaya-Dev/bdaya-website-infra"],
        priority=0,
        issue_ids=["Bdaya-Dev/bdaya-website-infra#902"],
        reason="test",
    )
    brief = reviewer_handoff_brief(
        bundle,
        "https://github.com/Bdaya-Dev/bdaya-website-infra/pull/308",
        "c28e4b90" * 2 + "12345678",
    )
    assert "kanban_cluster_submit" in brief  # #902: reviewer self-submits landing
    assert "landing" in brief.lower()
    # #905: the dispatch text must actually carry the gate — a landing task
    # without depends_on runs before any verdict exists.
    assert "depends_on" in brief, (
        "landing-task dispatch text omits depends_on — the gate #905 honors "
        "is never wired at the protocol level"
    )
    # The create handler must expose the dependency field the dispatch docs name.
    from hermes_cluster.models import SubmitTaskRequest

    assert "depends_on" in SubmitTaskRequest.model_fields, (
        "SubmitTaskRequest does not accept depends_on — a landing task told "
        "to wait on a verdict would run immediately (#905)"
    )
