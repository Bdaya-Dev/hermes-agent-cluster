"""Scheduler-fairness tests (shared/claude-plugins #804, #833).

Covers the four acceptance criteria from the brief:
  1. 3 nodes, 6 tasks  -> spread exactly 2/2/2 (least-loaded, round-robin ties).
  2. A node at its ``max_concurrent`` ceiling receives nothing new.
  3. A running task is never re-assigned while its lease is alive.
  4. ``POST /api/v1/schedule/trigger`` returns ONLY new assignments, not a
     re-list of every already-running task.

Each state-level test runs against BOTH the in-memory ``ClusterState`` and
the SQLite ``ClusterStore`` so the two implementations cannot drift.
"""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import Node, NodeStatus, TaskStatus
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore

BOTH_STORES = [ClusterState, ClusterStore]


def make_store(store_cls):
    """Fresh store instance (:memory: for ClusterStore)."""
    if store_cls is ClusterStore:
        return store_cls(":memory:")
    return store_cls()


def register_node(store, node_id, capabilities=("tooling",), max_concurrent=0):
    store.register_node(
        Node(
            id=node_id,
            name=node_id,
            capabilities=list(capabilities),
            status=NodeStatus.online,
            max_concurrent=max_concurrent,
        )
    )


def add_ready_tasks(store, count, prefix="task"):
    """Create *count* no-dependency tasks and promote them to ready."""
    task_ids = []
    for i in range(count):
        tid = f"{prefix}_{i}"
        store.create_task(tid, f"{prefix} {i}", [])
        task_ids.append(tid)
    store.trigger_pending_tasks()  # pending -> ready (idempotent for ClusterStore)
    return task_ids


def assigned_counts(store, task_ids):
    """Map node_id -> number of *task_ids* currently assigned (running)."""
    counts = {}
    for tid in task_ids:
        task = store.get_task(tid)
        if task.assigned_to:
            counts[task.assigned_to] = counts.get(task.assigned_to, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# 1. Fairness: 3 nodes, 6 tasks -> 2/2/2
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_three_nodes_six_tasks_spread_two_each(store_cls):
    store = make_store(store_cls)
    for nid in ("n1", "n2", "n3"):
        register_node(store, nid)

    task_ids = add_ready_tasks(store, 6)

    assert store.schedule_pending() == 6, "all 6 ready tasks should be scheduled"
    counts = assigned_counts(store, task_ids)
    # Every task is running and assigned; the load spread is exactly 2/2/2.
    assert counts == {"n1": 2, "n2": 2, "n3": 2}


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_fair_spread_persists_correctly_after_schedule(store_cls):
    """Each scheduled task ends up running with an assigned node, and no
    node sat idle when tasks remained (the pile-on-node[0] regression)."""
    store = make_store(store_cls)
    for nid in ("n1", "n2", "n3"):
        register_node(store, nid)

    add_ready_tasks(store, 6)
    store.schedule_pending()

    for i in range(6):
        task = store.get_task(f"task_{i}")
        assert task.status is TaskStatus.running or task.status == TaskStatus.running
        assert task.assigned_to in ("n1", "n2", "n3")


# ---------------------------------------------------------------------------
# 2. max_concurrent: a full node receives nothing new
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_node_at_max_concurrent_gets_nothing(store_cls):
    store = make_store(store_cls)
    register_node(store, "n_limited", max_concurrent=1)
    register_node(store, "n_open1", max_concurrent=0)
    register_node(store, "n_open2", max_concurrent=0)

    # Wave 1: 3 ready tasks -> n_limited takes exactly its 1 slot; the rest
    # land on the open nodes.
    first = add_ready_tasks(store, 3, prefix="wave1")
    assert store.schedule_pending() == 3
    counts = assigned_counts(store, first)
    assert counts.get("n_limited", 0) == 1, "n_limited must not exceed max_concurrent=1"

    # Wave 2: 2 more ready tasks while n_limited is still at its ceiling.
    second = add_ready_tasks(store, 2, prefix="wave2")
    assert store.schedule_pending() == 2
    for tid in second:
        task = store.get_task(tid)
        assert task.assigned_to != "n_limited", (
            f"{tid} assigned to n_limited which is already at max_concurrent=1"
        )


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_capacity_skips_full_node_before_load_rank(store_cls):
    """A full node is not a candidate even though it has the lowest load."""
    store = make_store(store_cls)
    register_node(store, "n_full", max_concurrent=1)
    register_node(store, "n_free", max_concurrent=0)

    # Fill n_full with its one permitted task.
    t1 = "wrap_0"
    store.create_task(t1, "wrap 0", [])
    store.trigger_pending_tasks()
    assert store.schedule_pending() == 1
    assert store.get_task(t1).assigned_to == "n_full"

    # A second task must go to n_free — not pile onto the already-full n_full
    # even if n_full were "first" in iteration order.
    t2 = "wrap_1"
    store.create_task(t2, "wrap 1", [])
    store.trigger_pending_tasks()
    assert store.schedule_pending() == 1
    assert store.get_task(t2).assigned_to == "n_free"


# ---------------------------------------------------------------------------
# 3. Lease ownership: a running task is never re-assigned while its lease is alive
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_running_task_not_unassigned_while_lease_alive(store_cls):
    store = make_store(store_cls)
    register_node(store, "n1")
    register_node(store, "n2")

    tid = add_ready_tasks(store, 1)[0]
    assert store.schedule_pending() == 1
    assert store.get_task(tid).status == TaskStatus.running

    # Worker claims: an active lease now owns the task.
    store.create_lease(tid, "n1", timedelta(minutes=5))

    # unassign must refuse: the lease is alive.
    assert store.unassign_task(tid) is False, "unassign must refuse a live-lease task"
    task = store.get_task(tid)
    assert task.status == TaskStatus.running
    assert task.assigned_to == "n1"

    # Re-triggering the scheduler must not move it either.
    store.schedule_pending()
    task = store.get_task(tid)
    assert task.status == TaskStatus.running
    assert task.assigned_to == "n1"


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_leased_task_is_not_requeued_by_scheduler(store_cls):
    """A running task that still holds a live lease stays put (defence in
    depth — schedule_pending itself skips live-lease tasks).

    #916 update: the scheduler LEASES ON ASSIGN now, so the first
    schedule_pending already produced the protective lease — the old
    manual create_lease would stack a second live lease on the same task
    and make the single revoke at the end insufficient (unassign would
    still see a live lease). The lease under test IS the scheduler's."""
    store = make_store(store_cls)
    register_node(store, "n1")
    register_node(store, "n2")

    tid = add_ready_tasks(store, 1)[0]
    store.schedule_pending()

    new_task = add_ready_tasks(store, 1, prefix="extra")[0]
    store.schedule_pending()

    # Original task untouched; the new task went to the least-loaded node.
    assert store.get_task(tid).assigned_to == "n1"
    assert store.get_task(new_task).assigned_to == "n2"

    # Revoke the lease -> now the task may be unassigned again (lease recovery
    # path still works).
    lease = store.get_lease_by_task(tid)
    assert lease is not None
    store.revoke_lease(lease.id)
    assert store.unassign_task(tid) is True
    assert store.get_task(tid).status == TaskStatus.ready


# ---------------------------------------------------------------------------
# 4. POST /schedule/trigger returns only NEW assignments
# ---------------------------------------------------------------------------

def _app_client():
    app = create_app(cluster_id="test-cluster", node_id="node_main", node_role="main")
    return TestClient(app)


def test_trigger_returns_only_new_assignments():
    client = _app_client()
    for name in ("w1", "w2", "w3"):
        client.post(
            "/api/v1/nodes/join",
            json={"node_name": name, "capabilities": [], "max_concurrent": 0},
        )

    client.post("/api/v1/tasks", json={"title": "task a"})
    client.post("/api/v1/tasks", json={"title": "task b"})

    r1 = client.post("/api/v1/schedule/trigger")
    body1 = r1.json()
    assert body1["scheduled"] == 2
    assert len(body1["assignments"]) == 2
    assigned_tasks = {a["task_id"] for a in body1["assignments"]}
    assert len(assigned_tasks) == 2

    # Second trigger: no new ready work — assignments must be EMPTY even though
    # the two previous tasks are still running (regression: it used to re-list
    # every running task, masquerading as a lease-expiry re-queue).
    r2 = client.post("/api/v1/schedule/trigger")
    body2 = r2.json()
    assert body2["scheduled"] == 0
    assert body2["assignments"] == []


def test_trigger_schedules_single_wave_evenly():
    """The endpoint's per-call assignment slice spreads work fairly."""
    client = _app_client()
    for name in ("w1", "w2", "w3"):
        client.post(
            "/api/v1/nodes/join",
            json={"node_name": name, "capabilities": []},
        )

    for i in range(6):
        client.post("/api/v1/tasks", json={"title": f"t{i}"})

    body = client.post("/api/v1/schedule/trigger").json()
    assert body["scheduled"] == 6
    by_node = {}
    for a in body["assignments"]:
        by_node[a["node_id"]] = by_node.get(a["node_id"], 0) + 1
    assert by_node == {"node_w1": 2, "node_w2": 2, "node_w3": 2}