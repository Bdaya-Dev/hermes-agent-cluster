"""#916 — recovery is blind to every scheduler-assigned task.

Measured live 2026-09-15T19:30Z: GET /api/v1/leases returned [] with NINE
tasks running. The lease is created at exactly ONE site (routers/tasks.py,
the /claim PULL path); ``schedule_pending`` (the PUSH path every fleet task
actually rides) assigns a task, flips it running, and creates NO lease —
while every recovery path (Revoker, Rescheduler wiring, the expiry scanner)
enumerates work via ``get_active_leases()``. A scheduler-assigned task is
therefore invisible to recovery FOREVER: a wedged push task holds its lane
under LFP-1 and nothing can end the sitting.

This file pins the fix (refs shared/claude-plugins#916):

  1. schedule_pending creates a lease when it assigns (in-memory
     ClusterState, SQLite ClusterStore, and the PG store — the same three
     surfaces the #829 parity suite drives);
  2. the full reclaim loop: assign -> worker goes silent -> TTL expires ->
     the recovery scan reclaims the sitting (lease revoked, task
     rescheduled) -> the LFP-1 lane queue unblocks;
  3. the worker-side renewal seam: while a spawn is alive its lease must
     be extended over an *identified* endpoint (a bare GET /api/v1/leases
     cannot find a scheduler-created lease because the executor's node id
     is the bare name while nodes are registered ``node_<name>``);
  4. VISIBILITY: a running task whose lease is absent or expired is
     countable — /api/v1/cluster/status carries ``unleased_running`` plus
     per-task detail, and the auto-recovery scan logs a WARNING naming
     each offender (a monitor keys on either).

RED PROOF (skill standard): on origin/main every non-control test fails on
the DEFECT assertion (lease list empty, field absent), not ImportError —
new symbols are imported INSIDE the tests that pin them.

BY-DESIGN CONTROLS (already pass on main; the #804/#833 invariants this
fix must not break):
  * a live lease blocks re-assignment and blocks unassign_task;
  * an expired lease is NOT protection: the scheduler may (re-)assign;
  * the /claim path keeps creating exactly one lease;
  * recovery never revives a terminal task (N2).
"""

import logging
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import (
    LeaseStatus,
    Node,
    NodeStatus,
    TaskStatus,
)
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _mem_store():
    return ClusterState()


def _register_online(store, node_id, capabilities=("tooling",), max_concurrent=0):
    store.register_node(Node(
        id=node_id, name=node_id, capabilities=list(capabilities),
        status=NodeStatus.online, max_concurrent=max_concurrent,
    ))


def _make_ready(store, task_id, requires=(), lane_key=""):
    store.create_task(task_id, f"task {task_id}", list(requires),
                      lane_key=lane_key)
    store.trigger_pending_tasks()


def _active_for(store, task_id):
    return [l for l in store.get_active_leases() if l.task_id == task_id]


def _expire_lease(store, task_id):
    """Force the task's active lease into the past (the TTL elapsed)."""
    now = datetime.utcnow()
    if hasattr(store, "_leases"):  # in-memory ClusterState
        with store._leases_lock:
            for l in store._leases.values():
                if l.task_id == task_id and l.status == LeaseStatus.active:
                    l.expires_at = now - timedelta(seconds=1)
        return
    # SQLite ClusterStore
    conn = getattr(store, "_conn", None)
    if conn is not None:
        with store._tx() as c:
            c.execute(
                "UPDATE leases SET expires_at = ? "
                "WHERE task_id = ? AND status = ?",
                ((now - timedelta(seconds=1)).isoformat(), task_id,
                 LeaseStatus.active.value),
            )
        return
    # PG SyncPostgresStore facade (test-only poking; mirrors what the TTL
    # does in real time).
    inner = store._store
    loop = store._bridge

    async def _do():
        await inner._exec_status_rowcount(
            "UPDATE leases SET expires_at = $1 "
            "WHERE task_id = $2 AND status = $3",
            now - timedelta(seconds=1), task_id, LeaseStatus.active.value,
        )
    loop.run(_do())


@pytest.fixture()
def app_client():
    app = create_app(cluster_id="c916", node_id="m", node_role="main")
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. schedule_pending creates a lease when it assigns — ALL THREE BACKENDS
# ---------------------------------------------------------------------------

def test_inmem_schedule_pending_creates_lease():
    """The defect: the push path assigns and runs WITHOUT a lease, so the
    lease table — the ONLY thing recovery enumerates — stays empty."""
    store = _mem_store()
    _register_online(store, "node_w1")
    _make_ready(store, "t1")

    assert store.schedule_pending() == 1
    task = store.get_task("t1")
    assert task.status == TaskStatus.running

    active = _active_for(store, "t1")
    assert len(active) == 1, (
        "schedule_pending must create a lease on assign: a running task "
        f"with no lease is invisible to recovery forever (#916); got {active!r}"
    )
    assert active[0].node_id == "node_w1"


def test_sqlite_schedule_pending_creates_lease():
    store = ClusterStore(":memory:")
    try:
        _register_online(store, "node_w1")
        _make_ready(store, "t1")

        assert store.schedule_pending() == 1
        active = _active_for(store, "t1")
        assert len(active) == 1, (
            "SQLite store: schedule_pending must create a lease on assign "
            f"(#916); got {active!r}"
        )
    finally:
        store.close()


def test_pg_schedule_pending_creates_lease(pg_dsn):
    from hermes_cluster.state.postgres_store import SyncPostgresStore
    store = SyncPostgresStore(pg_dsn)
    try:
        _register_online(store, "node_w1")
        _make_ready(store, "t1")
        assert store.schedule_pending() == 1
        active = _active_for(store, "t1")
        assert len(active) == 1, (
            "PG store: schedule_pending must create a lease on assign "
            f"(#916); got {active!r}"
        )
    finally:
        store.close()


def test_schedule_pending_reentry_does_not_stack_leases():
    """Idempotence: a second schedule_pending tick must NOT stack a second
    live lease on a still-leased task, and must not re-assign it (#833)."""
    store = _mem_store()
    _register_online(store, "node_w1")
    _register_online(store, "node_w2")
    _make_ready(store, "t1")

    assert store.schedule_pending() == 1
    assert store.schedule_pending() == 0, "leased running task must not re-assign"
    assert len(_active_for(store, "t1")) == 1, (
        "a still-live scheduler lease must not be duplicated by the next tick"
    )


# ---------------------------------------------------------------------------
# 2. the reclaim loop: assign -> silent worker -> expiry -> recovery
# ---------------------------------------------------------------------------

def test_recovery_reclaims_silent_push_task_and_unblocks_lane():
    """The end-to-end #916 scenario in miniature (the live incident's
    dominant shape — the worker's node goes silent):

      * two tasks on ONE lane key (LFP-1: one sitting per lane);
      * the scheduler pushes task 1 to the node; the node then goes dark
        (no heartbeat, no renewal, no completion) — watchdog takes it
        offline while its push lease sits expired;
      * the recovery scan runs: the dead lease is revoked and the task
        cannot be re-homed (no online node) -> FAILED off its lane seat;
      * the LFP-1 lane unblocks: a node coming back schedules task 2.

    On main NOTHING of this happens: the wedged task holds the lane and
    its queue forever (measured: 340 minutes on hermes-agent-cluster#main).
    """
    from hermes_cluster.models import NodeStatus
    from hermes_cluster.recovery.manager import RecoveryManager

    store = _mem_store()
    _register_online(store, "node_w1")
    mgr = RecoveryManager(store)

    _make_ready(store, "t1", lane_key="repo#main")
    _make_ready(store, "t2", lane_key="repo#main")

    # tick 1: t1 gets the lane, t2 stays ready (LFP-1 cardinality guard).
    assert store.schedule_pending() == 1
    assert store.get_task("t2").status == TaskStatus.ready

    # t1 must carry a lease for any of this to be visible to recovery.
    assert len(_active_for(store, "t1")) == 1, (
        "precondition: schedule_pending must have leased t1 (#916 push lease)"
    )

    # the worker goes dark: TTL lapses, the watchdog takes the node offline.
    _expire_lease(store, "t1")
    store.set_node_status("node_w1", NodeStatus.offline)

    # the recovery scan sees the dead sitting and reclaims it.
    result = mgr.detect_expired_leases()
    assert "node_w1" in result["recovered_nodes"], (
        f"scan must trigger recovery for the node whose lease died: {result!r}"
    )
    assert len(_active_for(store, "t1")) == 0, (
        "the wedged task's lease must no longer be active after recovery"
    )
    t1 = store.get_task("t1")
    assert t1.status == TaskStatus.failed and t1.assigned_to is None, (
        f"wedged task must be off its lane seat after reclaim: {t1!r}"
    )

    # the lane unblocks: a node coming back schedules t2 — leased too.
    store.set_node_status("node_w1", NodeStatus.online)
    assert store.schedule_pending() >= 1, "lane queue must unblock after reclaim"
    t2 = store.get_task("t2")
    assert t2.status == TaskStatus.running
    assert len(_active_for(store, "t2")) == 1, "the new sitting is leased too"


def test_recovery_reclaims_silent_push_task_sqlite():
    """Same loop on the SQLite surface (the hosted default before #829
    postgres; both stores must behave identically for recovery)."""
    from hermes_cluster.models import NodeStatus
    from hermes_cluster.recovery.manager import RecoveryManager

    store = ClusterStore(":memory:")
    try:
        _register_online(store, "node_w1")
        mgr = RecoveryManager(store)
        _make_ready(store, "t1", lane_key="repo#main")
        _make_ready(store, "t2", lane_key="repo#main")

        assert store.schedule_pending() == 1
        assert len(_active_for(store, "t1")) == 1, (
            "precondition: SQLite schedule_pending must lease on assign"
        )
        _expire_lease(store, "t1")
        store.set_node_status("node_w1", NodeStatus.offline)
        result = mgr.detect_expired_leases()
        assert "node_w1" in result["recovered_nodes"], (
            f"scan must trigger recovery on SQLite too: {result!r}"
        )
        assert len(_active_for(store, "t1")) == 0
        t1 = store.get_task("t1")
        assert t1.status == TaskStatus.failed, (
            f"wedged task must be terminal after reclaim: {t1!r}"
        )
        store.set_node_status("node_w1", NodeStatus.online)
        assert store.schedule_pending() >= 1
        assert store.get_task("t2").status == TaskStatus.running
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 2b. TTL design pin (#916 ask 1: "consider whether the TTL should differ
# from the claim path's, and say why")
# ---------------------------------------------------------------------------

def test_scheduler_lease_ttl_is_a_named_constant_equal_to_claim_default():
    """The push-path TTL is the SAME grace as the claim path's default:
    one proof-of-life window, one renewal machinery (the executor poll
    loop renews every live spawn, well inside it). The decision is
    argued in the PR body; this test pins that it is a NAMED constant —
    if the wedge-vs-renew contract ever changes, this is the place that
    has to say so, not an inline timedelta buried in three stores.

    getattr-pinned so the red on main is the DEFECT assertion (constant
    missing), not an ImportError."""
    from hermes_cluster.core import scheduler as sched
    from hermes_cluster.models import LeaseConfig

    ttl = getattr(sched, "SCHEDULER_ASSIGNED_TTL", None)
    assert ttl is not None, (
        "the scheduler-assigned lease TTL must be a named constant in "
        "core/scheduler (shared by all three stores), not a literal"
    )
    assert ttl == LeaseConfig().ttl, (
        "push TTL == claim default TTL is the design decision; changing it "
        "must be deliberate and re-argued"
    )
    assert ttl.total_seconds() > 0


# ---------------------------------------------------------------------------
# 3. worker-side renewal reaches the scheduler-created lease
# ---------------------------------------------------------------------------

def _bare_executor():
    from hermes_cluster.core.agent_executor import (
        AgentExecutor, AgentExecutorConfig,
    )
    cfg = AgentExecutorConfig(poll_interval=0.05)
    return AgentExecutor(
        config=cfg,
        node_id="pc_worker",
        cluster_endpoint="http://main.invalid:8787",
    )


def test_find_lease_tolerates_the_node_spelling_gap():
    """The renewal leg must REACH a scheduler-created lease. Nodes register
    as ``node_<name>`` and the scheduler's lease therefore carries
    node_id=node_pc_worker, while the executor runs with the BARE id
    (``pc_worker``) — the exact spelling gap _is_assigned_to_me already
    tolerates (#804). _find_lease_for_task does not: with the push lease
    live, an un-tolerant lookup would renew NOTHING and every long lane
    would be reclaimed at TTL despite a healthy worker. RED on main:
    '' (no lease found)."""
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()

    def fake(endpoint, method, path, data, token, node_id, timeout=15):
        assert path == "/api/v1/leases"
        # main's view: scheduler-created lease, node recorded node_*
        return [{
            "id": "lease_s1", "task_id": "t9",
            "node_id": "node_pc_worker",
            "status": "active",
        }]

    with patch.object(ae, "_signed_request", fake):
        found = ex._find_lease_for_task("t9")

    assert found == "lease_s1", (
        "a lease held for this node under the node_<bare> spelling must be "
        f"found by the executor; got {found!r}"
    )


def test_control_renewal_posts_extend_for_recorded_lease():
    """GREEN control on main: once the spawn RECORDS a lease id, the poll
    loop extends it over /api/v1/leases/<id>/extend (the existing seam the
    node-id fix above feeds)."""
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()

    renewals = []

    def fake(endpoint, method, path, data, token, node_id, timeout=15):
        renewals.append(path)
        return {"id": "lease_x", "status": "active"}

    with patch.object(ae, "_signed_request", fake):
        spawn = ae.ActiveSpawn(task_id="t9", task_title="t",
                               process=None, lease_id="lease_x")
        with ex._lock:
            ex._active_spawns["t9"] = spawn
        ex._renew_leases()

    assert renewals == ["/api/v1/leases/lease_x/extend"]


# ---------------------------------------------------------------------------
# 4. VISIBILITY: running task with absent/expired lease must surface
# ---------------------------------------------------------------------------

def test_status_surfaces_unleased_running_task(app_client):
    """Today the only way to notice is to notice: a running task with no
    lease (the #916 live incident: 9 running, 0 leases) is invisible. The
    cluster status must give a monitor a field to key on."""
    client = app_client
    node_id = client.post("/api/v1/nodes/join", json={
        "node_name": "w1", "capabilities": ["tooling"],
    }).json()["node_id"]
    r = client.post("/api/v1/tasks", json={
        "title": "unleased running task", "requires": ["tooling"],
    })
    assert r.status_code == 200
    task_id = r.json()["id"]
    # force it running WITHOUT a lease (the leaseless-running shape the
    # incident showed; how it got there is irrelevant to visibility).
    from hermes_cluster.routers import cluster as cluster_mod
    cluster_mod._state.set_task_status(task_id, TaskStatus.running)
    with cluster_mod._state._tasks_lock:
        cluster_mod._state._tasks[task_id].assigned_to = node_id

    status = client.get("/api/v1/cluster/status").json()
    assert "unleased_running" in status, (
        f"/cluster/status must expose a leaseless-running count: {status!r}"
    )
    assert status["unleased_running"] == 1, (
        f"the 9-running-0-leases incident must be a countable field, got "
        f"{status.get('unleased_running')!r}"
    )
    detail = status.get("unleased_running_tasks") or []
    ids = [d.get("task_id") for d in detail]
    assert task_id in ids, f"per-task detail must name the offender: {detail!r}"


def test_status_counts_expired_lease_as_unleased(app_client):
    """An EXPIRED-but-unreclaimed lease is functionally unleased: the same
    field must count it (a wedged-but-not-yet-scanned task is what a
    monitor pages on)."""
    client = app_client
    client.post("/api/v1/nodes/join", json={
        "node_name": "w1", "capabilities": ["tooling"]})
    task_id = client.post("/api/v1/tasks", json={
        "title": "expired lease task", "requires": ["tooling"]}).json()["id"]
    from hermes_cluster.routers import cluster as cluster_mod
    st = cluster_mod._state
    st.set_task_status(task_id, TaskStatus.running)
    st.create_lease(task_id, "node_w1", timedelta(seconds=-5))

    status = client.get("/api/v1/cluster/status").json()
    assert status.get("unleased_running", 0) >= 1, (
        f"expired-lease running task must count as unleased: {status!r}"
    )


def test_recovery_scan_logs_unleased_running(caplog):
    """The auto-recovery scan must make an unleased running task LOUD even
    if nobody polls /status: a WARNING naming task and node."""
    from hermes_cluster.recovery.manager import RecoveryManager

    store = _mem_store()
    _register_online(store, "node_w1")
    mgr = RecoveryManager(store)

    _make_ready(store, "t1")
    # run it WITHOUT a lease on purpose (the #916 shape).
    store.set_task_status("t1", TaskStatus.running)
    with store._tasks_lock:
        store._tasks["t1"].assigned_to = "node_w1"

    with caplog.at_level(logging.WARNING):
        mgr.detect_expired_leases()

    msgs = [r.message for r in caplog.records
            if r.levelno >= logging.WARNING]
    assert any("t1" in m and "node_w1" in m for m in msgs), (
        f"scan must warn about a running task with no live lease; got {msgs!r}"
    )


# ---------------------------------------------------------------------------
# controls (pass on main; pins of the invariants the fix must not break)
# ---------------------------------------------------------------------------

def test_control_live_lease_blocks_schedule_and_unassign():
    """#804/#833 analysis is correct: while a lease is ALIVE the scheduler
    skips the task and unassign_task refuses (#833 lease guard)."""
    store = _mem_store()
    _register_online(store, "node_w1")
    _register_online(store, "node_w2")
    _make_ready(store, "t1")
    assert store.schedule_pending() == 1
    store.create_lease("t1", "node_w1", timedelta(minutes=5))

    assert store.schedule_pending() == 0
    assert store.get_task("t1").assigned_to == "node_w1"
    assert store.unassign_task("t1") is False


def test_control_claim_path_still_creates_one_lease(app_client):
    """The PULL path is the reference behaviour — it must not regress into
    double-leasing: claim creates exactly one active lease."""
    client = app_client
    node_id = client.post("/api/v1/nodes/join", json={
        "node_name": "w1", "capabilities": ["tooling"]}).json()["node_id"]
    task_id = client.post("/api/v1/tasks", json={
        "title": "claimable", "requires": ["tooling"]}).json()["id"]
    r = client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
    assert r.status_code == 200
    leases = client.get("/api/v1/leases").json()
    mine = [l for l in leases
            if l["task_id"] == task_id and l["status"] == "active"]
    assert len(mine) == 1


def test_control_expired_lease_never_blocks_scheduling():
    """An expired lease must NOT masquerade as protection: the scheduler
    skips only not-yet-expired actives (the read-only leased-id filter)."""
    store = _mem_store()
    _register_online(store, "node_w1")
    _make_ready(store, "t1")
    store.create_lease("t1", "node_w1", timedelta(seconds=-30))
    # t1 is still ready; a dead lease must not hold it hostage
    assert store.schedule_pending() == 1


def test_control_recovery_never_revives_terminal_task():
    """N2: a completed/cancelled task must not be rescheduled by recovery."""
    from hermes_cluster.recovery.revoker import Revoker
    from hermes_cluster.recovery.rescheduler import Rescheduler

    store = _mem_store()
    _register_online(store, "node_w1")
    _make_ready(store, "t1")
    store.set_task_status("t1", TaskStatus.completed)
    Revoker(store).revoke_all_for_node("node_w1")
    assert store.get_task("t1").status == TaskStatus.completed
    assert Rescheduler(store).reschedule_orphaned(["t1"]) == 0
