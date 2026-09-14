"""#858 DEFECT 2 — lane-to-node affinity in the scheduler (tests_v3).

A task whose ``lane_key`` has a live ``lanes`` row naming node N must
schedule to N: that is where the lane's hermes session and working clone
live (LFP-1). Scheduling it elsewhere builds a second session and a second
clone of the same branch. Only an unplaced lane — no row, or a row that
names no node — is free to schedule anywhere, and lane-less tasks are
untouched by this rule.

Parked-on-offline policy (the brief asks for a decided, justified answer):
when the lane's node is OFFLINE or AT CAPACITY the task PARKS — it stays
``ready`` for a later trigger. Silently scheduling it elsewhere is the bug
restated; failing it is worse (the node may be back within one heartbeat
interval and a transient failure trains operators to ignore failures).
Rationale in hermes_cluster/core/lane_affinity.py.

Store coverage mirrors the #829 parity suite: the behavioral tests run
against the in-memory ClusterState and the SQLite ClusterStore directly,
and the core pinning/parking ones additionally against the Postgres store
(skipped without a server, exercised in CI's postgres:16 service).

RED proof against main (68f7f6e): main's scheduler has no notion of lane
placement — ``grep -rn 'affinity|preferred_node' hermes_cluster/`` is
empty — so a lane task pinned to n2 lands on the fair planner's pick
('n1') instead. See the MR for the exact failure output.
"""

import pytest

from hermes_cluster.models import Node, NodeStatus, TaskStatus
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore

BOTH_STORES = [ClusterState, ClusterStore]


def make_store(store_cls):
    if store_cls is ClusterStore:
        return store_cls(":memory:")
    return store_cls()


def register_node(store, node_id, capabilities=("tooling",),
                  max_concurrent=0, status=NodeStatus.online):
    store.register_node(Node(
        id=node_id, name=node_id, capabilities=list(capabilities),
        status=status, max_concurrent=max_concurrent,
    ))


def ready_task(store, task_id, title, **kw):
    """create_task + promote to ready (ClusterState needs the explicit
    trigger; ClusterStore.create_task self-promotes — both are idempotent
    through the same helper, mirroring tests_v3/test_scheduler_fairness)."""
    store.create_task(task_id, title, [], **kw)
    store.trigger_pending_tasks()


# ---------------------------------------------------------------------------
# Postgres parity: the SAME planner code runs in postgres_store's
# schedule_pending_detailed; these pin the two decisive behaviors (pin +
# park) through the sync facade, skipped when no server answers.
# ---------------------------------------------------------------------------

@pytest.fixture()
def pg(request):
    return request.getfixturevalue("pg_store")


def _pg_pins_lane_task(pg):
    register_node(pg, "n1", max_concurrent=2)
    register_node(pg, "n2", max_concurrent=2)
    pg.record_lane("L", session_id="sid_L", node="n2")
    pg.create_task("t_lane", "delivery on lane L", [], lane_key="L")
    assert pg.schedule_pending() == 1
    assert pg.get_task("t_lane").assigned_to == "n2"


def _pg_parks_offline_lane_node(pg):
    register_node(pg, "n1", max_concurrent=5)
    register_node(pg, "n2", max_concurrent=5)
    pg.record_lane("L", session_id="sid_L", node="n2")
    pg.set_node_status("n2", NodeStatus.offline)
    pg.create_task("t_lane", "delivery", [], lane_key="L")
    assert pg.schedule_pending() == 0
    assert pg.get_task("t_lane").status == TaskStatus.ready


def test_postgres_store_pins_lane_task(pg):
    _pg_pins_lane_task(pg)


def test_postgres_store_parks_offline_lane_node(pg):
    _pg_parks_offline_lane_node(pg)


# ---------------------------------------------------------------------------
# The pinning rule
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_lane_task_pins_to_lane_node_not_least_loaded(store_cls):
    """THE measured defect: lane L is placed on n2, and the fair planner
    (main's only behavior) hands the task to the least-loaded online node
    regardless. The affinity rule must override load-balancing: the lane
    task goes to n2 — anywhere else builds a second session and a second
    clone of the same branch."""
    store = make_store(store_cls)
    # n1 would win the round-robin tiebreak (registered first, both empty).
    register_node(store, "n1", max_concurrent=2)
    register_node(store, "n2", max_concurrent=2)

    # Lane L is placed on n2 (row exists with node=n2).
    store.record_lane("L", session_id="sid_L", node="n2")

    ready_task(store, "t_lane", "delivery on lane L", lane_key="L")
    assert store.schedule_pending() == 1

    task = store.get_task("t_lane")
    assert task.assigned_to == "n2", (
        f"lane task must go to the lane's node (n2), got {task.assigned_to!r} "
        "— a second machine would build a second session + clone of the "
        "same branch (#858)")


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_lane_task_pin_survives_load_tiebreak(store_cls):
    """Pin beats the load-balancer even when the lane node carries load the
    other node does not (n2 at max_concurrent=1 with its own lane task
    done-and-gone; n1 completely free)."""
    store = make_store(store_cls)
    register_node(store, "n1", max_concurrent=5)
    register_node(store, "n2", max_concurrent=1)
    store.record_lane("L", session_id="sid_L", node="n2")

    ready_task(store, "t_lane1", "first lane delivery", lane_key="L")
    assert store.schedule_pending() == 1
    assert store.get_task("t_lane1").assigned_to == "n2"
    store.set_task_status("t_lane1", TaskStatus.completed)  # n2 free again

    ready_task(store, "t_lane2", "second lane delivery", lane_key="L")
    store.schedule_pending()
    assert store.get_task("t_lane2").assigned_to == "n2", (
        "the second lane delivery must still pin to n2")


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_unplaced_lane_and_laneless_tasks_schedule_freely(store_cls):
    """Only a lane with a row is pinned: a lane_key with NO lanes row, and
    a lane row that names no node, both fall through to the fair planner."""
    store = make_store(store_cls)
    register_node(store, "n1", max_concurrent=5)
    register_node(store, "n2", max_concurrent=5)

    store.record_lane("nobody", session_id="s1", node="")  # unplaced lane
    ready_task(store, "t_nobody", "lane row, no node", lane_key="nobody")
    ready_task(store, "t_nolane", "lane_key, no row", lane_key="ghost")
    ready_task(store, "t_plain", "no lane at all")
    assert store.schedule_pending() == 3  # nothing parked


# ---------------------------------------------------------------------------
# Parking policy: offline / at-capacity lane node
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_lane_task_parks_when_lane_node_offline(store_cls):
    """Defensible-answer decision (#858): pinned node offline => PARK.
    The task stays ready; it is NEVER assigned to another node, and it is
    never failed for a transient outage."""
    store = make_store(store_cls)
    register_node(store, "n1", max_concurrent=5)
    register_node(store, "n2", max_concurrent=5)
    store.record_lane("L", session_id="sid_L", node="n2")
    store.set_node_status("n2", NodeStatus.offline)

    ready_task(store, "t_lane", "delivery", lane_key="L")
    assert store.schedule_pending() == 0, (
        "a parked lane task must not be scheduled at all")
    task = store.get_task("t_lane")
    assert task.status == TaskStatus.ready
    assert task.assigned_to is None

    # Recovery: when the node comes back, the SAME task lands on its lane's
    # node (parking preserves affinity; it is not a re-home).
    store.set_node_status("n2", NodeStatus.online)
    assert store.schedule_pending() == 1
    assert store.get_task("t_lane").assigned_to == "n2"


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_lane_task_parks_when_lane_node_at_capacity(store_cls):
    """Pinned node online but at max_concurrent => PARK too. Moving the
    task to a different machine is the second-session bug; failing it for
    transient load is noise. It waits its turn on its own node."""
    store = make_store(store_cls)
    register_node(store, "n1", max_concurrent=5)
    register_node(store, "n2", max_concurrent=1)
    store.record_lane("L", session_id="sid_L", node="n2")

    ready_task(store, "t_first", "occupies n2", lane_key="L")
    assert store.schedule_pending() == 1
    assert store.get_task("t_first").assigned_to == "n2"

    ready_task(store, "t_second", "queues behind on lane node", lane_key="L")
    assert store.schedule_pending() == 0
    assert store.get_task("t_second").status == TaskStatus.ready

    # n2 frees (t_first completes) -> the parked task lands on n2, not n1.
    store.set_task_status("t_first", TaskStatus.completed)
    assert store.schedule_pending() == 1
    assert store.get_task("t_second").assigned_to == "n2"


# ---------------------------------------------------------------------------
# Decision provenance + planner unit behavior
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_pinned_assignment_records_lane_affinity_reason(store_cls):
    """The SchedulingDecision tells the operator WHY the node was picked —
    invisible affinity is how a stray lane runs two hours on the wrong
    machine."""
    store = make_store(store_cls)
    register_node(store, "n1", max_concurrent=5)
    register_node(store, "n2", max_concurrent=5)
    store.record_lane("L", session_id="sid_L", node="n2")
    ready_task(store, "t_lane", "delivery", lane_key="L")
    store.schedule_pending()

    decisions = store.get_decisions()
    mine = [d for d in decisions if d.task_id == "t_lane"]
    assert mine and mine[-1].reason == "lane_affinity_pinned"
    assert mine[-1].node_id == "n2"


def test_pinned_node_capability_mismatch_parks():
    """Pure-planner unit: even if the lane node is online but stopped
    matching the task's requires, the answer is PARK (None, pinned=True) —
    never a re-home."""
    from hermes_cluster.core.lane_affinity import AffinityScheduler

    sched = AffinityScheduler()
    n1 = Node(id="n1", name="n1", capabilities=[], status=NodeStatus.online)
    n2 = Node(id="n2", name="n2", capabilities=[], status=NodeStatus.online)
    node, pinned = sched.choose_pinned(
        ["cuda"], [n1, n2], {}, pinned_node_id="n2")
    assert (node, pinned) == (None, True)


def test_choose_pinned_returns_pair_contract():
    """Unpinned tasks keep fair-scheduling behavior through the same entry
    point and report pinned=False so callers can label decisions honestly."""
    from hermes_cluster.core.lane_affinity import AffinityScheduler

    sched = AffinityScheduler()
    n1 = Node(id="n1", name="n1", capabilities=[], status=NodeStatus.online)
    n2 = Node(id="n2", name="n2", capabilities=[], status=NodeStatus.online)
    node, pinned = sched.choose_pinned([], [n1, n2], {}, pinned_node_id="")
    assert node is not None and pinned is False


# ---------------------------------------------------------------------------
# Executor-side last line of defense: never spawn a lane task whose row
# names another node (measured incident: the stray lane ran two hours on
# the wrong machine). The spawn must be REFUSED and the task released to
# the board with a readable reason, not run and not failed.
# ---------------------------------------------------------------------------

def test_executor_refuses_spawn_for_foreign_lane_node(tmp_path):
    from unittest.mock import patch

    from hermes_cluster.core.agent_executor import (
        AgentExecutor, AgentExecutorConfig)
    from hermes_cluster.state.cluster_store import ClusterStore

    store = ClusterStore(db_path=":memory:")
    store.record_lane("L", session_id="sid_L", node="other-machine")
    executor = AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60,
                                   worker="hermes", working_dir=str(tmp_path)),
        node_id="test-node", cluster_endpoint="http://127.0.0.1:9999",
        store=store, peer_token="tok",
    )

    requests = []

    def spy(endpoint, method, path, data, token, node_id, **kw):
        requests.append((method, path, data))
        return {}

    with patch("hermes_cluster.core.agent_executor.subprocess.Popen") as popen:
        with patch("hermes_cluster.core.agent_executor._signed_request",
                   side_effect=spy):
            executor._spawn_worker({
                "id": "t_stray", "title": "would run on the wrong machine",
                "lane_key": "L", "role": "author", "status": "running",
                "assigned_to": "test-node",
            })
        # A foreign-node lane must not spawn here:
        popen.assert_not_called()

    releases = [r for r in requests if r[1].endswith("/release")]
    assert len(releases) == 1, (
        f"misplaced task must be released to the board, saw {requests}")
    reason = releases[0][2]["reason"]
    assert "other-machine" in reason and "L" in reason
    # NOT failed: the incident's second failure mode (an unexplained failed
    # task) must not be traded for the first one.
    assert not [r for r in requests if r[1].endswith("/fail")]


def test_executor_same_node_spelling_variants_not_blocked(tmp_path):
    """node_<name> vs <name>: the lane row and the executor must match
    through the registration-prefix difference, or every lane self-blocks."""
    from hermes_cluster.state.cluster_store import ClusterStore
    from hermes_cluster.core.agent_executor import (
        AgentExecutor, AgentExecutorConfig)

    executor = AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60),
        node_id="macbook_worker",
        cluster_endpoint="http://127.0.0.1:9999",
        store=ClusterStore(db_path=":memory:"), peer_token="tok",
    )
    assert executor._node_ids_match("node_macbook_worker", "macbook_worker")
    assert executor._node_ids_match("macbook_worker", "node_macbook_worker")
    assert not executor._node_ids_match("node_pc", "macbook_worker")
    assert not executor._node_ids_match("", "macbook_worker")
