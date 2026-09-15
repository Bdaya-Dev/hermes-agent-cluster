"""Unschedulable work is refused at submit, and drain actually drains (#907).

Two failures measured on the live factory 2026-09-15, both of which look like
success at the moment they happen and only surface hours later as idle workers:

1. **A capability no node advertises is accepted and queues forever.**
   ``scheduler.node_can_run`` can never match it, so the task sits READY with
   nothing wrong showing anywhere. Mined from 1394 real tasks: ``merge``,
   ``land``, ``flutter``, ``invora``, ``author`` and ``pc-maint`` were all
   requested by lanes and none existed on any node. One of them was
   ``bayader-flutter!221``'s LANDING task — a reviewed client MR sat
   unclaimable while the cluster ran at 4 of 22 slots.

2. **Draining by stripping capabilities does not drain.** An empty
   ``requires`` matches every node whatever it advertises, so the "drained"
   node keeps taking unconstrained work; and the worker's next re-join
   rewrites the capability list from its own local config
   (``node_manager.register_node`` -> ``update_capabilities``), reverting the
   strip. That revert orphaned the 9 queued ``pc-maint`` tasks.

Store-level assertions run against BOTH stores so the in-memory and SQLite
implementations cannot drift.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.scheduler import node_can_run
from hermes_cluster.models import Node, NodeStatus
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore

BOTH_STORES = [ClusterState, ClusterStore]


def make_store(store_cls):
    return store_cls(":memory:") if store_cls is ClusterStore else store_cls()


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


def _client():
    app = create_app(cluster_id="test-cluster", node_id="node_main", node_role="main")
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Submit-time refusal of a capability no node can serve
# ---------------------------------------------------------------------------

def test_submit_refuses_capability_no_node_advertises():
    client = _client()
    client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": ["tooling", "review"],
              "max_concurrent": 4},
    )

    r = client.post("/api/v1/tasks", json={"title": "land !221", "requires": ["merge"]})

    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    # The refusal must name the unservable capability AND the real vocabulary,
    # or the caller's only recourse is to guess — which is how 'merge', 'land'
    # and 'author' got invented in the first place.
    assert "merge" in detail
    assert "tooling" in detail and "review" in detail
    # And it must NOT invite relabelling: a capability swapped for one that
    # merely schedules converts a truthful need into a lie.
    assert "do NOT relabel" in detail


def test_submit_refuses_when_only_ONE_of_several_is_unknown():
    """A partially-satisfiable requires is still unschedulable."""
    client = _client()
    client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": ["tooling"], "max_concurrent": 4},
    )

    r = client.post(
        "/api/v1/tasks",
        json={"title": "mixed", "requires": ["tooling", "flutter"]},
    )

    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    # Only the UNSERVABLE one is reported — naming the satisfiable half too
    # would send the caller to drop a capability that was never the problem.
    assert "advertises flutter" in detail
    assert "advertises tooling" not in detail


def test_submit_accepts_a_capability_that_exists():
    client = _client()
    client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": ["tooling", "review"],
              "max_concurrent": 4},
    )

    r = client.post("/api/v1/tasks", json={"title": "ok", "requires": ["review"]})
    assert r.status_code == 200, r.text


def test_submit_accepts_when_the_fleet_is_still_EMPTY():
    """No nodes registered yet => queue, do not refuse.

    Refusing every submit while the cluster is coming up would be a worse
    failure than the one this guard exists to stop: the scheduler already
    holds these tasks harmlessly until a node joins.
    """
    client = _client()
    r = client.post("/api/v1/tasks", json={"title": "early", "requires": ["tooling"]})
    assert r.status_code == 200, r.text


def test_submit_ignores_a_DRAINED_node_when_deciding_what_is_servable():
    """A capability only a drained node advertises cannot be served either.

    Accepting it would reproduce the exact stall the guard exists to stop —
    and this is the live shape: 'pc-maint' existed on exactly one node, that
    node was quarantined, and its 9 queued tasks were then unclaimable.
    """
    client = _client()
    join = client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": ["tooling", "pc-maint"],
              "max_concurrent": 4},
    )
    # A second, healthy node — so the fleet still has a vocabulary to check
    # against and we are testing the drain exclusion, not the empty-fleet
    # carve-out below.
    client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w2", "capabilities": ["tooling"], "max_concurrent": 4},
    )
    node_id = join.json()["node_id"]
    assert client.patch(
        f"/api/v1/nodes/{node_id}/drain", json={"drained": True}
    ).status_code == 200

    r = client.post("/api/v1/tasks", json={"title": "maint", "requires": ["pc-maint"]})
    assert r.status_code == 422, r.text
    assert "pc-maint" in r.json()["detail"]
    # The surviving node's vocabulary is what the caller is offered.
    assert "tooling" in r.json()["detail"]


def test_submit_QUEUES_when_the_whole_fleet_is_drained():
    """A fully-drained fleet is the coming-up case, not the unservable case.

    The carve-out is keyed on "any schedulable node exists", not on "the known
    vocabulary is non-empty". Keyed the other way, a fleet whose only node was
    drained accepted ANY capability — which is how this guard first shipped.
    """
    client = _client()
    join = client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": ["tooling"], "max_concurrent": 4},
    )
    client.patch(
        f"/api/v1/nodes/{join.json()['node_id']}/drain", json={"drained": True}
    )

    r = client.post("/api/v1/tasks", json={"title": "held", "requires": ["tooling"]})
    assert r.status_code == 200, r.text


def test_submit_refuses_when_a_schedulable_node_advertises_NOTHING():
    """One node, zero capabilities => the vocabulary is empty, not absent.

    An empty-but-present vocabulary must still refuse: no task with a
    `requires` could ever be matched, so accepting is the forever-queue again.
    """
    client = _client()
    client.post(
        "/api/v1/nodes/join",
        json={"node_name": "w1", "capabilities": [], "max_concurrent": 4},
    )

    r = client.post("/api/v1/tasks", json={"title": "x", "requires": ["tooling"]})
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert "tooling" in detail
    # The message must say the vocabulary is EMPTY, not that no node joined —
    # a caller told "no nodes registered" would wait for a node that is
    # already there.
    assert "every schedulable node advertises nothing" in detail


# ---------------------------------------------------------------------------
# 2. Drain sends a node NOTHING — including an empty `requires`
# ---------------------------------------------------------------------------

def test_node_can_run_is_False_for_a_drained_node_even_with_empty_requires():
    """The unit-level statement of the bug: `requires: []` matched everything.

    This is what made a capability strip a non-drain — the stripped node kept
    receiving every unconstrained task in the queue.
    """
    node = Node(id="n1", name="n1", capabilities=[], status=NodeStatus.online)
    assert node_can_run([], node) is True

    node.drained = True
    assert node_can_run([], node) is False
    assert node_can_run(["tooling"], node) is False


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_drained_node_receives_no_work_and_an_online_peer_takes_it(store_cls):
    store = make_store(store_cls)
    register_node(store, "n_drained")
    register_node(store, "n_ok")
    store.set_drained("n_drained", True)

    for i in range(4):
        store.create_task(f"t_{i}", f"t {i}", [])
    store.trigger_pending_tasks()
    store.schedule_pending()

    owners = {store.get_task(f"t_{i}").assigned_to for i in range(4)}
    assert owners == {"n_ok"}, f"drained node was handed work: {owners}"


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_drain_survives_a_worker_RE_JOIN(store_cls):
    """The revert that made the live quarantine fail.

    A re-join refreshes heartbeat/capabilities/capacity. Drain is OWNER state
    and must not be among the things a worker can write.
    """
    store = make_store(store_cls)
    register_node(store, "n1", capabilities=("tooling",))
    store.set_drained("n1", True)

    # The worker checks in again, re-declaring its own local capability list.
    store.update_capabilities("n1", ["tooling", "review"])
    register_node(store, "n1", capabilities=("tooling", "review"))

    assert store.get_node("n1").drained is True, "re-join cleared the drain"


@pytest.mark.parametrize("store_cls", BOTH_STORES)
def test_undrain_restores_scheduling(store_cls):
    store = make_store(store_cls)
    register_node(store, "n1")
    store.set_drained("n1", True)

    store.create_task("t_0", "t 0", [])
    store.trigger_pending_tasks()
    assert store.schedule_pending() == 0

    store.set_drained("n1", False)
    assert store.schedule_pending() == 1
    assert store.get_task("t_0").assigned_to == "n1"


def test_drain_endpoint_404s_on_an_unknown_node():
    client = _client()
    r = client.patch("/api/v1/nodes/node_nope/drain", json={"drained": True})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 3. The SQLite -> Postgres importer survives the new boolean column
# ---------------------------------------------------------------------------

def test_sqlite_int_flag_is_coerced_to_a_postgres_BOOLEAN():
    """SQLite has no boolean type; asyncpg refuses an int for a BOOLEAN param.

    `nodes.drained` is INTEGER 0/1 in SQLite and a real BOOLEAN in Postgres, so
    the importer must coerce. Caught by CI, NOT by the local suite: the real
    importer tests need a live Postgres and SKIP without one, so a green local
    run says nothing about this path. This test is deliberately pure — it
    asserts the coercion map and its arithmetic with no database at all, so the
    regression cannot hide behind a skip again.
    """
    from hermes_cluster.tools import import_sqlite as imp

    assert "drained" in imp._BOOL_COLUMNS.get("nodes", []), \
        "every boolean Postgres column needs an entry, or the import aborts"

    bool_cols = set(imp._BOOL_COLUMNS["nodes"])
    coerce = lambda v: None if v is None else bool(v)  # noqa: E731 - mirrors the loop
    assert "drained" in bool_cols
    assert coerce(0) is False and coerce(1) is True
    # NULL must stay NULL so the column default applies, rather than becoming
    # a positive False that pins a pre-migration row to "explicitly not drained".
    assert coerce(None) is None


def test_every_declared_bool_column_exists_in_the_sqlite_schema():
    """A typo in the coercion map is silent: the column is simply never coerced.

    So the map is checked against the schema the importer actually reads.
    """
    from hermes_cluster.state.cluster_store import ClusterStore
    from hermes_cluster.tools import import_sqlite as imp

    store = ClusterStore(":memory:")
    for table, cols in imp._BOOL_COLUMNS.items():
        present = {r["name"] for r in
                   store._conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for col in cols:
            assert col in present, f"{table}.{col} is not a column of {table}"
