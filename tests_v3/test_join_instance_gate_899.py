"""#899 — main-side duplicate-executor gate: /join with a DIFFERENT instance
token for a node id whose current instance is still heartbeating is refused
409; once the old instance has missed offline_after the new join takes the
node id over.

Policy choice (documented in NodeManager.join): REFUSE, not force-replace —
the main cannot tell "operator restarted the worker" apart from "a second
wrapper woke up beside the first", and the measured incident had BOTH
pollers live; adopting whichever packet arrived last would have hidden the
bug instead of surfacing it. The stale-heartbeat escape hatch keeps a
legitimate restart from ever wedging.

Older workers that never send instance_token keep the exact pre-#899
idempotent re-join behaviour (by-design control — must hold on main too).
"""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.node_manager import (
    DuplicateInstanceJoin,
    NodeManager,
    _WatchdogConfig,
)
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore


# ---------------------------------------------------------------------------
# NodeManager unit level (no HTTP)
# ---------------------------------------------------------------------------

def _nm(store=None, offline_after=30.0):
    store = store or ClusterState()
    return NodeManager(store,
                       watchdog_config=_WatchdogConfig(
                           check_interval=1.0, degraded_after=5.0,
                           offline_after=offline_after))


class TestJoinGateUnit:
    def test_first_join_records_the_instance_token(self):
        nm = _nm()
        node = nm.join("node_w", name="w", instance_token="tkA")
        assert node.instance_token == "tkA"
        got = nm.get_node("node_w")
        assert got.instance_token == "tkA"

    def test_same_token_rejoin_is_idempotent(self):
        nm = _nm()
        nm.join("node_w", name="w", instance_token="tkA")
        node = nm.join("node_w", name="w", instance_token="tkA")
        assert node.status.value == "online"
        assert node.instance_token == "tkA"

    def test_different_token_fresh_heartbeat_raises(self):
        nm = _nm()
        nm.join("node_w", name="w", instance_token="tkA")
        with pytest.raises(DuplicateInstanceJoin) as ei:
            nm.join("node_w", name="w", instance_token="tkB")
        exc = ei.value
        assert exc.node_id == "node_w"
        assert exc.holder_token == "tkA"
        assert "second executor" in str(exc)
        # The holder keeps its token — a refused join must NOT flip the owner.
        assert nm.get_node("node_w").instance_token == "tkA"

    def test_different_token_after_offline_window_takes_over(self):
        """The documented escape hatch: once the old instance has missed
        offline_after the new join wins and rewrites the token."""
        store = ClusterState()
        nm = _nm(store, offline_after=30.0)
        nm.join("node_w", name="w", instance_token="tkA")
        # age the heartbeat past the window by rewriting last_heartbeat
        node = store.get_node("node_w")
        node.last_heartbeat = datetime.utcnow() - timedelta(seconds=31)
        store.register_node(node)  # replace the row with the aged timestamp
        out = nm.join("node_w", name="w", instance_token="tkB")
        assert out.instance_token == "tkB"
        assert out.status.value == "online"

    def test_tokenless_rejoin_never_refused(self):
        """By-design control (must hold on main too): older workers send no
        token — the pre-#899 idempotent re-join is preserved byte-for-byte."""
        nm = _nm()
        nm.join("node_w", name="w", instance_token="tkA")
        node = nm.join("node_w", name="w")          # no token at all
        assert node.status.value == "online"
        # holder token untouched by a tokenless re-join
        assert node.instance_token == "tkA"

    def test_first_tokenless_join_then_token_join_ok(self):
        """A node first registered WITHOUT a token (older worker) accepts a
        tokened join — there is no previous instance to disprove."""
        nm = _nm()
        nm.join("node_w", name="w")
        node = nm.join("node_w", name="w", instance_token="tkA")
        assert node.instance_token == "tkA"

    def test_sqlite_store_persists_instance_token_roundtrip(self, tmp_path):
        """The gate must read the token back from the SAME store flavours
        the hosted main uses: SQLite (file) here; in-memory above."""
        db = str(tmp_path / "n.db")
        s1 = ClusterStore(db_path=db)
        nm = NodeManager(s1)
        nm.join("node_w", name="w", instance_token="tkA")
        s1.close()
        s2 = ClusterStore(db_path=db)
        assert s2.get_node("node_w").instance_token == "tkA"
        nm2 = NodeManager(s2)
        with pytest.raises(DuplicateInstanceJoin):
            nm2.join("node_w", name="w", instance_token="tkB")
        s2.close()


# ---------------------------------------------------------------------------
# HTTP level: POST /api/v1/nodes/join -> 409
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app(cluster_id="test-cluster", node_id="test-node",
                     node_role="main")
    return TestClient(app)


def _join(client, name, token=None, caps=None):
    body = {"node_name": name, "capabilities": caps or ["coding"]}
    if token is not None:
        body["instance_token"] = token
    return client.post("/api/v1/nodes/join", json=body)


class TestJoinHttp409:
    def test_join_carries_instance_token_and_second_is_409(self, client):
        r1 = _join(client, "w1", token="tkA")
        assert r1.status_code == 200
        r2 = _join(client, "w1", token="tkB")
        assert r2.status_code == 409, r2.text
        detail = r2.json()["detail"]
        assert "second executor" in detail
        assert "offline_after" in detail

    def test_same_token_rejoin_200(self, client):
        assert _join(client, "w1", token="tkA").status_code == 200
        assert _join(client, "w1", token="tkA").status_code == 200

    def test_tokenless_workers_unaffected(self, client):
        """By-design control: legacy joins (no token field) stay 200."""
        assert _join(client, "w1").status_code == 200
        assert _join(client, "w1").status_code == 200

    def test_takeover_after_offline_window_via_state(self, client):
        """Restart path through the API: age the first instance's heartbeat
        past offline_after in the store, then the new join is 200 again."""
        assert _join(client, "w1", token="tkOld").status_code == 200
        # Reach the app's store the way the test surface does: via state.
        from hermes_cluster import app as app_mod
        # The app keeps managers on module refs; simplest: drive through a
        # fresh join after manipulating the in-memory node directly.
        # (ClusterState nodes are mutable; register_node replaces.)
        # Locate the state via the nodes route's init wiring.
        from hermes_cluster.routers import nodes as nodes_router
        state = nodes_router._state
        node = state.get_node("node_w1")
        node.last_heartbeat = datetime.utcnow() - timedelta(seconds=31)
        state.register_node(node)
        r = _join(client, "w1", token="tkNew")
        assert r.status_code == 200, r.text
        assert state.get_node("node_w1").instance_token == "tkNew"
