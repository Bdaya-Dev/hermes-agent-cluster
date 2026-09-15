"""#909 — main's lane affinity has NO DATA: workers never report lane
placement to the main, so `lane_nodes` is always empty on the scheduler's
side and every stateful lane bounce (30 of 375 measured on hosted main at
76b55c7, the #858 affinity merge) is silently fair-scheduled to whatever
node has capacity — building a second Hermes session and a second clone.

This file pins the fixed shape on three surfaces:

  A. Main's HTTP surface: `POST /api/v1/lanes/report` exists, upserts the
     placement into main's store, re-triggers scheduling (a parked task gets
     its lane node back the moment the report lands), and REFUSES a
     placement that claims a node other than the authenticated peer
     (X-Peer-Node) — a worker cannot pin a lane to someone else's machine.
     Plus `GET /api/v1/lanes` / `GET /api/v1/lanes/{lane_key}` (main today
     answers 404 — measured live on hermes.bdaya-dev.com).

  B. The executor REPORTS: every local `record_lane` (spawn-time placement
     persist and reap-time session capture) is mirrored to main with the
     node id it recorded — otherwise the report surface has no producer.

  C. The scheduler HONORS the reported pin across the node-id spelling gap
     (the executor runs as bare `macbook_worker`, the node registry is
     `node_macbook_worker`; a pin stored under one spelling must match the
     registered node, or the "fix" parks every lane forever).

RED on main (ddfceaa): A -> POST returns 404 (no route); B -> zero signed
requests carry a lanes path; C -> choose_pinned('n2') vs registered
'node_n2' returns (None, True) i.e. parked instead of pinned.
"""

import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.lane_affinity import AffinityScheduler
from hermes_cluster.models import Node, NodeStatus, TaskStatus
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore


@pytest.fixture()
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def _register(c, node_id, capabilities=("tooling",), max_concurrent=2):
    resp = c.post("/api/v1/nodes/join", json={
        "node_name": node_id.removeprefix("node_"),
        "capabilities": list(capabilities),
        "max_concurrent": max_concurrent,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()["node_id"]


def _submit(c, title, lane_key, requires=("tooling",)):
    resp = c.post("/api/v1/tasks", json={
        "title": title, "lane_key": lane_key, "requires": list(requires),
    })
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["id"]


# ---------------------------------------------------------------------------
# A. Main's lane-placement surface
# ---------------------------------------------------------------------------

class TestLaneReportEndpoint:
    def test_report_upserts_placement_into_mains_store(self, client):
        nid = _register(client, "node_n1")
        resp = client.post(
            "/api/v1/lanes/report",
            json={"lane_key": "L", "node_id": nid, "session_id": "sid_L",
                  "role": "author"},
            headers={"X-Peer-Node": nid},
        )
        assert resp.status_code == 200, resp.text
        got = client.get("/api/v1/lanes/L")
        assert got.status_code == 200, (
            "main must expose the placement it accepted")
        lane = got.json()
        assert lane["node"] == nid
        assert lane["session_id"] == "sid_L"

    def test_report_pinned_task_schedules_to_the_lane_node(self, client):
        nid = _register(client, "node_n1")
        other = _register(client, "node_n2")
        # L lives on n2 (reported by n2's executor)...
        r = client.post(
            "/api/v1/lanes/report",
            json={"lane_key": "L", "node_id": other},
            headers={"X-Peer-Node": other},
        )
        assert r.status_code == 200, r.text
        # ...so the NEXT delivery must go to n2, never the fair pick.
        t = _submit(client, "delivery on L", "L")
        c = client.post("/api/v1/schedule/trigger").json()
        assert c["scheduled"] >= 1
        assert client.get(f"/api/v1/tasks/{t}").json()["assigned_to"] == other

    def test_report_triggers_scheduling_in_the_same_call(self, client):
        """The report is the moment the pin becomes known; a parked/ready
        lane task must be placed by THIS call, not wait for an external
        trigger — the window between report and trigger is exactly when a
        fair re-schedule elsewhere in the cluster could steal the lane."""
        nid = _register(client, "node_n2")
        _register(client, "node_n1")
        t = _submit(client, "delivery", "L")
        # n2 is online; the pin from the report must land on it in this call.
        r = client.post(
            "/api/v1/lanes/report",
            json={"lane_key": "L", "node_id": nid},
            headers={"X-Peer-Node": nid},
        )
        assert r.status_code == 200, r.text
        assigned = r.json().get("scheduled", -1)
        assert assigned >= 1, (
            "POST /lanes/report must trigger scheduling and report how many "
            "tasks it placed (parked -> pinned in the same call)")
        assert client.get(f"/api/v1/tasks/{t}").json()["assigned_to"] == nid

    def test_report_refuses_to_claim_another_nodes_lanes(self, client):
        """Defense in depth: the signature (middleware) already proves who is
        calling; when a peer identity IS known (X-Peer-Node present, as it is
        under peer auth), a report pinning lanes to a DIFFERENT node is 403 —
        macbook_worker must not be able to park every lane onto itself."""
        nid = _register(client, "node_n1")
        other = _register(client, "node_n2")
        resp = client.post(
            "/api/v1/lanes/report",
            json={"lane_key": "L", "node_id": other},
            headers={"X-Peer-Node": nid},
        )
        assert resp.status_code == 403, (
            f"a node may not place its lanes on another node, got {resp.status_code}")
        got = client.get("/api/v1/lanes/L")
        assert got.status_code == 404, (
            "the refused report must not leave a placement behind")

    def test_report_requires_lane_key_and_node(self, client):
        resp = client.post("/api/v1/lanes/report", json={"node_id": "node_n1"})
        assert resp.status_code == 422
        resp = client.post("/api/v1/lanes/report", json={"lane_key": "L"})
        assert resp.status_code == 422

    def test_list_lanes_endpoint(self, client):
        nid = _register(client, "node_n1")
        client.post("/api/v1/lanes/report",
                    json={"lane_key": "A", "node_id": nid},
                    headers={"X-Peer-Node": nid})
        client.post("/api/v1/lanes/report",
                    json={"lane_key": "B", "node_id": nid},
                    headers={"X-Peer-Node": nid})
        resp = client.get("/api/v1/lanes")
        assert resp.status_code == 200
        keys = {l["lane_key"] for l in resp.json()["lanes"]}
        assert {"A", "B"} <= keys


# ---------------------------------------------------------------------------
# B. The executor mirrors every placement it records to the main
# ---------------------------------------------------------------------------

def _executor(store, **cfg_overrides):
    from hermes_cluster.core.agent_executor import (
        AgentExecutor, AgentExecutorConfig)
    defaults = dict(enabled=True, poll_interval=60, worker="hermes")
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
        store=store,
        peer_token="test-token",
    )


class _AliveProc:
    pid = 777
    stderr = None
    stdout = None

    def poll(self):
        return None


def _lane_task(task_id, lane_key="L", node="test-node"):
    return {
        "id": task_id, "title": f"task {task_id}", "description": "d",
        "status": "running", "assigned_to": node, "priority": 3,
        "lane_key": lane_key, "role": "author",
    }


class TestExecutorReportsPlacement:
    def test_first_spawn_reports_placement_to_main(self, tmp_path):
        from hermes_cluster.core.agent_executor import ActiveSpawn  # noqa: F401
        store = ClusterStore(db_path=":memory:")
        executor = _executor(store, working_dir=str(tmp_path))

        reports = []
        real = None

        def fake_signed(endpoint, method, path, data, token, node_id, **kw):
            if method == "POST" and path.startswith("/api/v1/lanes"):
                reports.append((path, dict(data or {}), node_id))
            return {"node_id": "ok"} if path == "/api/v1/lanes/report" else None

        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=lambda cmd, **kw: _AliveProc()), \
             patch("hermes_cluster.core.agent_executor._signed_request",
                   side_effect=fake_signed):
            executor._spawn_hermes_worker(_lane_task("t1"))

        assert any(p == "/api/v1/lanes/report" for p, _, _ in reports), (
            "the executor records the lane placement LOCALLY but never tells "
            "main — main's affinity scheduler then sees no pin at all "
            "(the #909 bounce class); got paths: "
            f"{[p for p, _, _ in reports]}")
        path, body, node_id = [r for r in reports
                               if r[0] == "/api/v1/lanes/report"][0]
        assert body["lane_key"] == "L"
        assert body["node_id"] == "test-node"
        assert node_id == "test-node"

    def test_reap_session_capture_reports_to_main(self, tmp_path):
        """The reap-time record_lane is the one that carries the session id —
        main's pin must see it too (a report with node but a stale/empty
        session is still a placement; consistency matters for operators)."""
        store = ClusterStore(db_path=":memory:")
        executor = _executor(store, working_dir=str(tmp_path))
        from hermes_cluster.core.agent_executor import ActiveSpawn
        spawn = ActiveSpawn(
            task_id="t1", task_title="x", process=_AliveProc(),
            started_at=time.time(), lane_name="hermes-t1", mode="hermes",
            lane_key="L", role="author", session_id="",
            stderr_path=str(tmp_path / "e.txt"),
        )
        (tmp_path / "e.txt").write_text("session_id: sid_reap\n",
                                        encoding="utf-8")
        reports = []

        def fake_signed(endpoint, method, path, data, token, node_id, **kw):
            reports.append((path, dict(data or {})))
            return {"node_id": "ok"}

        with patch("hermes_cluster.core.agent_executor._signed_request",
                   side_effect=fake_signed):
            executor._touch_lane_from_spawn(spawn, "t1")

        lane_reports = [r for r in reports if r[0] == "/api/v1/lanes/report"]
        assert lane_reports, (
            "reap-time session capture must mirror to main; local-only write "
            "is what left main blind (the measured 30-lane bounce census)")
        assert lane_reports[-1][1]["session_id"] == "sid_reap"

    def test_report_failure_never_blocks_a_spawn(self, tmp_path):
        """#897 posture: main being down must not cost a delivery — the
        report is best-effort (the local write + spawn proceed)."""
        store = ClusterStore(db_path=":memory:")
        executor = _executor(store, working_dir=str(tmp_path))
        popens = []

        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=lambda cmd, **kw: popens.append(cmd) or _AliveProc()), \
             patch("hermes_cluster.core.agent_executor._signed_request",
                   return_value=None):  # main unreachable for everything
            executor._spawn_hermes_worker(_lane_task("t1"))

        assert len(popens) == 1, "spawn must proceed despite a dead report path"
        assert store.get_lane("L") is not None, "local placement still recorded"


# ---------------------------------------------------------------------------
# C. The pin survives the node-id spelling gap
# ---------------------------------------------------------------------------

class TestPinnedSpellingNormalization:
    """Executors run as bare `<name>`; the registry is `node_<name>`; the
    lanes row stores whichever spelling the recording node used. The
    scheduler compares the stored pin against REGISTERED node ids — so a
    bare pin must match its node_ twin (and vice versa), or the fix would
    park every lane whose report arrived with the other spelling."""

    def _nodes(self):
        return [Node(id="node_n1", name="n1", capabilities=["tooling"],
                     status=NodeStatus.online, max_concurrent=4),
                Node(id="node_n2", name="n2", capabilities=["tooling"],
                     status=NodeStatus.online, max_concurrent=4)]

    def test_bare_pin_matches_registered_node_prefixed(self):
        s = AffinityScheduler()
        node, pinned = s.choose_pinned(["tooling"], self._nodes(), {},
                                       pinned_node_id="n2")
        assert pinned and node is not None and node.id == "node_n2", (
            "a lane reported as 'n2' must pin to registered 'node_n2', "
            f"not park; got ({node and node.id}, {pinned})")

    def test_prefixed_pin_matches_bare_registered_node(self):
        s = AffinityScheduler()
        nodes = [Node(id="n1", name="n1", capabilities=["tooling"],
                      status=NodeStatus.online, max_concurrent=4)]
        node, pinned = s.choose_pinned(["tooling"], nodes, {},
                                       pinned_node_id="node_n1")
        assert pinned and node is not None and node.id == "n1"

    def test_foreign_spelling_collision_still_parks(self):
        s = AffinityScheduler()
        node, pinned = s.choose_pinned(["tooling"], self._nodes(), {},
                                       pinned_node_id="node_n9")
        assert node is None and pinned, "unknown node parks, never substitutes"


# ---------------------------------------------------------------------------
# by-design controls (must hold on main too)
# ---------------------------------------------------------------------------

class TestByDesignControls:
    def test_laneless_tasks_unaffected(self, client):
        _register(client, "node_n1")
        t = _submit(client, "no lane", "")
        c = client.post("/api/v1/schedule/trigger").json()
        assert c["scheduled"] == 1
        assert client.get(f"/api/v1/tasks/{t}").json()["assigned_to"] == "node_n1"

    def test_second_report_same_lane_upserts(self, client):
        nid = _register(client, "node_n1")
        for sid in ("sid_v1", "sid_v2"):
            r = client.post("/api/v1/lanes/report",
                            json={"lane_key": "L", "node_id": nid,
                                  "session_id": sid},
                            headers={"X-Peer-Node": nid})
            assert r.status_code == 200
        lane = client.get("/api/v1/lanes/L").json()
        assert lane["session_id"] == "sid_v2"
        assert len(client.get("/api/v1/lanes").json()["lanes"]) == 1

    def test_report_never_clobbers_a_known_session_with_empty(self, client):
        """A node-id-only report (e.g. a bare re-home probe) must not wipe
        main's knowledge of the lane's session."""
        nid = _register(client, "node_n1")
        client.post("/api/v1/lanes/report",
                    json={"lane_key": "L", "node_id": nid,
                          "session_id": "sid_keep"},
                    headers={"X-Peer-Node": nid})
        r = client.post("/api/v1/lanes/report",
                        json={"lane_key": "L", "node_id": nid},
                        headers={"X-Peer-Node": nid})
        assert r.status_code == 200
        assert client.get("/api/v1/lanes/L").json()["session_id"] == "sid_keep"
