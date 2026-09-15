"""#909 TDDD-1 fence: main's affinity scheduler gets a lane-pin DATA SOURCE.

Measured 2026-09-15 on hosted main: 30 of 375 lane_keys had deliveries
assigned to different nodes ("bounces"), 22 of them DATED AFTER the #858
affinity fix deployed. The machinery was correct but starved: record_lane()
wrote only the WORKER-local lanes table; main's lane_nodes map stayed empty;
pinned_node_id was always '' at choose time. A bounced delivery silently
cold-starts a fresh Hermes session on a machine with neither the session nor
the clone — LFP-1's entire saving leaking.

Covers (issue fix-shape items 1-3):
  * POST /api/v1/lanes/report upserts into MAIN's store and the NEXT
    delivery of that lane pins to the reported node end-to-end (report ->
    lanes row -> scheduler). RED today: no /lanes route exists at all
    (main's GET /api/v1/lanes 404'd — measured).
  * both node-id spellings (macbook_worker / node_macbook_worker) resolve
    to the registered id, one row;
  * a worker CANNOT pin to another node's machine (body node_id mismatch
    vs authenticated X-Peer-Node -> 403);
  * GET /api/v1/lanes + /api/v1/lanes/{key} read surface;
  * executor mirrors placement on spawn-persist AND reap-capture,
    best-effort: a main outage never blocks/fails a spawn.
"""
import hashlib
import hmac
import json
import os
import time
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.agent_executor import (
    AgentExecutor, AgentExecutorConfig, ActiveSpawn)
from hermes_cluster.models import Node, NodeStatus
from hermes_cluster.routers import lanes as lanes_mod

PEER = "macbook_worker"
PEER_TOKEN = "t-secret-909"


def _app_with_peer_auth():
    os.environ["PEER_TOKEN"] = "main-token-909"
    os.environ["PEER_TOKENS"] = f"{PEER}:{PEER_TOKEN}"
    try:
        return create_app(cluster_id="t", node_id="main", node_role="main")
    finally:
        os.environ.pop("PEER_TOKEN", None)
        os.environ.pop("PEER_TOKENS", None)


def _sign(method, path, body: bytes):
    ts = int(time.time())
    bh = hashlib.sha256(body).hexdigest()
    sig = hmac.new(PEER_TOKEN.encode(),
                   f"{PEER}:{ts}:{method}:{path}:{bh}".encode(),
                   hashlib.sha256).hexdigest()
    return {"X-Peer-Node": PEER, "X-Peer-Timestamp": str(ts),
            "X-Peer-Signature": sig, "Content-Type": "application/json"}


def _signed_get(c, path):
    # middleware signs request.url.path VERBATIM (httpx keeps the percent-
    # encoding), so sign the raw path the request was made with.
    return c.get(path, headers=_sign("GET", path, b""))


def _report(c, lane_key, node_id="", **kw):
    body = json.dumps({"lane_key": lane_key, "node_id": node_id, **kw}).encode()
    return c.post("/api/v1/lanes/report", content=body,
                  headers=_sign("POST", "/api/v1/lanes/report", body))


# ------------------------------------------------------------------ the API

def test_report_lands_in_main_store_and_read_surface():
    app = _app_with_peer_auth()
    with TestClient(app) as c:
        lanes_mod._state.register_node(Node(id=f"node_{PEER}", name=PEER))
        r = _report(c, "invora-flutter-env-dev", PEER,
                    session_id="sid42", role="author", last_task_id="task_1")
        assert r.status_code == 200, r.text
        lane = r.json()["lane"]
        assert lane["node"] == f"node_{PEER}", \
            "reported node must normalize to the REGISTERED id"
        g = _signed_get(c, "/api/v1/lanes/invora-flutter-env-dev")
        assert g.status_code == 200 and g.json()["session_id"] == "sid42"
        # a lane_key WITH '#' round-trips too (the {lane_key:path} param gets
        # the raw segment; percent-encoding it in a GET path is a Starlette
        # decode-before-match hazard, so real clients read it via the list).
        r2 = _report(c, "invora-flutter#env/dev", PEER, session_id="s#")
        assert r2.status_code == 200, r2.text
        lst = _signed_get(c, "/api/v1/lanes")
        assert lst.status_code == 200
        assert sorted(x["lane_key"] for x in lst.json()) == [
            "invora-flutter#env/dev", "invora-flutter-env-dev"]


def test_both_spellings_resolve_to_one_row():
    app = _app_with_peer_auth()
    with TestClient(app) as c:
        lanes_mod._state.register_node(Node(id=f"node_{PEER}", name=PEER))
        r1 = _report(c, "L", "")                       # body empty -> header
        r2 = _report(c, "L", PEER, session_id="s2")    # bare spelling
        r3 = _report(c, "L", f"node_{PEER}")           # registered spelling
        nodes = {r1.json()["lane"]["node"], r2.json()["lane"]["node"],
                 r3.json()["lane"]["node"]}
        assert nodes == {f"node_{PEER}"}
        assert lanes_mod._state.get_all_lanes() and \
            len(lanes_mod._state.get_all_lanes()) == 1, "no per-spelling twin row"


def test_worker_cannot_pin_another_nodes_machine():
    app = _app_with_peer_auth()
    with TestClient(app) as c:
        lanes_mod._state.register_node(Node(id="node_windows_pc", name="windows_pc"))
        r = _report(c, "L", "node_windows_pc")   # signed as macbook_worker
        assert r.status_code == 403, r.text
        assert "does not match authenticated peer" in r.text
        assert _signed_get(c, "/api/v1/lanes/L").status_code == 404  # lie left nothing


def test_empty_lane_key_refused_and_unauth_denied():
    app = _app_with_peer_auth()
    with TestClient(app) as c:
        assert _report(c, "", PEER).status_code == 400
        r = c.post("/api/v1/lanes/report",
                   json={"lane_key": "L", "node_id": PEER})  # unsigned
        assert r.status_code == 401


# ------------------------------------------------ end-to-end: pin follows

def test_reported_lane_pins_next_delivery():
    """THE measured failure closed: a lane reported to main pins its next
    delivery even when another node would win the fair tiebreak."""
    app = _app_with_peer_auth()
    with TestClient(app) as c:
        st = lanes_mod._state
        # n_far registers FIRST — the fair planner's tiebreak pick without
        # a pin (exactly the bounce shape measured on hosted main).
        st.register_node(Node(id="node_n_far", name="n_far",
                              capabilities=["tooling"], max_concurrent=2))
        st.register_node(Node(id=f"node_{PEER}", name=PEER,
                              capabilities=["tooling"], max_concurrent=2))
        r = _report(c, "bayader-flutter#env/dev", PEER, session_id="warm")
        assert r.status_code == 200
        st.create_task("t1", "next delivery", ["tooling"],
                       lane_key="bayader-flutter#env/dev")
        st.trigger_pending_tasks()
        assert st.schedule_pending() == 1
        task = st.get_task("t1")
        assert task.assigned_to == f"node_{PEER}", (
            f"delivery must pin to the lane's home node, got {task.assigned_to!r}")


# ------------------------------------------------------- executor reporting

def _executor(**cfg):
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id=PEER,
        cluster_endpoint="http://main.test",
        peer_token=PEER_TOKEN,
    )


def _spawn():
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 1
    proc.stderr = None
    return ActiveSpawn(task_id="tX", task_title="x", process=proc,
                       lease_id="lX", started_at=time.time(),
                       lane_name="hermes-tX", lane_key="L#main",
                       role="author", session_id="s1")


def test_spawn_persist_reports_placement():
    ex = _executor()
    with patch("hermes_cluster.core.agent_executor._signed_request") as sr:
        ex._report_lane_placement("L#main", "s1", "author", "tX")
    posts = [cl for cl in sr.call_args_list
             if cl.args[1] == "POST" and cl.args[2] == "/api/v1/lanes/report"]
    assert len(posts) == 1
    body = posts[0].args[3]
    assert body["lane_key"] == "L#main" and body["node_id"] == PEER
    assert body["session_id"] == "s1"
    assert posts[0].args[5] == PEER  # signed AS US (X-Peer-Node == our id)
    # token resolves through the estate chain (env > token file > explicit):
    # assert present, not its value — the value is node-owned.
    assert posts[0].args[4]


def test_reap_capture_reports_placement():
    """_touch_lane_from_spawn (reap-time session capture) must mirror too —
    the captured session id is exactly what the pin needs to be useful."""
    ex = _executor()
    spawn = _spawn()
    ex._store = MagicMock()
    with patch.object(ex, "_read_spawn_stderr", return_value="session_id: sFRESH"), \
         patch.object(ex, "_extract_session_id", return_value="sFRESH"), \
         patch.object(ex, "_report_lane_placement") as rep:
        ex._touch_lane_from_spawn(spawn, "tX")
    rep.assert_called_once_with("L#main", "sFRESH", "author", "tX")


def test_report_failure_never_raises_into_the_lane_path():
    ex = _executor()
    with patch("hermes_cluster.core.agent_executor._signed_request",
               side_effect=OSError("main unreachable")):
        ex._report_lane_placement("L#main", "s1", "author", "tX")  # no raise


def test_no_token_or_endpoint_skips_quietly():
    ex = AgentExecutor(config=AgentExecutorConfig(enabled=True),
                       node_id=PEER, cluster_endpoint="", peer_token="")
    with patch("hermes_cluster.core.agent_executor._signed_request") as sr:
        ex._report_lane_placement("L", "s", "author", "t")
    sr.assert_not_called()
