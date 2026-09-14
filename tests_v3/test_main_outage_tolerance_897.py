"""#897 — main-outage tolerance on the worker side (issue direction 3).

Measured twice today (bdaya-website-infra#301 release window, 08:33Z; and a
mid-day Autopilot preemption, 09:14Z): the hosted main is unreachable or
RESTARTED for minutes. Two concrete worker-side failures followed, both
pinned RED against main here:

  1. UNKNOWN-NODE ORPHANING. The connector's own NOTE on main documents it:
     after a main restart the worker's heartbeat is dropped ("heartbeat for
     unknown node") but the router still answers {"status":"ok"}, so the
     connector cannot detect it and the worker stays orphaned until the
     WORKER process restarts. Fix has two ends:
       * main: /api/v1/nodes/heartbeat answers 200
         {"status":"unknown_node","node_id":...} for unregistered ids —
         still 200 so a 4xx never makes an old connector treat it as auth.
       * worker: on that status, clear registered_id -> the existing loop
         re-JOINs next cycle (join is idempotent on main: node_manager.
         join() re-registers + refreshes heartbeat).

  2. LOST TERMINAL REPORTS. When a lane finishes while main is unreachable,
     _report_completion/_report_failure fire exactly once, log an error, and
     the result is GONE — the spawn was already popped from _active_spawns,
     so the task sits running until lease expiry and recovery reschedules
     (double-spending a worker on work that already succeeded). Fix: a
     pending-report queue retried every poll cycle with exponential backoff,
     capped; the task is never marked failed because main is unreachable.

BY-DESIGN CONTROLS (must also pass on main, pinning existing guarantees):
  * claim-poll transport failure keeps the loop alive and retries (no task
    is failed, no crash).
  * reap keeps waiting on lane-status query failure (the _870 contract).

Every test imports production modules INSIDE the test body so reds are the
defect's assertion, not an ImportError.
"""

import json
import threading
import time
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# 1a. main side: heartbeat answers DISTINGUISHABLY for unknown nodes
# ---------------------------------------------------------------------------

def _app_client():
    from fastapi.testclient import TestClient
    from hermes_cluster.app import create_app
    app = create_app(cluster_id="c897", node_id="m", node_role="main")
    return TestClient(app)


def test_heartbeat_known_node_plain_ok():
    """Control + setup: a joined node's heartbeat keeps answering ok."""
    client = _app_client()
    r = client.post("/api/v1/nodes/join",
                    json={"node_name": "pc_worker", "capabilities": ["tooling"]})
    assert r.status_code == 200, r.text
    node_id = r.json()["node_id"]
    r = client.post("/api/v1/nodes/heartbeat", json={"node_id": node_id})
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


def test_heartbeat_unknown_node_is_distinguishable():
    """RED on main: the router returns {"status":"ok"} for unknown node ids —
    the connector can never detect the orphaning this file exists to fix."""
    client = _app_client()
    r = client.post("/api/v1/nodes/heartbeat", json={"node_id": "node_ghost"})
    assert r.status_code == 200, "200 (not 4xx) so old connectors stay compatible"
    body = r.json()
    assert body.get("status") == "unknown_node", f"RED on main: {body!r}"
    assert body.get("node_id") == "node_ghost"


# ---------------------------------------------------------------------------
# 1b. worker side: connector re-joins on unknown_node, survives transport errors
# ---------------------------------------------------------------------------

class _PostScript:
    """Scripted fake for worker_connector._signed_post."""

    def __init__(self, heartbeat_script):
        # each entry: dict response, or None (transport failure)
        self._hb = list(heartbeat_script)
        self.calls = []

    def __call__(self, endpoint, path, data, token, node_id, timeout=10):
        self.calls.append(path)
        if path.endswith("/heartbeat"):
            nxt = self._hb.pop(0) if self._hb else {"status": "ok"}
            return nxt
        if path.endswith("/join"):
            return {"node_id": "node_" + data["node_name"], "status": "registered"}
        return {"status": "ok"}


def _run_connector_briefly(patched_post, interval=0.02, seconds=1.0):
    """Start the real connector thread with _signed_post patched; let it
    cycle; stop it. (The module's _connector_started idempotency guard is
    reset around the run — threads are daemons, nothing leaks past assert.)"""
    from hermes_cluster.core import worker_connector as wc

    with patch.object(wc, "_connector_started", False), \
         patch.object(wc, "_signed_post", patched_post):
        wc.start_worker_connector(
            node_id="pc_worker",
            cluster_endpoint="http://main.invalid:8787",
            capabilities=["tooling"],
            peer_token="test-token-not-secret",
            heartbeat_interval=interval,
        )
        time.sleep(seconds)
    return patched_post.calls


def test_connector_rejoins_after_unknown_node_heartbeat():
    """RED on main: heartbeat responses are discarded entirely, so an
    unknown_node answer never re-triggers join. On main there are ZERO
    /join calls after the first (registered via join before orphaning)."""
    # script: join succeeds (first cycle), then ONE unknown_node heartbeat,
    # then plain oks. A fixed connector must see: join, heartbeat, join,
    # heartbeat...
    script = [{"status": "unknown_node", "node_id": "node_pc_worker"}]
    posts = _PostScript(script)
    calls = _run_connector_briefly(posts, seconds=0.6)

    joins = [c for c in calls if c.endswith("/join")]
    assert len(joins) >= 2, (
        "connector must re-join after main reports the node unknown "
        f"(#897 orphaning); calls: {calls[:10]}")


def test_connector_ignores_transport_failure_without_deregistering():
    """BY-DESIGN control, GREEN on main too: heartbeat transport failure
    (None) is NOT an unknown-node signal — one join only, then heartbeats.
    (Runs the first cycle with a None queued after the join.)"""
    script = [None, None, None, None]
    posts = _PostScript(script)
    calls = _run_connector_briefly(posts, seconds=0.4)
    joins = [c for c in calls if c.endswith("/join")]
    assert len(joins) == 1, f"transport failures must not re-join storm: {calls[:10]}"
    assert any(c.endswith("/heartbeat") for c in calls)


# ---------------------------------------------------------------------------
# 2. agent executor: terminal reports survive a main outage
# ---------------------------------------------------------------------------

def _bare_executor():
    from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig
    cfg = AgentExecutorConfig(poll_interval=0.05)
    ex = AgentExecutor(
        config=cfg,
        node_id="pc_worker",
        cluster_endpoint="http://main.invalid:8787",
    )
    return ex


def test_report_completion_retries_until_main_returns():
    """RED on main: _report_completion fires EXACTLY ONCE; if main is down the
    completion is lost (spawn already popped). Fix: queue + retry."""
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()

    attempts = {"n": 0}

    def flaky(endpoint, method, path, data, token, node_id, timeout=15):
        attempts["n"] += 1
        if attempts["n"] < 3:
            return None          # main unreachable
        return {"status": "ok"}

    with patch.object(ae, "_signed_request", flaky):
        ex._report_completion("task_abc", detail="lane completed")
        # drive the retry queue with a virtual clock so the test never sleeps
        # on wall time: _drain_pending_reports(now=...) honours backoff.
        drained = getattr(ex, "_drain_pending_reports", None)
        assert drained is not None, (
            "AgentExecutor must expose _drain_pending_reports(now=None) "
            "(#897): retry queue for terminal reports")
        t0 = time.monotonic()
        for i in range(60):
            drained(now=t0 + 2.0 * i)              # 2 s ticks beat the 0.5 s base
            if not getattr(ex, "_pending_reports", {}):
                break
    assert not getattr(ex, "_pending_reports", {}), "report left in queue"
    assert attempts["n"] >= 3


def test_lost_report_survives_many_cycles_then_gets_flushed():
    """Queue the completion of a lane while main is fully down for 10 poll
    cycles, then bring main back: exactly one successful /complete POST."""
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()

    state = {"down": True, "posts": []}

    def fake(endpoint, method, path, data, token, node_id, timeout=15):
        if state["down"]:
            return None
        state["posts"].append(path)
        return {"status": "ok"}

    with patch.object(ae, "_signed_request", fake):
        ex._report_completion("task_x")           # main down -> must queue
        assert getattr(ex, "_pending_reports", {}), "completion must be queued, not lost"
        drain = ex._drain_pending_reports
        t0 = time.monotonic()
        for i in range(10):
            drain(now=t0)                          # main down: still queued
            assert ex._pending_reports
        state["down"] = False
        for i in range(60):
            drain(now=t0 + 2.0 * i)                # virtual clock advances
            if not ex._pending_reports:
                break
    assert state["posts"] == ["/api/v1/tasks/task_x/complete"], state["posts"]


def test_failure_report_is_queued_like_completion():
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()
    with patch.object(ae, "_signed_request",
                      lambda *a, **k: None):
        ex._report_failure("task_y", "lane state=failed")
    q = getattr(ex, "_pending_reports", {})
    assert any("task_y" in str(v) or "task_y" in k for k, v in q.items()), \
        f"failure reports must also be queued: {q!r}"


def test_backoff_bounds_stay_under_recovery_ttl():
    """Design pin: retry intervals grow exponentially but the first delay
    must be small enough that a queued completion usually lands BEFORE the
    main's lease expiry/reschedule (duplicate-lane prevention), and the cap
    must never be zero (no hot spin)."""
    from hermes_cluster.core import agent_executor as ae
    ex = _bare_executor()
    t0 = time.monotonic()
    with patch.object(ae, "_signed_request", lambda *a, **k: None):
        ex._report_completion("task_z")
    entry = list(ex._pending_reports.values())[0]
    delay = entry["next_at"] - t0
    assert 0.0 < delay <= 60.0, f"first retry delay: {delay}"


# ---------------------------------------------------------------------------
# 3. plugin tool surface: a ONE-SECOND GCLB blip must not fail the call
# ---------------------------------------------------------------------------

def test_plugin_api_call_retries_transport_error_then_succeeds():
    """#897 direction 3: kanban_cluster_* tools during the rollout window got
    URLError('Remote end closed connection')/503-body connection resets and
    the LANE treated that as a task failure. _api_call must retry transport
    failures (with backoff), returning the parsed body once main is back.
    Red on main: the first URLError becomes {"error": ...} directly."""
    from hermes_cluster import plugin
    from urllib.error import URLError

    calls = {"n": 0}

    class _Resp:
        def read(self):
            return b'{"status": "ok", "tasks": []}'
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise URLError("<urlopen error [Errno 61] Connection refused>")
        return _Resp()

    with patch.object(plugin, "_base_url", "http://main.invalid:8787"), \
         patch.object(plugin, "urlopen", fake_urlopen), \
         patch.object(plugin.time, "sleep"):        # no wall time in tests
        out = plugin._api_call("GET", "/api/v1/tasks")
    assert "error" not in out, f"RED on main (one shot, no retry): {out!r}"
    assert out.get("status") == "ok"
    assert calls["n"] == 3


def test_plugin_api_call_still_surfaces_http_error_bodies():
    """Control (GREEN on main too): an HTTPError with a parseable JSON body is
    returned as-is — retries are for transport failures, not for rewriting
    real API answers."""
    import urllib.error as uerr
    from hermes_cluster import plugin

    class _Resp:
        def read(self):
            return b'{"error": "task not found"}'
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    with patch.object(plugin, "_base_url", "http://main.invalid:8787"), \
         patch.object(plugin, "urlopen", lambda req, timeout=None: _Resp()):
        out = plugin._api_call("POST", "/api/v1/tasks/9999/complete")
    assert out.get("error") == "task not found"


def test_plugin_api_call_real_httperror_not_retried_body_decoded():
    """Reviewer note on PR #50 @ 6f57a2b (applied): a REAL urllib HTTPError
    (4xx/5xx — main IS reachable) must NOT be swallowed by the URLError
    retry branch: no retries (one call only) and the response body is
    decoded and returned, so a 409/404 JSON error from the main surfaces
    intact instead of degrading to 'HTTP Error 404: Not Found'.

    RED before the narrowing: HTTPError subclasses URLError, so the bare
    `except URLError` retried it twice and returned str(e) (the reason
    phrase, body lost)."""
    import io
    import urllib.error as uerr
    from hermes_cluster import plugin

    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise uerr.HTTPError(
            "http://main.invalid:8787/api/v1/tasks/x/fail", 409,
            "Conflict", {}, io.BytesIO(b'{"error": "already terminal"}'))

    with patch.object(plugin, "_base_url", "http://main.invalid:8787"), \
         patch.object(plugin, "urlopen", fake_urlopen), \
         patch.object(plugin.time, "sleep"):
        out = plugin._api_call("POST", "/api/v1/tasks/x/fail", {"reason": "r"})
    assert calls["n"] == 1, f"HTTPError must not be retried, calls={calls['n']}"
    assert out.get("error") == "already terminal", \
        f"RED pre-narrowing (str(HTTPError), body lost): {out!r}"
