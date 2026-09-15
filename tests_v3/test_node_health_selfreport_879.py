"""#879 — a worker can be 100% CPU-saturated with a duplicate executor and
the cluster still reports it ``online`` with an empty ``status_reason``: make
the node report its own health.

The incident shape (measured on windows_desktop_worker): a second executor
for one node id doubled every lane and pinned the box at 100% CPU, while
``GET /api/v1/nodes`` stayed ``online``/``status_reason=""`` the whole time —
the connector's own thread beats healthy regardless of what the machine is
doing, and every beat forced ``online`` (the unconditional force #892 already
removed for disk). The fix mirrors the #892 precedent:

  1. the worker_connector rides ``cpu_load_pct`` / ``lane_count`` /
     ``duplicate_executor`` on EVERY join and heartbeat payload (absent
     field = older worker = rule inert — pre-#879 semantics hold);
  2. the executor exposes the observations (#899's persisted-refresh
     already RE-ATTACHES a duplicate's spawns — that live set IS the signal,
     and it self-heals once the duplicate's lanes drain);
  3. the main thresholds them (CPU vs node.max_cpu_load — YAML only, same
     owner ruling as #892; lane_count vs the node's declared max_concurrent;
     duplicate_executor is self-evident) and degrades the node WITH A REASON
     visible in GET /api/v1/nodes; the watchdog re-fires on the stored
     readings so its own online-force can never undo the degradation;
  4. a refused duplicate (409 at /join) is LOUD in the challenger's log.

Fake convention (per the #892 suite): monkeypatch the probes, never real CPU.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_cluster.core.health_selfreport import (
    CpuSampler,
    health_reason,
    is_cpu_saturated,
    is_lane_backlog,
)
from hermes_cluster.models import HeartbeatRequest, JoinRequest, Node


# ---------------------------------------------------------------------------
# 0. the pure main-side rules
# ---------------------------------------------------------------------------

def test_pure_health_rules():
    # CPU: rule inert on None or ceiling<=0; fires at/above the ceiling
    assert is_cpu_saturated(0.95, 0.9) is True
    assert is_cpu_saturated(0.9, 0.9) is True      # at == saturated
    assert is_cpu_saturated(0.89, 0.9) is False
    assert is_cpu_saturated(None, 0.9) is False    # unknown != saturated
    assert is_cpu_saturated(1.0, 0.0) is False     # rule disabled
    # lanes: fires only OVER the node's own declared ceiling
    assert is_lane_backlog(3, 2) is True
    assert is_lane_backlog(2, 2) is False          # at ceiling = legit
    assert is_lane_backlog(None, 2) is False       # older worker
    assert is_lane_backlog(9, 0) is False          # 0 = unlimited, no ceiling
    # composed reason: fixed order, all three conditions, '' when nothing
    r = health_reason(0.95, 0.9, 4, 2, True)
    assert "duplicate executor" in r and "lane backlog" in r and "cpu saturated" in r
    assert r.index("duplicate") < r.index("lane backlog") < r.index("cpu saturated")
    assert health_reason(None, 0.9, 2, 2, None) == ""
    assert health_reason(None, 0.9, None, None, False) == ""


# ---------------------------------------------------------------------------
# 1. the worker-side probes: safe, absent-when-unknown, never real CPU in CI
# ---------------------------------------------------------------------------

def test_windows_cpu_delta_math():
    """The pure seam between two (idle, kernel, user) GetSystemTimes tuples."""
    # 100% busy window: idle did not advance at all
    assert CpuSampler._busy_from_samples((100, 200, 100),
                                         (100, 250, 150)) == pytest.approx(1.0)
    # half busy: idle advanced as much as the non-idle time
    assert CpuSampler._busy_from_samples((0, 0, 0),
                                         (50, 50, 50)) == pytest.approx(0.5)
    # first sample primes: prev None -> None
    assert CpuSampler._busy_from_samples(None, (1, 1, 1)) is None
    # degenerate window -> None, never a divide-by-zero
    assert CpuSampler._busy_from_samples((0, 0, 0), (0, 0, 0)) is None


def test_posix_cpu_measurement(monkeypatch):
    from hermes_cluster.core import health_selfreport as hs
    # Windows has no os.getloadavg — set the attribute regardless of platform
    monkeypatch.setattr(sys, "platform", "linux", raising=False)
    monkeypatch.setattr(os, "getloadavg", lambda: (8.0, 4.0, 2.0), raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    assert CpuSampler().measure() == pytest.approx(2.0)   # 8/4 cores

    def boom():
        raise OSError("no loadavg")
    monkeypatch.setattr(os, "getloadavg", boom, raising=False)
    assert CpuSampler().measure() is None                 # unreadable -> omit

    monkeypatch.setattr(os, "getloadavg", lambda: (1.0, 0.5, 0.2), raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 0)
    assert CpuSampler().measure() is None                 # no cores -> omit


# ---------------------------------------------------------------------------
# 2. payload models: absent-by-default (older-worker contract)
# ---------------------------------------------------------------------------

def test_models_accept_and_tolerate_health_fields():
    hb = HeartbeatRequest(**{"node_id": "n", "cpu_load_pct": 0.42,
                             "lane_count": 5, "duplicate_executor": True})
    assert hb.cpu_load_pct == 0.42 and hb.lane_count == 5
    assert hb.duplicate_executor is True
    old = HeartbeatRequest(**{"node_id": "n"})
    assert old.cpu_load_pct is None and old.lane_count is None
    assert old.duplicate_executor is None
    j = JoinRequest(**{"node_name": "w", "cpu_load_pct": 0.99, "lane_count": 3})
    assert j.cpu_load_pct == 0.99 and j.lane_count == 3
    assert j.duplicate_executor is None
    n = Node(id="x", name="x", capabilities=[])
    assert n.cpu_load_pct is None and n.lane_count is None
    assert n.duplicate_executor is None


# ---------------------------------------------------------------------------
# 3. the main degrades on the self-report and restores on a healthy beat
# ---------------------------------------------------------------------------

def _mk_nm(max_cpu=None):
    from hermes_cluster.core.node_manager import NodeManager
    from hermes_cluster.state import ClusterState
    st = ClusterState()
    return NodeManager(st, max_cpu_load=max_cpu), st


def test_heartbeat_duplicate_executor_degrades_and_restores():
    nm, st = _mk_nm()
    nm.join("node_w", name="w", capabilities=["review"], max_concurrent=2)
    n = st.get_node("node_w")
    assert n.status.value == "online" and n.status_reason == ""

    # survivor observes a duplicate's spawn records -> next beat is degraded
    nm.send_heartbeat("node_w", duplicate_executor=True)
    n = st.get_node("node_w")
    assert n.status.value == "degraded"
    assert "duplicate executor" in n.status_reason
    assert n.duplicate_executor is True

    # the duplicate drained -> flag clears -> ONLINE again automatically
    nm.send_heartbeat("node_w", duplicate_executor=False)
    n = st.get_node("node_w")
    assert n.status.value == "online" and n.status_reason == ""


def test_heartbeat_lane_count_over_ceiling_degrades_with_reason():
    nm, st = _mk_nm()
    nm.join("node_w2", name="w2", capabilities=["review"], max_concurrent=1)
    nm.send_heartbeat("node_w2", lane_count=3)  # doubled executor
    n = st.get_node("node_w2")
    assert n.status.value == "degraded" and "lane backlog" in n.status_reason
    # at the ceiling = legit load, never degrades
    nm.send_heartbeat("node_w2", lane_count=1)
    assert st.get_node("node_w2").status.value == "online"


def test_heartbeat_cpu_rule_respects_config():
    nm, st = _mk_nm(max_cpu=None)   # rule DISABLED by default
    nm.join("node_c", name="c", capabilities=["review"])
    nm.send_heartbeat("node_c", cpu_load_pct=0.99)
    assert st.get_node("node_c").status.value == "online"  # opt-in rule only

    nm2, st2 = _mk_nm(max_cpu=0.9)
    nm2.join("node_c2", name="c2", capabilities=["review"])
    nm2.send_heartbeat("node_c2", cpu_load_pct=0.99)
    n = st2.get_node("node_c2")
    assert n.status.value == "degraded" and "cpu saturated" in n.status_reason
    nm2.send_heartbeat("node_c2", cpu_load_pct=0.3)
    assert st2.get_node("node_c2").status.value == "online"


def test_join_with_bad_health_degrades_at_registration():
    nm, st = _mk_nm()
    nm.join("node_d", name="d", capabilities=["review"], max_concurrent=1,
            lane_count=4, duplicate_executor=True)
    n = st.get_node("node_d")
    assert n.status.value == "degraded"
    assert "lane backlog" in n.status_reason
    assert "duplicate executor" in n.status_reason


def test_disk_and_health_reasons_compose_and_scheduler_excludes():
    from hermes_cluster.models import Task, TaskStatus
    nm, st = _mk_nm(0.9)
    nm.join("node_e", name="e", capabilities=["review"], max_concurrent=2)
    nm.send_heartbeat("node_e", disk_free_gb=0.1, cpu_load_pct=0.95)
    n = st.get_node("node_e")
    assert n.status.value == "degraded"
    assert "disk below floor" in n.status_reason
    assert "cpu saturated" in n.status_reason

    st._tasks["t1"] = Task(id="t1", title="review something",
                           requires=["review"], status=TaskStatus.ready)
    assignments = st.schedule_pending_detailed()
    assert all(a["node_id"] != "node_e" for a in assignments), assignments

    # healthy beat restores: both rules clear together
    nm.send_heartbeat("node_e", disk_free_gb=80.0, cpu_load_pct=0.1)
    n = st.get_node("node_e")
    assert n.status.value == "online" and n.status_reason == ""
    st._tasks["t1"].status = TaskStatus.ready
    assignments = st.schedule_pending_detailed()
    assert any(a["node_id"] == "node_e" for a in assignments), assignments


def test_older_worker_without_health_fields_is_untouched():
    nm, st = _mk_nm(0.5)  # even with the CPU rule ARMED
    nm.join("node_old", name="old", capabilities=["review"])
    nm.send_heartbeat("node_old")                      # bare beat
    n = st.get_node("node_old")
    assert n.status.value == "online"
    assert n.cpu_load_pct is None and n.lane_count is None
    assert n.duplicate_executor is None


# ---------------------------------------------------------------------------
# 4. the watchdog re-fires on stored readings (cannot undo the degradation)
# ---------------------------------------------------------------------------

def test_watchdog_health_rule_sticks_and_self_heals():
    from hermes_cluster.core.watchdog import Watchdog, HeartbeatNode

    class Reg:
        def __init__(self, node):
            self.node = node
            self.status = {}
            self.reason = ""
        def get_all_heartbeat_nodes(self):
            self.node.status = self.status.get("n1", self.node.status)
            return [self.node]
        def update_node_status(self, node_id, status, reason=""):
            self.status[node_id] = status
            self.reason = reason

    sick = HeartbeatNode("n1", datetime.utcnow(), "online",
                         cpu_load_pct=0.97, lane_count=5,
                         max_concurrent=2, duplicate_executor=True)
    reg = Reg(sick)
    wd = Watchdog(reg, degraded_after=15, offline_after=30, max_cpu_load=0.9)
    events = wd.check_now()
    assert [e.event_type for e in events] == ["degraded"]
    assert "duplicate executor" in reg.reason
    assert "lane backlog" in reg.reason

    # the beat that follows carries a clean self-report; watchdog check with
    # the updated stored readings must force online again (self-heal)
    reg.node = HeartbeatNode("n1", datetime.utcnow(), "degraded",
                             cpu_load_pct=0.2, lane_count=1,
                             max_concurrent=2, duplicate_executor=False)
    events = wd.check_now()
    assert [e.event_type for e in events] == ["online"]

    # older worker: no health fields -> rules can never fire
    reg2 = Reg(HeartbeatNode("n1", datetime.utcnow(), "online"))
    wd2 = Watchdog(reg2, max_cpu_load=0.5)
    assert wd2.check_now() == []
    assert reg2.status.get("n1", "online") == "online"


# ---------------------------------------------------------------------------
# 5. the connector rides the fields (join + every beat); 409 is loud
# ---------------------------------------------------------------------------

def _drive_one_connector_cycle(wc, monkeypatch, sent, responses=None):
    """Capture the connector loop (no real thread), run exactly one
    join+heartbeat cycle, and stop it with SystemExit inside sleep — the
    established fake convention from test_disk_preflight_892."""
    calls = {"i": 0}

    def _post(endpoint, path, data, token, node_id, **kw):
        sent.append((path, dict(data)))
        if responses:
            return responses[min(calls["i"], len(responses) - 1)]
        calls["i"] += 1
        if path.endswith("/join"):
            return {"node_id": "node_w"}
        return {"status": "ok"}

    monkeypatch.setattr(wc, "_signed_post", _post)

    holder = {}

    class _T:
        def __init__(self, target=None, **kw):
            holder["loop"] = target
        def start(self):
            pass

    monkeypatch.setattr(wc.threading, "Thread", _T)

    state = {"first": True}

    def _sleep(_s):
        if state["first"]:
            state["first"] = False   # let one join+beat pass...
            return
        raise SystemExit             # ...then stop the loop
    monkeypatch.setattr(wc.time, "sleep", _sleep)
    wc._connector_started = False
    return holder


def test_connector_carries_health_fields_on_join_and_beat(monkeypatch):
    from hermes_cluster.core import worker_connector as wc

    # CPU: pin the module's sampler seam (no real CPU in tests)
    monkeypatch.setattr(wc, "cpu_load_pct", lambda sampler=None: 0.95)

    sent = []
    spawns = [SimpleNamespace(started_at=0.0), SimpleNamespace(started_at=0.0)]
    holder = _drive_one_connector_cycle(wc, monkeypatch, sent)
    wc.start_worker_connector(
        node_id="w", cluster_endpoint="http://main:1", capabilities=["review"],
        peer_token="t", max_concurrent=1,
        health_fn=lambda: (spawns, False),
    )
    try:
        holder["loop"]()
    except SystemExit:
        pass
    join_posts = [p for p in sent if p[0].endswith("/join")]
    hb_posts = [p for p in sent if p[0].endswith("/heartbeat")]
    assert join_posts and hb_posts, sent
    for _path, data in (join_posts[0], hb_posts[0]):
        assert data["cpu_load_pct"] == 0.95
        assert data["lane_count"] == 2
        assert data["duplicate_executor"] is False
        assert data["node_name" if "node_name" in data else "node_id"] in ("w", "node_w")


def test_connector_omits_fields_without_probe(monkeypatch):
    """No executor wired (health_fn=None) and an unreadable CPU: the payload
    must simply omit the fields — the older-worker contract the main honours."""
    from hermes_cluster.core import worker_connector as wc

    monkeypatch.setattr(wc, "cpu_load_pct", lambda sampler=None: None)
    sent = []
    holder = _drive_one_connector_cycle(wc, monkeypatch, sent)
    wc.start_worker_connector(
        node_id="w", cluster_endpoint="http://main:1", capabilities=["review"],
        peer_token="t",
    )
    try:
        holder["loop"]()
    except SystemExit:
        pass
    assert sent, "loop produced no payloads"
    for _path, data in sent:
        assert "cpu_load_pct" not in data
        assert "lane_count" not in data
        assert "duplicate_executor" not in data


def test_health_probe_error_never_kills_the_beat(monkeypatch):
    from hermes_cluster.core import worker_connector as wc

    monkeypatch.setattr(wc, "cpu_load_pct", lambda sampler=None: 0.1)

    def bad_probe():
        raise RuntimeError("executor wedged")

    sent = []
    holder = _drive_one_connector_cycle(wc, monkeypatch, sent)
    wc.start_worker_connector(
        node_id="w", cluster_endpoint="http://main:1", capabilities=["review"],
        peer_token="t", health_fn=bad_probe,
    )
    try:
        holder["loop"]()
    except SystemExit:
        pass
    hb = [d for _p, d in sent if _p.endswith("/heartbeat")]
    assert hb, "probe error killed the heartbeat loop"
    assert "lane_count" not in hb[0]          # fields omitted, beat survived
    assert hb[0]["cpu_load_pct"] == 0.1       # CPU still rides


def test_join_409_is_loud_and_retried(monkeypatch, caplog):
    from hermes_cluster.core import worker_connector as wc

    monkeypatch.setattr(wc, "cpu_load_pct", lambda sampler=None: None)

    sent = []
    holder = _drive_one_connector_cycle(
        wc, monkeypatch, sent,
        responses=[{"__http_error__": 409}, {"node_id": "node_w"}])
    wc.start_worker_connector(
        node_id="w", cluster_endpoint="http://main:1", capabilities=["review"],
        peer_token="t", instance_token="tok",
    )
    with caplog.at_level("ERROR"):
        try:
            holder["loop"]()
        except SystemExit:
            pass
    joins = [p for p in sent if p[0].endswith("/join")]
    assert len(joins) >= 2, f"409 must retry the join, got {sent}"
    err = [r for r in caplog.records if "REFUSED 409" in r.getMessage()]
    assert err, "the refused duplicate must log LOUDLY"


# ---------------------------------------------------------------------------
# 6. the executor exposes the observations (#899's refresh IS the signal)
# ---------------------------------------------------------------------------

def test_executor_duplicate_observation_set_drains_with_records():
    from hermes_cluster.core.agent_executor import (
        AgentExecutor, AgentExecutorConfig)

    ex = AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60),
        node_id="test-node", cluster_endpoint="http://127.0.0.1:9999")

    class Store:
        records: list = []
        def get_all_task_spawns(self):
            return list(self.records)

    st = Store()
    ex._store = st
    assert ex.duplicate_executor_observed() is False
    # a second executor writes a record; the refresh sees it AND re-attaches
    # it — which is exactly what makes the survivor's lane_count see the
    # duplicate's spawn (the over-ceiling backlog signal).
    st.records = [{"task_id": "t_foreign", "pid": 1}]
    ex._last_persisted_refresh = 0.0
    ex._persisted_refresh_s = 0
    ex._refresh_persisted_ids()
    assert ex.duplicate_executor_observed() is True
    assert [s.task_id for s in ex.live_spawns()] == ["t_foreign"]
    # the duplicate's lane drains (record deleted) -> self-heals
    st.records = []
    ex._last_persisted_refresh = 0.0
    ex._refresh_persisted_ids()
    assert ex.duplicate_executor_observed() is False


def test_config_key_is_yaml_only_no_env():
    """Owner ruling (from #892): node.max_cpu_load comes from the cluster
    YAML; no config path may read an env var named for it."""
    repo = Path(__file__).resolve().parent.parent
    hits = []
    for f in (repo / "hermes_cluster").rglob("*.py"):
        txt = f.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(txt.splitlines(), 1):
            low = line.lower()
            if "max_cpu_load" in low and "environ" in low:
                hits.append(f"{f.name}:{i}: {line.strip()}")
    assert hits == []
    serve = (repo / "hermes_cluster" / "serve.py").read_text(encoding="utf-8")
    assert 'cfg["node"].get("max_cpu_load"' in serve  # the YAML wiring


# ---------------------------------------------------------------------------
# 7. the API surface: reported health is visible; scheduler-safe degradation
# ---------------------------------------------------------------------------

def _node_manager_of(app):
    try:
        from hermes_cluster.routers import nodes as nodes_mod
        return getattr(nodes_mod._state, "_node_manager", None)
    except Exception:
        return None


def test_nodes_api_surfaces_health_and_reason(tmp_path):
    from fastapi.testclient import TestClient
    from hermes_cluster.app import create_app

    app = create_app(cluster_id="t879", node_id="node_main", node_role="main",
                     node_min_free_disk_gb=5.0, node_max_cpu_load=0.9,
                     db_path=str(tmp_path / "c879.db"))
    client = TestClient(app)
    try:
        r = client.post("/api/v1/nodes/join", json={
            "node_name": "desk", "capabilities": ["review"],
            "max_concurrent": 1,
            "cpu_load_pct": 0.99, "lane_count": 2,
            "duplicate_executor": False})
        assert r.status_code == 200, r.text
        node_id = r.json()["node_id"]
        node = [n for n in client.get("/api/v1/nodes").json()
                if n["id"] == node_id][0]
        assert node["cpu_load_pct"] == pytest.approx(0.99)
        assert node["status"] == "degraded"
        assert "cpu saturated" in node["status_reason"]

        # healthy beat restores automatically
        client.post("/api/v1/nodes/heartbeat", json={
            "node_id": node_id, "cpu_load_pct": 0.1, "lane_count": 1,
            "duplicate_executor": False})
        node = [n for n in client.get("/api/v1/nodes").json()
                if n["id"] == node_id][0]
        assert node["status"] == "online"
        assert node["status_reason"] in ("", None)

        # older-worker payload (no health fields at all) -> pre-#879 behaviour
        r2 = client.post("/api/v1/nodes/join",
                         json={"node_name": "old",
                               "capabilities": ["tooling"]})
        old_id = r2.json()["node_id"]
        client.post("/api/v1/nodes/heartbeat", json={"node_id": old_id})
        old = [n for n in client.get("/api/v1/nodes").json()
               if n["id"] == old_id][0]
        assert old["status"] == "online" and old["cpu_load_pct"] is None
    finally:
        nm = _node_manager_of(app)
        if nm:
            nm.stop_watchdog()


def test_sqlite_roundtrip_persists_health(tmp_path):
    from hermes_cluster.state.cluster_store import ClusterStore
    st = ClusterStore(db_path=str(tmp_path / "h879.db"))
    try:
        st.register_node(Node(id="node_h", name="h", capabilities=["review"],
                              max_concurrent=1, cpu_load_pct=0.5,
                              lane_count=7, duplicate_executor=True))
        n = st.get_node("node_h")
        assert n.cpu_load_pct == 0.5 and n.lane_count == 7
        assert n.duplicate_executor is True
        # a bare (older-worker) beat keeps the stored readings — absent = unknown
        st.update_heartbeat("node_h")
        assert st.get_node("node_h").lane_count == 7
        # an explicit False clears
        st.update_heartbeat("node_h", duplicate_executor=False, lane_count=0)
        n = st.get_node("node_h")
        assert n.duplicate_executor is False and n.lane_count == 0
    finally:
        st.close()


def test_postgres_store_heartbeat_sql_binds_all_fields():
    """The PG update_heartbeat builds its SET clause dynamically; prove the
    placeholder numbering aligns with the params (asyncpg binds by position)
    without a live server."""
    import asyncio
    from hermes_cluster.state.postgres_store import PostgresClusterStore

    calls = []

    async def fake_fetch(sql, *params):
        calls.append((sql, params))

    store = PostgresClusterStore.__new__(PostgresClusterStore)
    store._fetch = fake_fetch
    asyncio.run(store.update_heartbeat(
        "n", load=0.0, disk_free_gb=1.0, status_reason="r",
        cpu_load_pct=0.9, lane_count=3, duplicate_executor=True))
    sql, params = calls[0]
    # 8 set values + the WHERE id placeholder: $1..$9 with 9 params
    for i in range(1, 10):
        assert f"${i}" in sql, sql
    assert "WHERE id = $9" in sql
    assert len(params) == 9
    assert params[8] == "n"
