"""#892 factory resilience — worker disk preflight.

The 2026-09-14 05:32Z incident: node_windows_pc_worker filled its disk, kept
claiming tasks and killed every lane it took (OSError: [Errno 28] in
concurrent_log_handler, later rc=120) while the main kept routing every
``review`` task to it, because every heartbeat forces status=online and the
scheduler only filters on status. Four layers, one fix:

  1. worker_connector reports ``disk_free_gb`` in every join/heartbeat payload;
  2. the executor REFUSES to claim when free disk < node.min_free_disk_gb
     (cluster YAML, default 5 GB, never an env var) and logs one clear line;
  3. the main degrades a below-floor node (excluded from scheduling) while the
     report is low and restores it automatically when it recovers, with the
     reason visible in GET /api/v1/nodes;
  4. the lane runner writes a first-line sentinel into the task's result file
     BEFORE starting hermes, so a crashed lane always leaves a diagnostic.

No behaviour change when the field is absent (older workers): every disk rule
requires a KNOWN reading, and None never fires.

Fake convention (per the brief): monkeypatch shutil.disk_usage.
"""

import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hermes_cluster.core.disk_gate import disk_gate_blocks
from hermes_cluster.core.disk_preflight import (
    DEFAULT_MIN_FREE_DISK_GB,
    disk_free_gb,
    disk_reason,
    effective_min_free_gb,
    is_below_floor,
)
from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
)


def _fake_usage(monkeypatch, free_gb):
    """Patch shutil.disk_usage with a fake 100 GB volume holding *free_gb*."""
    fake = shutil._ntuple_diskusage(  # type: ignore[attr-defined]
        total=100 * 1024**3,
        used=(100 - free_gb) * 1024**3,
        free=int(free_gb * 1024**3),
    )
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: fake)


def _executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


# ---------------------------------------------------------------------------
# 0. the pure rules
# ---------------------------------------------------------------------------

def test_pure_floor_rules():
    assert is_below_floor(4.9, 5.0) is True
    assert is_below_floor(5.1, 5.0) is False
    assert is_below_floor(None, 5.0) is False        # unknown ≠ below
    assert is_below_floor(0.1, 0.0) is False         # floor disabled
    assert effective_min_free_gb(None) == DEFAULT_MIN_FREE_DISK_GB == 5.0
    assert effective_min_free_gb(2.5) == 2.5
    r = disk_reason(1.0, 5.0)
    assert "disk below floor" in r and "min_free_disk_gb" in r
    assert disk_reason(9.0, 5.0) == ""


def test_disk_free_gb_unreadable_returns_none(monkeypatch):
    def _boom(_p):
        raise OSError("statvfs failed")
    monkeypatch.setattr(shutil, "disk_usage", _boom)
    assert disk_free_gb("C:/whatever") is None


# ---------------------------------------------------------------------------
# 1. the worker reports disk_free_gb in join + heartbeat payloads
# ---------------------------------------------------------------------------

def test_heartbeat_payload_sent_with_disk(monkeypatch):
    """One connector loop iteration must carry disk_free_gb in BOTH the join
    and the heartbeat POST payloads (measured 0.25 GB via fake disk_usage)."""
    from hermes_cluster.core import worker_connector as wc

    _fake_usage(monkeypatch, 0.25)
    sent = []

    def _post(endpoint, path, data, token, node_id, **kw):
        sent.append((path, dict(data)))
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

    def _sleep(_s):
        raise SystemExit  # run exactly one loop iteration
    monkeypatch.setattr(wc.time, "sleep", _sleep)
    wc._connector_started = False
    wc.start_worker_connector(
        node_id="w", cluster_endpoint="http://main:1", capabilities=["review"],
        peer_token="t", disk_probe_path="C:/lanes",
    )
    try:
        holder["loop"]()
    except SystemExit:
        pass
    join_posts = [p for p in sent if p[0].endswith("/join")]
    hb_posts = [p for p in sent if p[0].endswith("/heartbeat")]
    assert join_posts and hb_posts, sent
    assert join_posts[0][1]["disk_free_gb"] == pytest.approx(0.25)
    assert hb_posts[0][1]["disk_free_gb"] == pytest.approx(0.25)


def test_models_accept_and_tolerate_disk_field():
    from hermes_cluster.models import HeartbeatRequest, JoinRequest
    assert HeartbeatRequest(**{"node_id": "n", "disk_free_gb": 0.5}).disk_free_gb == 0.5
    assert HeartbeatRequest(**{"node_id": "n"}).disk_free_gb is None  # old payload
    assert JoinRequest(**{"node_name": "w", "disk_free_gb": 0.5}).disk_free_gb == 0.5
    assert JoinRequest(**{"node_name": "w"}).disk_free_gb is None


# ---------------------------------------------------------------------------
# 2. the executor refuses to claim below the floor
# ---------------------------------------------------------------------------

def test_claim_guard_blocks_below_floor_and_logs(monkeypatch, caplog):
    _fake_usage(monkeypatch, 1.0)
    with caplog.at_level("WARNING"):
        blocked = disk_gate_blocks("C:/lanes", 5.0)
    assert blocked is True
    lines = [r for r in caplog.records if "refusing to claim" in r.getMessage()]
    assert len(lines) == 1  # ONE clear line


def test_claim_guard_passes_above_floor(monkeypatch, caplog):
    _fake_usage(monkeypatch, 50.0)
    with caplog.at_level("WARNING"):
        assert disk_gate_blocks("C:/lanes", 5.0) is False
    assert not [r for r in caplog.records if "refusing to claim" in r.getMessage()]


def test_claim_guard_disabled_zero_and_unknown(monkeypatch):
    _fake_usage(monkeypatch, 0.0)
    assert disk_gate_blocks("C:/lanes", 0.0) is False   # explicitly disabled

    def _boom(_p):
        raise OSError
    monkeypatch.setattr(shutil, "disk_usage", _boom)
    assert disk_gate_blocks("C:/lanes", 5.0) is False   # unknown → no refusal


def test_claim_cycle_refuses_and_fetches_nothing(monkeypatch, tmp_path):
    """Wired into AgentExecutor._claim_and_spawn: below floor, the cycle must
    return BEFORE the GET /api/v1/tasks poll (tasks stay unclaimed)."""
    from hermes_cluster.core import agent_executor as ae_mod

    _fake_usage(monkeypatch, 0.5)
    calls = []
    monkeypatch.setattr(
        ae_mod, "_signed_request",
        lambda *a, **kw: calls.append(a[1:3]) or [],
    )
    ex = AgentExecutor(
        config=AgentExecutorConfig(enabled=True, worker="hermes",
                                   working_dir=str(tmp_path),
                                   min_free_disk_gb=5.0),
        node_id="w", cluster_endpoint="http://main:1", peer_token="t",
    )
    ex._claim_and_spawn(2)
    assert calls == [], f"tasks fetched despite full disk: {calls}"


def test_config_key_is_yaml_only_no_env():
    """Owner ruling: node.min_free_disk_gb comes from the cluster YAML; no
    config path may read an env var named for it."""
    repo = Path(__file__).resolve().parent.parent
    disk_reads = []
    for f in (repo / "hermes_cluster").rglob("*.py"):
        txt = f.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(txt.splitlines(), 1):
            low = line.lower()
            if "min_free" in low and "environ" in low:
                disk_reads.append(f"{f.name}:{i}: {line.strip()}")
    assert disk_reads == []
    serve = (repo / "hermes_cluster" / "serve.py").read_text(encoding="utf-8")
    assert 'cfg["node"].get("min_free_disk_gb"' in serve  # the YAML wiring


# ---------------------------------------------------------------------------
# 3. the main degrades a below-floor node and restores it automatically
# ---------------------------------------------------------------------------

def _mk_nm(min_free_gb=None):
    from hermes_cluster.core.node_manager import NodeManager
    from hermes_cluster.state import ClusterState
    st = ClusterState()
    return NodeManager(st, min_free_disk_gb=min_free_gb), st


def test_heartbeat_below_floor_degrades_above_restores():
    from hermes_cluster.models import Task, TaskStatus
    nm, st = _mk_nm(5.0)
    nm.join("node_w", name="w", capabilities=["review"], disk_free_gb=50.0)
    n = st.get_node("node_w")
    assert n.status.value == "online" and n.disk_free_gb == 50.0

    # full disk: same node, fresh heartbeat, below floor → degraded + reason
    nm.send_heartbeat("node_w", disk_free_gb=0.4)
    n = st.get_node("node_w")
    assert n.status.value == "degraded"
    assert "disk below floor" in n.status_reason

    # the scheduler excludes it even with a fresh heartbeat
    st._tasks["t1"] = Task(id="t1", title="review something",
                           requires=["review"], status=TaskStatus.ready)
    assignments = st.schedule_pending_detailed()
    assert all(a["node_id"] != "node_w" for a in assignments), assignments

    # recovery: a report above the floor re-forces online, reason cleared
    nm.send_heartbeat("node_w", disk_free_gb=60.0)
    n = st.get_node("node_w")
    assert n.status.value == "online" and n.status_reason == ""

    # and work can now land on it again
    st._tasks["t1"].status = TaskStatus.ready
    assignments = st.schedule_pending_detailed()
    assert any(a["node_id"] == "node_w" for a in assignments), assignments


def test_older_worker_without_field_is_untouched():
    nm, st = _mk_nm(5.0)
    nm.join("node_old", name="old", capabilities=["review"])   # no disk field
    nm.send_heartbeat("node_old")
    n = st.get_node("node_old")
    assert n.status.value == "online" and n.disk_free_gb is None
    nm.send_heartbeat("node_old", disk_free_gb=None)
    assert st.get_node("node_old").status.value == "online"


def test_join_below_floor_degrades_at_registration():
    nm, st = _mk_nm(5.0)
    nm.join("node_low", name="low", capabilities=["review"], disk_free_gb=0.1)
    n = st.get_node("node_low")
    assert n.status.value == "degraded" and "disk below floor" in n.status_reason


def test_watchdog_disk_rule_fires_on_last_report_and_restores():
    from hermes_cluster.core.watchdog import Watchdog, HeartbeatNode

    class Reg:
        def __init__(self, d):
            self.d = d
            self.status = {}
        def get_all_heartbeat_nodes(self):
            return [HeartbeatNode("n1", datetime.utcnow(),
                                  self.status.get("n1", "online"),
                                  disk_free_gb=self.d)]
        def update_node_status(self, node_id, status, reason=""):
            self.status[node_id] = status
            self.reason = reason

    reg = Reg(1.0)
    wd = Watchdog(reg, degraded_after=15, offline_after=30,
                  min_free_disk_gb=5.0)
    events = wd.check_now()
    assert [e.event_type for e in events] == ["degraded"]
    assert "disk below floor" in reg.reason

    # disk recovers (worker reports above floor) → restored automatically
    reg.d = 40.0
    events = wd.check_now()
    assert [e.event_type for e in events] == ["online"]

    # older worker: disk None → rule can never fire (staleness-only watchdog)
    reg2 = Reg(None)
    wd2 = Watchdog(reg2, min_free_disk_gb=5.0)
    assert wd2.check_now() == []
    assert reg2.status.get("n1", "online") == "online"


def test_nodes_api_surfaces_disk_and_reason(tmp_path):
    from fastapi.testclient import TestClient
    from hermes_cluster.app import create_app

    app = create_app(cluster_id="t892", node_id="node_main", node_role="main",
                     node_min_free_disk_gb=5.0,
                     db_path=str(tmp_path / "c892.db"))
    client = TestClient(app)
    r = client.post("/api/v1/nodes/join", json={
        "node_name": "pc", "capabilities": ["review"], "disk_free_gb": 0.2})
    node_id = r.json()["node_id"]
    node = [n for n in client.get("/api/v1/nodes").json()
            if n["id"] == node_id][0]
    assert node["disk_free_gb"] == pytest.approx(0.2)
    assert node["status"] == "degraded"
    assert "disk below floor" in node["status_reason"]

    # heartbeat recovers → back online automatically, reason cleared
    client.post("/api/v1/nodes/heartbeat",
                json={"node_id": node_id, "disk_free_gb": 80.0})
    node = [n for n in client.get("/api/v1/nodes").json()
            if n["id"] == node_id][0]
    assert node["status"] == "online" and node["status_reason"] in ("", None)

    # older-worker payload (no disk field at all) → pre-#892 behaviour
    r2 = client.post("/api/v1/nodes/join",
                     json={"node_name": "old", "capabilities": ["tooling"]})
    old_id = r2.json()["node_id"]
    client.post("/api/v1/nodes/heartbeat", json={"node_id": old_id})
    old = [n for n in client.get("/api/v1/nodes").json()
           if n["id"] == old_id][0]
    assert old["status"] == "online" and old["disk_free_gb"] is None
    # stop background threads the app started (watchdog/heartbeat/reaper)
    nm = app_state_node_manager(app)
    if nm:
        nm.stop_watchdog()
        nm.stop_heartbeat_sender()


def app_state_node_manager(app):
    for r in getattr(app, "router", MagicMock()).routes:
        pass
    # the managers live on the state object wired at create time
    try:
        from hermes_cluster.routers import nodes as nodes_mod
        return getattr(nodes_mod._state, "_node_manager", None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 4. a crashed lane always leaves a FIRST LINE diagnostic in its result file
# ---------------------------------------------------------------------------
#
# Brief wording: "writes a first line to the task's result file BEFORE
# starting Hermes so a crashed lane always leaves a diagnostic." The literal
# pre-spawn write to result.md was implemented, then REVERTED on measured
# evidence: it re-opens the exact #868 wound — Windows ``Path.rename`` fails
# FileExistsError [WinError 183] onto a pre-existing target (the executor's
# own test child, tests_v3/test_agent_executor_result_file_868.py, dies on it
# the moment result.md pre-exists), and #868's contract is that the
# deliverable path must not exist at spawn. The sentinel+diagnostic is
# therefore written at REAP for every lane that died without a deliverable
# (rc!=0, and the rc==0-no-result / lost paths). All five 05:32Z deaths were
# reap-visible (rc=120 / Errno 28 stderr) — the incident's goal, "a crashed
# lane always leaves a diagnostic," is met with zero collision against #868.

def test_crashed_lane_leaves_first_line_diagnostic(monkeypatch, tmp_path):
    captured = {}

    class _FakeProc:
        pid = 777
        stderr = None
        stdout = None
        def poll(self):
            return None

    def fake_popen(cmd, **kw):
        captured["cmd"] = cmd
        # #868 contract preserved: the deliverable path does NOT pre-exist.
        rp = tmp_path / "hermes-results" / "task_h1.result.md"
        assert not rp.exists(), "executor must not create result.md at spawn"
        return _FakeProc()

    monkeypatch.setattr("hermes_cluster.core.agent_executor.subprocess.Popen",
                        fake_popen)
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    executor._spawn_hermes_worker({"id": "task_h1", "title": "crashy",
                                   "description": "d"})
    assert captured["cmd"]

    # simulate the death: rc=120 with only the last stderr line known
    spawn = executor._active_spawns["task_h1"]
    spawn.process.poll = lambda: 120
    spawn.spawn_exit_stderr = "OSError: [Errno 28] No space left on device"
    with patch.object(executor, "_capture_spawn_exits"):
        resolved = []
        executor._reap_hermes_spawn("task_h1", spawn, 15.0, resolved)
    assert resolved and resolved[0][2] == "spawn_failed"
    body = (tmp_path / "hermes-results" / "task_h1.result.md").read_text(
        encoding="utf-8")
    assert body.startswith("<!-- LANE-STARTED:")       # FIRST LINE diagnostic
    assert "No space left on device" in body           # stderr detail inside
    assert "rc=120" in body


def test_delivered_lane_never_rewritten(monkeypatch, tmp_path):
    """A lane that delivered is untouched even when it exits nonzero later —
    the sentinel path applies ONLY to a missing/sentinel-first file."""
    ex = _executor(worker="hermes", working_dir=str(tmp_path))
    rp = ex._hermes_result_path("t7")
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text("# The actual deliverable\n\nVERDICT: PASS\n", encoding="utf-8")
    spawn = ActiveSpawn(task_id="t7", task_title="t", process=MagicMock(),
                        mode="hermes", result_path=str(rp),
                        started_at=__import__("time").time())
    ex._append_crash_diagnostics(spawn, "\nCRASH BLOCK\n")
    assert rp.read_text(encoding="utf-8") == "# The actual deliverable\n\nVERDICT: PASS\n"


def test_sentinel_only_rc0_is_not_a_done(tmp_path):
    """The #870 false-completion trap: the executor's OWN sentinel must never
    pass the non-empty-content gate at rc=0 — it falls through to the
    transcript/no-deliverable paths."""
    ex = _executor(worker="hermes", working_dir=str(tmp_path))
    rp = ex._hermes_result_path("t8")
    assert ex._write_result_sentinel(rp, "t8") is True
    spawn = ActiveSpawn(
        task_id="t8", task_title="t", process=MagicMock(), mode="hermes",
        result_path=str(rp), stdout_path=str(ex._hermes_stdout_path("t8")),
        stderr_path=str(ex._hermes_stderr_path("t8")),
        started_at=__import__("time").time(),
    )
    spawn.process.poll.return_value = 0
    spawn.process.pid = 1
    ex._active_spawns["t8"] = spawn
    with patch.object(ex, "_capture_spawn_exits"):
        resolved = []
        ex._reap_hermes_spawn("t8", spawn, 3.0, resolved)
    assert len(resolved) == 1
    outcome = resolved[0][2]
    assert outcome != "done", "sentinel-only file must NOT reap as done"
    assert outcome in {"no_result", "lost_deliverable", "transcript_promoted"}
    # and a promoted/done path can't carry a bare sentinel body
    assert AgentExecutor._content_after_sentinel(
        f"{AgentExecutor.RESULT_SENTINEL}\n<!-- task: t9 -->\n") == ""


def test_no_behaviour_change_without_field_sqlite(tmp_path):
    """Store-level: a heartbeat payload without disk info keeps the exact
    pre-#892 force-online semantics; below-floor degrades; recovery clears."""
    from hermes_cluster.state.cluster_store import ClusterStore
    from hermes_cluster.models import Node
    st = ClusterStore(db_path=str(tmp_path / "c.db"))
    try:
        st.register_node(Node(id="n1", name="n1"))
        st.update_heartbeat("n1")
        n = st.get_node("n1")
        assert n.status.value == "online" and n.disk_free_gb is None
        st.update_heartbeat("n1", disk_free_gb=0.1,
                            status_reason="disk below floor: x")
        n = st.get_node("n1")
        assert n.status.value == "degraded" and n.disk_free_gb == 0.1
        st.update_heartbeat("n1", disk_free_gb=42.0)
        n = st.get_node("n1")
        assert n.status.value == "online" and n.status_reason == ""
        assert n.disk_free_gb == 42.0
    finally:
        st.close()
