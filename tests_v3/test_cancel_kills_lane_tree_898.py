"""#898 item 0 — a cancelled task must KILL the lane's whole process tree.

Measured on the live cluster 2026-09-14 (shared/claude-plugins#898 note
138234): cancelling a task left the spawned lane session (pid 64584) AND its
two children alive on the worker, and the zombie kept submitting reviewer
tasks. The executor had NO cancel path at all — _reap_finished_spawns only
looks at lane state / result files / timeout, never at whether main still
considers the task live. Cancel revoked the lease and set
`cancel_requested`, and the executor happily kept the spawn "active" forever
until the outer spawn_timeout.

Pinned here:
  1. a poll cycle that sees its running task in cancel_requested (or a
     terminal cancelled/failed) on main terminates the spawn's PROCESS TREE
     (not just the direct child) and acks the cancel;
  2. the kill is a tree kill — a real grandchild is dead too, not orphaned;
  3. the timeout path also tree-kills (same orphan-children defect class);
  4. main refuses kanban_cluster_submit from a lane whose OWN task is no
     longer active (source_task_id gate) — a zombie that escapes the kill
     cannot mint new tasks.

RED on main: nothing polls task status for active spawns (test 1 shows the
spawn still alive + no kill call), and submit ignores source_task_id.
"""

import json
import os
import subprocess
import sys
import time
from unittest.mock import patch

import pytest

from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
)
from hermes_cluster.state.cluster_store import ClusterStore


def _store():
    return ClusterStore(db_path=":memory:")


def _executor(store, **cfg_overrides):
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
    def __init__(self, pid=64584):
        self.pid = pid
        self.stderr = None
        self.stdout = None

    def poll(self):
        return None


class _DeadProc:
    def __init__(self, pid=64584):
        self.pid = pid
        self.stderr = None
        self.stdout = None

    def poll(self):
        return -15


# Child script: prints its own pid, spawns a grandchild that outlives it,
# then sleeps. Used by the REAL tree-kill proof (test 2).
_TREE_SCRIPT = """
import subprocess, sys, time, os
child = subprocess.Popen([sys.executable, "-c",
                          "import time; time.sleep(120)"])
print(child.pid, flush=True)
time.sleep(120)
"""


def _seed_running_spawn(executor, task_id="tX", lane_key="L", proc=None):
    spawn = ActiveSpawn(
        task_id=task_id, task_title="cancelling?",
        process=proc or _AliveProc(), started_at=time.time(),
        lane_name=f"hermes-{task_id}", mode="hermes",
        lane_key=lane_key, role="author",
        result_path="",
    )
    with executor._lock:
        executor._active_spawns[task_id] = spawn
    executor._persist_spawn(spawn)
    return spawn


def _tasks_payload(store, task_id, status):
    """Shape of GET /api/v1/tasks as the executor sees it."""
    return [{"id": task_id, "title": "t", "description": "", "status": status,
             "assigned_to": "test-node", "priority": 3, "lane_key": "L",
             "role": "author"}]


# ---------------------------------------------------------------------------
# 1. Cancel detected -> tree kill + ack (mechanism test, mocked kill runner)
# ---------------------------------------------------------------------------

def test_cancel_requested_task_kills_spawn_and_acks(tmp_path):
    store = _store()
    executor = _executor(store, working_dir=str(tmp_path))
    spawn = _seed_running_spawn(executor)

    kill_calls = []

    def fake_run(cmd, **kw):
        kill_calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0)

    signed_calls = []

    def fake_signed(endpoint, method, path, data, token, node_id, timeout=15):
        signed_calls.append((method, path, data))
        if method == "GET":
            return _tasks_payload(store, "tX", "cancel_requested")
        return {"status": "cancelled"}

    with patch("hermes_cluster.core.agent_executor.subprocess.run", fake_run), \
         patch("hermes_cluster.core.agent_executor._signed_request", fake_signed):
        executor._handle_cancellations()

    assert kill_calls, (
        "#898: cancel_requested task must terminate the spawn process — "
        "on main nothing ever kills it and the zombie lane keeps submitting")
    # A TREE kill, not a bare pid kill: the command must include the
    # tree flag (/T on Windows taskkill; process-group term on POSIX).
    joined = " ".join(kill_calls[0]).lower()
    assert "/t" in joined or "killpg" in joined or str(-spawn.process.pid) in joined, (
        f"kill must cover the tree, got: {kill_calls[0]}")
    # The cancel is ACKED to main (two-phase protocol: worker's report
    # closes cancel_requested -> cancelled).
    posts = [c for c in signed_calls if c[0] == "POST" and c[1].endswith("/fail")]
    assert posts, "worker must ack the cancel via /fail (closes to cancelled)"
    assert "cancel" in (posts[0][2] or {}).get("reason", "").lower()
    # The spawn record is gone: it cannot be double-killed or resurrected.
    with executor._lock:
        assert "tX" not in executor._active_spawns


def test_cancelled_or_failed_task_also_terminates_spawn(tmp_path):
    """A task already terminal (e.g. another actor cancelled it while the
    lease expiry lagged) must not keep a live lane either."""
    for terminal_status in ("cancelled", "failed"):
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        _seed_running_spawn(executor, task_id="tY")
        kills = []
        with patch("hermes_cluster.core.agent_executor.subprocess.run",
                   lambda cmd, **kw: kills.append(cmd)), \
             patch("hermes_cluster.core.agent_executor._signed_request",
                   lambda e, m, p, d, t, n, timeout=15:
                   _tasks_payload(store, "tY", terminal_status) if m == "GET" else {}):
            executor._handle_cancellations()
        assert kills, f"{terminal_status} task must terminate its spawn (tY)"


def test_running_task_is_left_alone(tmp_path):
    """The poll must not kill a merely-slow lane: status `running` stays live."""
    store = _store()
    executor = _executor(store, working_dir=str(tmp_path))
    _seed_running_spawn(executor, task_id="tZ")
    kills = []
    with patch("hermes_cluster.core.agent_executor.subprocess.run",
               lambda cmd, **kw: kills.append(cmd)), \
         patch("hermes_cluster.core.agent_executor._signed_request",
               lambda e, m, p, d, t, n, timeout=15:
               _tasks_payload(store, "tZ", "running") if m == "GET" else {}):
        executor._handle_cancellations()
    assert kills == [], "a running task's spawn must NOT be killed"
    with executor._lock:
        assert "tZ" in executor._active_spawns


# ---------------------------------------------------------------------------
# 2. REAL process tree: a grandchild must die too (the measured failure)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    sys.platform not in ("win32", "linux", "darwin"),
    reason="tree-kill proof covers Windows + POSIX")
def test_tree_kill_really_kills_grandchildren(tmp_path):
    store = _store()
    executor = _executor(store, working_dir=str(tmp_path))
    script = tmp_path / "tree_child.py"
    script.write_text(_TREE_SCRIPT, encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        if os.name == "nt" else 0,
    )
    try:
        grandchild_pid = int(proc.stdout.readline().strip())
        spawn = ActiveSpawn(
            task_id="tTree", task_title="tree", process=proc,
            started_at=time.time(), lane_name="hermes-tTree", mode="hermes",
            result_path="",
        )
        executor._kill_process_tree(spawn)
        deadline = time.time() + 10
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            time.sleep(0.2)
        # proc.poll() reaps the Win32 handle; a not-None returncode proves
        # the direct child died. (os.kill(pid, 0) alone cannot: on Windows
        # the object lingers while OUR handle is open.)
        assert proc.poll() is not None, "direct child must be dead"
        # Grandchild probe: its pid must no longer be a live process.
        deadline = time.time() + 10
        while time.time() < deadline:
            if not executor._pid_alive(grandchild_pid):
                break
            time.sleep(0.2)
        assert not executor._pid_alive(grandchild_pid), (
            "#898: a TREE kill must take the grandchildren with it — "
            f"grandchild pid {grandchild_pid} survived (the measured bug)")
    finally:
        for p in (proc,):
            try:
                p.kill()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 3. The timeout path shares the tree kill (same defect class)
# ---------------------------------------------------------------------------

def test_timeout_uses_tree_kill(tmp_path):
    store = _store()
    executor = _executor(store, working_dir=str(tmp_path))
    spawn = _seed_running_spawn(executor, task_id="tTo")
    kills = []
    with patch.object(executor, "_kill_process_tree",
                      lambda s: kills.append(s.process.pid)), \
         patch("hermes_cluster.core.agent_executor._signed_request",
               lambda e, m, p, d, t, n, timeout=15:
               _tasks_payload(store, "tTo", "running") if m == "GET" else {}):
        spawn.started_at = time.time() - 10 ** 6  # blow the timeout
        executor._config.spawn_timeout = 1
        executor._reap_finished_spawns()
    assert kills == [spawn.process.pid], (
        "spawn timeout must go through the same tree kill (#898 orphan class)")


# ---------------------------------------------------------------------------
# 4. main refuses submit from a lane whose OWN task is no longer active
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient
from hermes_cluster.app import create_app


@pytest.fixture
def client():
    return TestClient(
        create_app(cluster_id="test-cluster", node_id="test-node", node_role="main")
    )


def _new_task(client, title, lane_key="", **extra):
    body = {"title": title}
    if lane_key:
        body["lane_key"] = lane_key
    body.update(extra)
    return client.post("/api/v1/tasks", json=body)


def test_zombie_lane_source_task_cancelled_is_refused(client):
    """The escape hatch behind the kill: a session that survived cancellation
    must not be able to mint NEW tasks. source_task_id names the lane's own
    (now cancelled) delivery — 409 with the reason."""
    own = _new_task(client, "lane work", "zombielane").json()
    assert client.post(f"/api/v1/tasks/{own['id']}/cancel").status_code == 200

    r = client.post("/api/v1/tasks", json={
        "title": "REVIEW zombielane PR", "lane_key": "zombielane-rev",
        "source_task_id": own["id"],
    })
    assert r.status_code == 409, (
        f"#898: a cancelled session must be refused submit, got {r.status_code}")
    detail = json.dumps(r.json()).lower()
    assert "not active" in detail or "cancelled" in detail


def test_active_source_task_submits_normally(client):
    own = _new_task(client, "lane work", "livelane")
    r = client.post("/api/v1/tasks", json={
        "title": "REVIEW livelane PR", "lane_key": "livelane-rev",
        "source_task_id": own.json()["id"],
    })
    assert r.status_code == 200
    assert not r.json().get("deduped")


def test_unknown_source_task_id_is_refused(client):
    """A source_task_id that names nothing is a lie or a bug — refuse it.
    (Silently accepting would let a stale env var from a recycled container
    authorize anything.)"""
    r = client.post("/api/v1/tasks", json={
        "title": "REVIEW nothing PR", "source_task_id": "task_doesnotexist",
    })
    assert r.status_code == 409


def test_no_source_task_id_keeps_legacy_behaviour(client):
    """Manual/lead submits carry no source_task_id — unchanged."""
    r = _new_task(client, "plain manual task")
    assert r.status_code == 200
