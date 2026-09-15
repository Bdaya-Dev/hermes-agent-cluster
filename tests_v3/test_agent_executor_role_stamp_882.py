"""#882 follow-through: the NATIVE hermes spawn path must stamp the lane role.

The reviewer dispatch path (`bdaya-dispatch run --role reviewer`) already stamps
BDAYA_LANE_ROLE into the lane's settings env — the #882 enforcement mechanism.
The cluster executor's native hermes worker mode (`worker: hermes`,
`_spawn_hermes_worker`) did NOT: it spawned every task — author or reviewer —
with a bare `dict(os.environ)` and no lane env at all (measured on
windows_desktop_worker 2026-09-14: a role='reviewer' lane session saw no
BDAYA_LANE_ROLE/BDAYA_LANE_KEY/BDAYA_LANE_ID, and every lanes.db on the node
had zero rows because the hosted main is Postgres-backed while the gate's
3rd role source reads a SQLite path).

Consequence: the #882 readonly gates (`readonly_reviewer_gate.py`,
`readonly-lane-*-guard.js`) can never recognise a reviewer lane dispatched this
way — they resolve the role from exactly those markers — so READ-ONLY stayed
prose on the executor's primary lane path even after !921.

These tests pin the contract the guards key on:
  - role='reviewer'  -> BDAYA_LANE_ROLE=reviewer reaches the child env
  - author/default   -> BDAYA_LANE_ROLE=author (explicit; never inherited)
  - a stale BDAYA_LANE_ROLE in the executor's own env is ALWAYS overridden
  - BDAYA_LANE_KEY / BDAYA_LANE_ID carry the lane_key (registry + diagnostics)
  - a durable lane-role-<id>.json marker is written before spawn for
    crash-resume visibility (the dispatch path's registry equivalent)
"""

import json
import os
from pathlib import Path

from hermes_cluster.core.agent_executor import (
    AgentExecutor,
    AgentExecutorConfig,
)


def _executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


class _FakeProc:
    pid = 9911
    stderr = None
    stdout = None

    def poll(self):
        return None


def _capture_spawn(monkeypatch, executor, task, tmp_path):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))  # keep durable markers inside tmp
    captured = {}

    def fake_popen(cmd, **kw):
        captured["cmd"] = cmd
        captured["kw"] = kw
        return _FakeProc()

    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen
    )
    executor._spawn_hermes_worker(task)
    return captured


def _hermes_executor(tmp_path, lanes_dir=None):
    return _executor(
        worker="hermes",
        working_dir=str(tmp_path),
        **({"lanes_dir": lanes_dir} if lanes_dir else {}),
    )


def test_reviewer_lane_gets_role_env_stamp(monkeypatch, tmp_path):
    """A role='reviewer' task spawns with BDAYA_LANE_ROLE=reviewer in its env."""
    executor = _hermes_executor(tmp_path)
    c = _capture_spawn(
        monkeypatch, executor,
        {"id": "task_r1", "title": "review", "lane_key": "repo!42", "role": "reviewer"},
        tmp_path,
    )
    env = c["kw"]["env"]
    assert env.get("BDAYA_LANE_ROLE") == "reviewer", (
        "#882: the readonly gates key on BDAYA_LANE_ROLE; without the stamp, a "
        "READ-ONLY reviewer brief is prose on the native hermes path."
    )
    assert env.get("BDAYA_LANE_KEY") == "repo!42"
    assert env.get("BDAYA_LANE_ID") == "repo!42"


def test_author_lane_is_stamped_not_inherited(monkeypatch, tmp_path):
    """Author/default lanes carry an explicit author stamp (fail-closed vocabulary)."""
    monkeypatch.setenv("BDAYA_LANE_ROLE", "reviewer")  # stale value in executor env
    executor = _hermes_executor(tmp_path)
    c = _capture_spawn(
        monkeypatch, executor,
        {"id": "task_a1", "title": "build", "lane_key": "repo#branch"},
        tmp_path,
    )
    env = c["kw"]["env"]
    assert env.get("BDAYA_LANE_ROLE") == "author", (
        "an inherited BDAYA_LANE_ROLE=reviewer would deny an author lane its "
        "merge surface; the stamp must always be explicit."
    )


def test_no_lane_key_still_stamps_from_role(monkeypatch, tmp_path):
    """A task with no lane_key (one-shot delivery) still gets its role stamped."""
    monkeypatch.setenv("BDAYA_LANE_ROLE", "reviewer")
    executor = _hermes_executor(tmp_path)
    c = _capture_spawn(
        monkeypatch, executor,
        {"id": "task_n1", "title": "t", "role": "reviewer"},
        tmp_path,
    )
    env = c["kw"]["env"]
    assert env.get("BDAYA_LANE_ROLE") == "reviewer"
    assert env.get("BDAYA_LANE_KEY", "") == "" or "BDAYA_LANE_KEY" not in env


def test_durable_lane_role_marker_written_before_spawn(monkeypatch, tmp_path):
    """lane-role-<safe-id>.json exists at spawn time (crash-resume visibility)."""
    lanes_dir = tmp_path / "lane-roles"
    executor = _executor(worker="hermes", working_dir=str(tmp_path / "wd"),
                         lanes_dir=str(lanes_dir))
    c = _capture_spawn(
        monkeypatch, executor,
        {"id": "task_r2", "title": "review", "lane_key": "shared/repo!7", "role": "reviewer"},
        tmp_path,
    )
    marker = lanes_dir / "lane-role-shared_repo_7.json"
    assert marker.exists(), (
        "#882: the durable role registry is the 2nd gate source when env is lost "
        "(--resume / crash respawn)."
    )
    data = json.loads(marker.read_text(encoding="utf-8"))
    assert data.get("role") == "reviewer"
    assert data.get("lane") == "shared/repo!7"


def test_bdaya_dispatch_path_gets_role_flag(monkeypatch, tmp_path):
    """The bdaya-dispatch spawn path must pass --role so the dispatch stamp fires."""
    captured = {}

    def fake_popen(cmd, **kw):
        captured["cmd"] = cmd
        captured["kw"] = kw
        return _FakeProc()

    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen
    )
    executor = _executor(worker="bdaya-dispatch", working_dir=str(tmp_path))
    executor._spawn_worker(
        {"id": "task_d1", "title": "review mr", "role": "reviewer"}
    )
    cmd = captured["cmd"]
    assert "--role" in cmd, (
        "#882: `bdaya-dispatch run --role reviewer` is the implemented stamp "
        "(claude-plugins !921); the executor was not calling it."
    )
    assert cmd[cmd.index("--role") + 1] == "reviewer"
