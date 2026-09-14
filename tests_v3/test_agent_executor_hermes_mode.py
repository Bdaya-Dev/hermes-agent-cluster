"""Tests for the NATIVE hermes worker mode in AgentExecutor.

Config ``agent_executor.worker: "hermes"`` spawns a non-interactive hermes
session (``hermes -p <profile> chat --query-file <brief> -Q``) and tracks the
process by pid + exit code + result file — instead of bdaya-dispatch lane
status. C14 (shared/claude-plugins#847-adjacent).

Covers:
  - config default worker stays bdaya-dispatch
  - hermes mode spawn builds the documented invocation
  - hermes mode reaps by result file + exit code (done / fail / no-result / timeout)
  - spawned hermes lane writes result file to hermes-results/<task>.result.md
"""

import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
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


def _hermes_spawn(task_id: str, started_at: float = None, **kw) -> ActiveSpawn:
    mock_proc = MagicMock()
    mock_proc.poll.return_value = None  # still running
    mock_proc.pid = 4242
    mock_proc.stderr = None
    mock_proc.stdout = None
    return ActiveSpawn(
        task_id=task_id,
        task_title=f"task {task_id}",
        process=mock_proc,
        lease_id=f"lease_{task_id}",
        started_at=started_at or time.time(),
        lane_name=f"hermes-{task_id}",
        mode="hermes",
        result_path=str(kw.pop("result_path", "") or f"/tmp/{task_id}.result.md"),
        **kw,
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestHermesConfig:
    def test_default_worker_is_bdaya_dispatch(self):
        assert AgentExecutorConfig().worker == "bdaya-dispatch"

    def test_hermes_mode_values(self):
        cfg = AgentExecutorConfig(worker="hermes", hermes_profile="bdaya", hermes_bin="/custom/hermes")
        assert cfg.worker == "hermes"
        assert cfg.hermes_profile == "bdaya"
        assert cfg.hermes_bin == "/custom/hermes"


# ---------------------------------------------------------------------------
# Spawn
# ---------------------------------------------------------------------------

class TestHermesSpawn:
    def test_spawn_hermes_uses_query_file_quiet_and_profile(self, monkeypatch, tmp_path):
        """The native invocation is the documented one:
        ``hermes -p <profile> chat --query-file <brief> -Q``."""
        captured = {}

        class _FakeProc:
            pid = 777
            stderr = None
            stdout = None
            def poll(self):
                return None

        def fake_popen(cmd, **kw):
            captured["cmd"] = cmd
            return _FakeProc()

        monkeypatch.setattr("hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen)

        executor = _executor(
            worker="hermes",
            hermes_profile="default",
            hermes_bin="/usr/local/bin/hermes",
            working_dir=str(tmp_path),
        )
        executor._spawn_hermes_worker({"id": "task_h1", "title": "do hermes thing", "description": "desc"})

        cmd = captured["cmd"]
        assert cmd[0] == "/usr/local/bin/hermes"
        assert cmd[cmd.index("-p") + 1] == "default"
        assert "chat" in cmd
        assert "-Q" in cmd
        assert "--query-file" in cmd
        brief = Path(cmd[cmd.index("--query-file") + 1])
        assert brief.exists()
        assert "do hermes thing" in brief.read_text(encoding="utf-8")

    def test_spawn_hermes_persists_result_path_and_maps_spawn(self, monkeypatch, tmp_path):
        captured = {}

        class _FakeProc:
            pid = 778
            stderr = None
            stdout = None
            def poll(self):
                return None

        def fake_popen(cmd, **kw):
            captured["cmd"] = cmd
            return _FakeProc()

        monkeypatch.setattr("hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen)

        executor = _executor(
            worker="hermes",
            working_dir=str(tmp_path),
        )
        executor._spawn_hermes_worker({"id": "task_h2", "title": "t"})

        spawn = executor._active_spawns["task_h2"]
        assert spawn.mode == "hermes"
        assert spawn.process.pid == 778
        result = Path(spawn.result_path)
        assert result.parent == tmp_path / "hermes-results"

    def test_spawn_hermes_missing_bin_reports_failure(self, monkeypatch, tmp_path):
        def missing_popen(cmd, **kw):
            raise FileNotFoundError("hermes")

        monkeypatch.setattr("hermes_cluster.core.agent_executor.subprocess.Popen", missing_popen)
        executor = _executor(worker="hermes", working_dir=str(tmp_path))
        with patch.object(executor, "_report_failure") as mock_fail:
            executor._spawn_hermes_worker({"id": "task_h3", "title": "t"})
            mock_fail.assert_called_once()
            assert "hermes" in mock_fail.call_args[0][1].lower()


# ---------------------------------------------------------------------------
# Reaping
# ---------------------------------------------------------------------------

class TestHermesReap:
    def test_done_when_exit_zero_and_result_written(self, tmp_path):
        executor = _executor(worker="hermes")
        result_path = tmp_path / "r.md"
        result_path.write_text("here is the answer", encoding="utf-8")
        spawn = _hermes_spawn("t_done", result_path=str(result_path))
        spawn.process.poll.return_value = 0
        executor._active_spawns["t_done"] = spawn

        with patch.object(executor, "_capture_spawn_exits"):
            resolved = []
            executor._reap_hermes_spawn("t_done", spawn, 12.0, resolved)

        assert len(resolved) == 1
        task_id, _spawn, outcome, detail = resolved[0]
        assert outcome == "done"
        assert str(result_path) in detail

    def test_fail_when_exit_nonzero(self, tmp_path):
        executor = _executor(worker="hermes")
        result_path = tmp_path / "empty.md"
        result_path.write_text("", encoding="utf-8")
        spawn = _hermes_spawn("t_fail", result_path=str(result_path))
        spawn.process.poll.return_value = 3
        executor._active_spawns["t_fail"] = spawn

        resolved = []
        executor._reap_hermes_spawn("t_fail", spawn, 5.0, resolved)

        assert resolved and resolved[0][2] == "spawn_failed"
        assert "rc=3" in resolved[0][3]

    def test_no_result_when_exit_zero_but_empty_file(self, tmp_path):
        executor = _executor(worker="hermes")
        result_path = tmp_path / "blank.md"
        result_path.write_text("   \n", encoding="utf-8")
        spawn = _hermes_spawn("t_blank", result_path=str(result_path))
        spawn.process.poll.return_value = 0
        executor._active_spawns["t_blank"] = spawn

        resolved = []
        executor._reap_hermes_spawn("t_blank", spawn, 3.0, resolved)

        assert resolved and resolved[0][2] == "no_result"

    def test_not_done_while_running_even_if_stdout_has_content(self, tmp_path):
        """#851/3: the result file is the child's stdout — a startup warning
        must NOT resolve a still-running lane as done."""
        executor = _executor(spawn_timeout=3600)
        result_path = tmp_path / "noisy.md"
        result_path.write_text(
            "Warning: Unknown toolsets: bdaya-gitlab, bdaya-lsp, bdaya_gcp\n",
            encoding="utf-8",
        )
        spawn = _hermes_spawn("t_noisy", result_path=str(result_path))
        spawn.process.poll.return_value = None  # still running

        resolved = []
        executor._reap_hermes_spawn("t_noisy", spawn, 30.0, resolved)

        assert resolved == []

    def test_done_only_after_exit_with_content(self, tmp_path):
        executor = _executor(spawn_timeout=3600)
        result_path = tmp_path / "noisy_done.md"
        result_path.write_text("Warning: x\nfinal answer\n", encoding="utf-8")
        spawn = _hermes_spawn("t_noisy_done", result_path=str(result_path))
        spawn.process.poll.return_value = 0

        resolved = []
        executor._reap_hermes_spawn("t_noisy_done", spawn, 30.0, resolved)

        assert resolved and resolved[0][2] == "done"

    def test_resumed_process_reports_exit_for_dead_pid(self):
        """#851/3 follow-up: on Windows os.kill(<dead pid>, 0) raises OSError
        errno 22 / WinError 87, not ProcessLookupError. If that is swallowed,
        poll() returns None forever and a reconciled lane can never complete
        once "done" requires a clean exit."""
        from hermes_cluster.core.agent_executor import _ResumedProcess

        proc = _ResumedProcess(pid=999999)
        err = OSError(22, 'The parameter is incorrect')
        err.winerror = 87

        with patch('hermes_cluster.core.agent_executor.os.kill', side_effect=err), \
             patch('hermes_cluster.core.agent_executor.os.name', 'nt'):
            assert proc.poll() == 0
        # sticky: no further probing once the process is known gone
        assert proc.poll() == 0

    def test_resumed_process_still_alive_returns_none(self):
        from hermes_cluster.core.agent_executor import _ResumedProcess

        proc = _ResumedProcess(pid=4242)
        with patch('hermes_cluster.core.agent_executor.os.kill', return_value=None):
            assert proc.poll() is None

    def test_reconciled_spawn_completes_after_pid_gone(self, tmp_path):
        """End to end: a reconciled spawn whose pid is gone resolves done
        once its result file has content (the restart-reconcile path).

        The Windows dead-pid probe (os.name == 'nt') is patched ONLY around
        the poll() call itself: patching it across _reap_hermes_spawn would
        also flip pathlib.Path to WindowsPath, which cannot be constructed
        on Linux CI runners. The _exited latch set during the patched probe
        carries into the unpatched reap.
        """
        from hermes_cluster.core.agent_executor import _ResumedProcess

        executor = _executor(spawn_timeout=3600)
        result_path = tmp_path / 'reconciled.md'
        result_path.write_text('the lane answer', encoding='utf-8')
        spawn = _hermes_spawn('t_reconciled', result_path=str(result_path))
        proc = _ResumedProcess(pid=999999)
        spawn.process = proc
        err = OSError(22, 'The parameter is incorrect')
        err.winerror = 87

        # simulate the Windows probe: pid-gone => poll() latches exited
        with patch('hermes_cluster.core.agent_executor.os.kill', side_effect=err), \
             patch('hermes_cluster.core.agent_executor.os.name', 'nt'):
            assert proc.poll() == 0
        assert proc._exited is True

        resolved = []
        executor._reap_hermes_spawn('t_reconciled', spawn, 42.0, resolved)

        assert resolved and resolved[0][2] == 'done'

    def test_timeout_when_still_running(self, tmp_path):
        executor = _executor(worker="hermes", spawn_timeout=10.0)
        result_path = tmp_path / "run.md"
        result_path.write_text("", encoding="utf-8")
        spawn = _hermes_spawn(
            "t_run",
            started_at=time.time() - 20.0,  # exceeds 10s timeout
            result_path=str(result_path),
        )
        executor._active_spawns["t_run"] = spawn

        resolved = []
        executor._reap_hermes_spawn("t_run", spawn, 20.0, resolved)

        assert resolved and resolved[0][2] == "timeout"

    def test_keeps_waiting_while_alive_within_timeout(self, tmp_path):
        executor = _executor(worker="hermes", spawn_timeout=60.0)
        result_path = tmp_path / "w.md"
        result_path.write_text("", encoding="utf-8")
        spawn = _hermes_spawn("t_wait", result_path=str(result_path))
        spawn.process.poll.return_value = None
        executor._active_spawns["t_wait"] = spawn

        resolved = []
        executor._reap_hermes_spawn("t_wait", spawn, 5.0, resolved)

        assert resolved == []

    def test_reap_finished_skips_lane_status_for_hermes(self, tmp_path):
        """Hermes-mode spawns must NOT be reaped through bdaya-dispatch status."""
        executor = _executor(worker="hermes")
        result_path = tmp_path / "done2.md"
        result_path.write_text("answer", encoding="utf-8")
        spawn = _hermes_spawn("t_h5", result_path=str(result_path))
        spawn.process.poll.return_value = 0
        executor._active_spawns["t_h5"] = spawn

        with patch.object(executor, "_query_all_lane_statuses") as mock_query:
            with patch.object(executor, "_report_completion") as mock_done:
                executor._reap_finished_spawns()
                mock_query.assert_not_called()
                # #874: _report_completion now also carries the deliverable.
                # Same strength as before -- exactly one call for this task, with
                # its detail -- plus the new result kwarg the executor must pass.
                mock_done.assert_called_once_with(
                    "t_h5",
                    detail=mock_done.call_args[1]["detail"],
                    result=mock_done.call_args[1]["result"],
                )
                assert "result" in mock_done.call_args[1], (
                    "the executor must pass a result, even when it is None"
                )

        assert "t_h5" not in executor._active_spawns