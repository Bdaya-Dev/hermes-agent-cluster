"""Tests for lane-status-based completion tracking in AgentExecutor.

The executor no longer relies on subprocess exit codes (bdaya-dispatch run
backgrounds the lane and exits 0 immediately). Instead it polls
``bdaya-dispatch status --json`` and inspects the lane state.

Covers:
  - done/completed lane -> _report_completion called
  - stopped/failed/missing/ambiguous lane -> _report_failure called
  - blocked/working lanes -> NOT reaped (keep waiting)
  - lane absent -> grace window before failing (not instant)
  - query failure -> keep waiting (not fail)
  - spawn_timeout -> fail only for non-terminal lanes (done beats timeout)
  - _query_all_lane_statuses parses JSON regardless of exit code
  - _query_all_lane_statuses returns None on true failure
  - spawn exit nonzero + lane never registered -> diagnostic failure
"""

import json
import time
import pytest
from unittest.mock import patch, MagicMock

from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig, ActiveSpawn


def _make_executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _make_spawn(task_id: str, lane_name: str = "", started_at: float = None, **kw) -> ActiveSpawn:
    mock_proc = MagicMock()
    mock_proc.poll.return_value = None  # process "still running" (backgrounded)
    mock_proc.pid = 12345
    mock_proc.stderr = None
    mock_proc.stdout = None
    return ActiveSpawn(
        task_id=task_id,
        task_title=f"task {task_id}",
        process=mock_proc,
        lease_id=f"lease_{task_id}",
        started_at=started_at or time.time(),
        lane_name=lane_name or f"hermes-{task_id}",
        **kw,
    )


# ---------------------------------------------------------------------------
# Lane status query
# ---------------------------------------------------------------------------

class TestQueryAllLaneStatuses:
    def test_parses_json_correctly(self):
        executor = _make_executor()
        fake_output = json.dumps({
            "lanes": [
                {"lane": "hermes-t1", "state": "working"},
                {"lane": "hermes-t2", "state": "done"},
                {"lane": "hermes-t3", "state": "blocked"},
            ]
        })
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = fake_output
        mock_result.stderr = ""

        with patch("hermes_cluster.core.agent_executor.subprocess.run", return_value=mock_result):
            statuses = executor._query_all_lane_statuses()

        assert statuses == {
            "hermes-t1": "working",
            "hermes-t2": "done",
            "hermes-t3": "blocked",
        }

    def test_parses_json_on_rc1_health_alarm(self):
        """rc=1 is a health alarm, not an error — valid JSON must still be parsed (F1)."""
        executor = _make_executor()
        fake_output = json.dumps({
            "lanes": [
                {"lane": "hermes-t1", "state": "working"},
                {"lane": "hermes-t2", "state": "blocked"},
            ]
        })
        mock_result = MagicMock()
        mock_result.returncode = 1  # health alarm
        mock_result.stdout = fake_output
        mock_result.stderr = ""

        with patch("hermes_cluster.core.agent_executor.subprocess.run", return_value=mock_result):
            statuses = executor._query_all_lane_statuses()

        assert statuses is not None
        assert statuses == {"hermes-t1": "working", "hermes-t2": "blocked"}

    def test_returns_none_on_empty_output(self):
        executor = _make_executor()
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("hermes_cluster.core.agent_executor.subprocess.run", return_value=mock_result):
            assert executor._query_all_lane_statuses() is None

    def test_returns_none_on_invalid_json(self):
        executor = _make_executor()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not json"

        with patch("hermes_cluster.core.agent_executor.subprocess.run", return_value=mock_result):
            assert executor._query_all_lane_statuses() is None

    def test_returns_none_on_timeout(self):
        import subprocess
        executor = _make_executor()
        with patch("hermes_cluster.core.agent_executor.subprocess.run",
                    side_effect=subprocess.TimeoutExpired(cmd="npx", timeout=30)):
            assert executor._query_all_lane_statuses() is None

    def test_returns_none_on_npx_not_found(self):
        executor = _make_executor()
        with patch("hermes_cluster.core.agent_executor.subprocess.run",
                    side_effect=FileNotFoundError("npx")):
            assert executor._query_all_lane_statuses() is None


# ---------------------------------------------------------------------------
# Reap finished spawns — lane-state-based (corrected)
# ---------------------------------------------------------------------------

class TestReapFinishedSpawnsLaneTracking:
    def test_done_lane_triggers_completion(self):
        executor = _make_executor()
        spawn = _make_spawn("task_abc", "hermes-task_abc")
        executor._active_spawns["task_abc"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_abc": "done"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion") as mock_complete:
                    with patch.object(executor, "_report_failure") as mock_fail:
                        executor._reap_finished_spawns()
                        mock_complete.assert_called_once()
                        assert mock_complete.call_args[0][0] == "task_abc"
                        assert "completed" in mock_complete.call_args[1].get("detail", "")
                        mock_fail.assert_not_called()

        assert "task_abc" not in executor._active_spawns

    def test_completed_alias_triggers_completion(self):
        """'completed' is an alias for 'done' (F6)."""
        executor = _make_executor()
        spawn = _make_spawn("task_comp", "hermes-task_comp")
        executor._active_spawns["task_comp"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_comp": "completed"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion") as mock_complete:
                    executor._reap_finished_spawns()
                    mock_complete.assert_called_once()

    def test_blocked_lane_not_reaped(self):
        """blocked is ACTIVE in bdaya-dispatch — must NOT trigger failure (F4)."""
        executor = _make_executor()
        spawn = _make_spawn("task_blk", "hermes-task_blk")
        executor._active_spawns["task_blk"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_blk": "blocked"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    with patch.object(executor, "_report_completion") as mock_complete:
                        executor._reap_finished_spawns()
                        mock_fail.assert_not_called()
                        mock_complete.assert_not_called()

        assert "task_blk" in executor._active_spawns

    def test_stopped_lane_triggers_failure(self):
        executor = _make_executor()
        spawn = _make_spawn("task_stp", "hermes-task_stp")
        executor._active_spawns["task_stp"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_stp": "stopped"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()
                    assert "stopped" in mock_fail.call_args[0][1]

    def test_failed_lane_triggers_failure(self):
        executor = _make_executor()
        spawn = _make_spawn("task_fail", "hermes-task_fail")
        executor._active_spawns["task_fail"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_fail": "failed"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()
                    assert "failed" in mock_fail.call_args[0][1]

    def test_ambiguous_lane_triggers_failure(self):
        """'ambiguous' is a terminal fail state from the tool (F6)."""
        executor = _make_executor()
        spawn = _make_spawn("task_amb", "hermes-task_amb")
        executor._active_spawns["task_amb"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_amb": "ambiguous"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()
                    assert "ambiguous" in mock_fail.call_args[0][1]

    def test_explicit_missing_state_triggers_failure(self):
        """Tool's own 'missing' state row triggers fail (F6)."""
        executor = _make_executor()
        spawn = _make_spawn("task_mis", "hermes-task_mis")
        executor._active_spawns["task_mis"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_mis": "missing"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()

    def test_working_lane_not_reaped(self):
        executor = _make_executor()
        spawn = _make_spawn("task_wip", "hermes-task_wip")
        executor._active_spawns["task_wip"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_wip": "working"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion") as mock_complete:
                    with patch.object(executor, "_report_failure") as mock_fail:
                        executor._reap_finished_spawns()
                        mock_complete.assert_not_called()
                        mock_fail.assert_not_called()

        assert "task_wip" in executor._active_spawns

    def test_missing_lane_has_grace_window(self):
        """Lane absent from status must NOT fail instantly — grace window (F2)."""
        executor = _make_executor()
        spawn = _make_spawn("task_grace", "hermes-task_grace")
        executor._active_spawns["task_grace"] = spawn

        # First few polls: absent but within grace → no failure
        with patch.object(executor, "_query_all_lane_statuses", return_value={}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_not_called()

        assert "task_grace" in executor._active_spawns
        assert spawn.miss_count == 1

    def test_missing_lane_fails_after_grace_exceeded(self):
        """Lane absent beyond grace window → fail (F2)."""
        executor = _make_executor()
        # Started long enough ago to exceed grace seconds
        spawn = _make_spawn("task_grace2", "hermes-task_grace2",
                            started_at=time.time() - 100)  # > 90s grace
        spawn.miss_count = 3  # one more will hit _MISS_GRACE_COUNT (4)
        executor._active_spawns["task_grace2"] = spawn

        with patch.object(executor, "_query_all_lane_statuses", return_value={}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()
                    assert "not found" in mock_fail.call_args[0][1].lower()

    def test_query_failure_keeps_waiting(self):
        """Query failure (None return) → keep waiting, not fail (F2)."""
        executor = _make_executor()
        spawn = _make_spawn("task_qf", "hermes-task_qf")
        executor._active_spawns["task_qf"] = spawn

        with patch.object(executor, "_query_all_lane_statuses", return_value=None):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    with patch.object(executor, "_report_completion") as mock_complete:
                        executor._reap_finished_spawns()
                        mock_fail.assert_not_called()
                        mock_complete.assert_not_called()

        assert "task_qf" in executor._active_spawns

    def test_done_beats_timeout(self):
        """A done lane observed after timeout still completes — state before timeout (F3)."""
        executor = _make_executor(spawn_timeout=10.0)
        spawn = _make_spawn("task_late", "hermes-task_late",
                            started_at=time.time() - 20.0)  # 20s ago, timeout is 10s
        executor._active_spawns["task_late"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_late": "done"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion") as mock_complete:
                    with patch.object(executor, "_report_failure") as mock_fail:
                        executor._reap_finished_spawns()
                        mock_complete.assert_called_once()
                        mock_fail.assert_not_called()

    def test_spawn_timeout_for_non_terminal(self):
        """Timeout fails non-terminal lanes that are still working."""
        executor = _make_executor(spawn_timeout=10.0)
        spawn = _make_spawn("task_to", "hermes-task_to",
                            started_at=time.time() - 20.0)
        executor._active_spawns["task_to"] = spawn

        with patch.object(executor, "_query_all_lane_statuses",
                          return_value={"hermes-task_to": "working"}):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_failure") as mock_fail:
                    executor._reap_finished_spawns()
                    mock_fail.assert_called_once()
                    assert "exceeded" in mock_fail.call_args[0][1].lower()

    def test_no_active_spawns_skips_query(self):
        executor = _make_executor()

        with patch.object(executor, "_query_all_lane_statuses") as mock_query:
            executor._reap_finished_spawns()
            mock_query.assert_not_called()

    def test_multiple_spawns_mixed_states(self):
        executor = _make_executor()
        executor._active_spawns["t1"] = _make_spawn("t1", "hermes-t1")
        executor._active_spawns["t2"] = _make_spawn("t2", "hermes-t2")
        executor._active_spawns["t3"] = _make_spawn("t3", "hermes-t3")
        executor._active_spawns["t4"] = _make_spawn("t4", "hermes-t4")
        executor._active_spawns["t5"] = _make_spawn("t5", "hermes-t5")

        lane_statuses = {
            "hermes-t1": "done",
            "hermes-t2": "working",
            "hermes-t3": "blocked",     # NOT terminal (F4)
            "hermes-t4": "failed",
            "hermes-t5": "stopped",
        }

        with patch.object(executor, "_query_all_lane_statuses", return_value=lane_statuses):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion") as mock_complete:
                    with patch.object(executor, "_report_failure") as mock_fail:
                        executor._reap_finished_spawns()

                        # #874: the call now carries the deliverable too.
                        mock_complete.assert_called_once_with(
                            "t1",
                            detail=mock_complete.call_args[1]["detail"],
                            result=mock_complete.call_args[1]["result"],
                        )
                        assert "result" in mock_complete.call_args[1]

                        fail_task_ids = {call[0][0] for call in mock_fail.call_args_list}
                        assert fail_task_ids == {"t4", "t5"}

        # t2 working + t3 blocked → still active
        assert "t2" in executor._active_spawns
        assert "t3" in executor._active_spawns
        assert "t1" not in executor._active_spawns
        assert "t4" not in executor._active_spawns

    def test_spawn_exit_nonzero_lane_never_registered(self):
        """Spawn process exited nonzero + lane never appeared → diagnostic fail (F7)."""
        executor = _make_executor()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 2  # nonzero exit
        mock_proc.pid = 12345
        mock_proc.stderr = None
        mock_proc.stdout = MagicMock()
        mock_proc.stdout.read.return_value = b"error: not a cpm multi-account machine"

        spawn = ActiveSpawn(
            task_id="task_spawn_fail",
            task_title="spawn fail task",
            process=mock_proc,
            lease_id="lease_sf",
            started_at=time.time(),
            lane_name="hermes-task_spawn_fail",
        )
        executor._active_spawns["task_spawn_fail"] = spawn

        with patch.object(executor, "_query_all_lane_statuses", return_value={}):
            with patch.object(executor, "_report_failure") as mock_fail:
                executor._reap_finished_spawns()
                mock_fail.assert_called_once()
                reason = mock_fail.call_args[0][1]
                assert "rc=2" in reason
                assert "never registered" in reason

    def test_completion_detail_is_logged(self):
        """_report_completion logs the detail (F5)."""
        executor = _make_executor()

        with patch("hermes_cluster.core.agent_executor._signed_request", return_value={"status": "completed"}):
            with patch("hermes_cluster.core.agent_executor.logger") as mock_logger:
                executor._report_completion("task_x", detail="lane completed in 120s")
                mock_logger.info.assert_called_once()
                assert "120s" in mock_logger.info.call_args[0][2]
