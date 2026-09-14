"""#899 item 3 — `_reconcile_persisted_spawns` re-attaches to LIVE child
processes by the persisted pid, so a restarted executor never spawns a
second session for a task whose first session is still running, and reaps
it normally when it exits.

The windows_desktop incident (#899) was this failure mode at fleet scale:
the relaunched executor treated the still-running lane as gone and spawned
a duplicate. The persisted-record reattach existed before (#804) but only
its fake-pid path was ever tested; here the child is a REAL subprocess:

  * live child pid  -> resumed spawn polls it with the OS-level liveness
    probe and _claim_and_spawn spawns ZERO second session for the task;
  * child exits    -> the resumed spawn's poll() turns non-None, so the
    normal reap path resolves the task (reaps it, drops the record);
  * a record written by a SIBLING executor after our start (bypassing the
    lock, e.g. an older build) is picked up by the periodic
    _refresh_persisted_ids pass on the next poll cycle and re-attached
    instead of being re-spawned (red on main: the suppression set was
    seeded once at reconcile, so a late record never suppressed a spawn).
"""

import subprocess
import sys
import time
from unittest.mock import patch

import pytest

from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
    _ResumedProcess,
)
from hermes_cluster.state.cluster_store import ClusterStore


def _store():
    return ClusterStore(db_path=":memory:")


def _executor(store, **cfg_overrides):
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    ex = AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="my-node",
        cluster_endpoint="http://127.0.0.1:9999",
        store=store,
    )
    return ex


def _live_child():
    """A real OS process we can point a persisted pid at."""
    return subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(30)"])


def _running_task(task_id="t_live"):
    return {"id": task_id, "title": "live task", "status": "running",
            "assigned_to": "my-node", "priority": 3}


class TestRestartReattachesLiveChild:
    def test_live_child_pid_reattached_and_zero_spawns(self):
        """Brief scenario: executor restarted while its child session is
        still running -> reattach by persisted pid, zero new sessions."""
        store = _store()
        child = _live_child()
        try:
            store.record_task_spawn(
                task_id="t_live", mode="bdaya-dispatch",
                job_id="hermes-t_live", pid=child.pid,
                started_at=time.time(), lane_name="hermes-t_live")
            executor = _executor(store)
            executor._reconcile_persisted_spawns()

            spawn = executor._active_spawns["t_live"]
            assert spawn.resumed is True
            assert spawn.process.pid == child.pid
            # The OS-level probe says the persisted pid is ALIVE.
            assert spawn.process.poll() is None

            # Main still lists the task assigned to us: zero spawns.
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[_running_task()]):
                with patch.object(executor, "_spawn_worker") as mock_spawn:
                    executor._claim_and_spawn(max_spawns=5)
                    mock_spawn.assert_not_called()
        finally:
            child.terminate()
            child.wait(timeout=10)

    def test_exited_child_reaped_normally(self):
        """After the (already dead) child's pid is gone, the resumed spawn's
        poll turns non-None — the reap logic resolves it instead of it
        being tracked forever, and it is NOT re-spawned as a live process."""
        store = _store()
        child = _live_child()
        pid = child.pid
        child.terminate()
        child.wait(timeout=10)
        store.record_task_spawn(
            task_id="t_dead", mode="bdaya-dispatch",
            job_id="hermes-t_dead", pid=pid,
            started_at=time.time(), lane_name="hermes-t_dead")
        executor = _executor(store)
        executor._reconcile_persisted_spawns()
        spawn = executor._active_spawns["t_dead"]
        # pid is dead now -> the probe reports an exit code.
        assert spawn.process.poll() is not None
        # and _claim still suppresses re-spawn via the persisted record
        with patch("hermes_cluster.core.agent_executor._signed_request",
                   return_value=[{"id": "t_dead", "title": "x",
                                  "status": "running",
                                  "assigned_to": "my-node", "priority": 3}]):
            with patch.object(executor, "_spawn_worker") as mock_spawn:
                executor._claim_and_spawn(max_spawns=5)
                mock_spawn.assert_not_called()

    def test_resumed_process_probe_semantics(self):
        """_ResumedProcess: live pid -> None; dead pid -> rc (the exact
        're-attach live children / reap normally' primitive)."""
        child = _live_child()
        try:
            p = _ResumedProcess(child.pid)
            assert p.poll() is None
        finally:
            child.terminate()
            child.wait(timeout=10)
        # give the OS a beat to release the pid
        deadline = time.time() + 5
        while time.time() < deadline and child.poll() is None:
            time.sleep(0.05)
        p = _ResumedProcess(child.pid)
        # After wait(), the pid is recycled-eligible; poll() must eventually
        # stop reporting alive. On macOS a waited pid is definitely gone.
        assert p.poll() is not None


class TestSiblingRecordRefresh:
    """The late-record case: a second executor (older build that skipped
    the lock) persists a spawn record AFTER ours reconciled. Main's
    behaviour: the task is re-spawned beside the sibling's live session
    (the duplicate spawn #899 forbids). Ours: re-attached, zero spawns."""

    def test_late_sibling_record_suppresses_spawn_and_reattaches(self):
        store = _store()
        executor = _executor(store)
        executor._reconcile_persisted_spawns()   # empty at start

        child = _live_child()
        try:
            # Sibling writes a record mid-run (after our reconcile).
            store.record_task_spawn(
                task_id="t_sib", mode="bdaya-dispatch",
                job_id="hermes-t_sib", pid=child.pid,
                started_at=time.time(), lane_name="hermes-t_sib")

            # Force the refresh pass to fire on this cycle.
            executor._last_persisted_refresh = 0.0
            executor._persisted_refresh_s = 0.0

            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[{"id": "t_sib", "title": "sib task",
                                      "status": "running",
                                      "assigned_to": "my-node", "priority": 3}]):
                with patch.object(executor, "_spawn_worker") as mock_spawn:
                    executor._claim_and_spawn(max_spawns=5)
                    # DEFECT on main: mock_spawn called once (the record
                    # appeared after reconcile, invisible to the stale
                    # suppression set) -> duplicate session beside the
                    # sibling's live child.
                    mock_spawn.assert_not_called()

            # Re-attached against the live pid, exactly like a restart record.
            spawn = executor._active_spawns["t_sib"]
            assert spawn.resumed is True
            assert spawn.process.pid == child.pid
            assert spawn.process.poll() is None
        finally:
            child.terminate()
            child.wait(timeout=10)

    def test_refresh_disabled_restores_pre899_seeding(self, monkeypatch):
        """Ops knob: HERMES_CLUSTER_PERSISTED_REFRESH_S<0 disables refresh
        (the documented pre-#899 one-shot seeding)."""
        monkeypatch.setenv("HERMES_CLUSTER_PERSISTED_REFRESH_S", "-1")
        from hermes_cluster.core.agent_executor import (
            _persisted_ids_refresh_interval)
        assert _persisted_ids_refresh_interval() < 0
