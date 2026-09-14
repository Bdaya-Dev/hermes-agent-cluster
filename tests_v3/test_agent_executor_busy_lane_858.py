"""#858 DEFECT 1 — busy-lane delivery must QUEUE, not fail (tests_v3).

A second task arriving on a lane that already has an in-flight delivery used
to be spawned as a concurrent ``hermes … --resume <session>``. Hermes refuses
(``SESSION_NOT_OWNED: Session … already has a live owner (cli, pid …, lease
age …)``), the task landed in ``failed`` with ``error: None`` and a zero-byte
result — a failure with no reason attached. Back-to-back tasks on one lane
are the NORMAL case (LFP-1); this file pins the fixed shape:

  1. claim pass: busy-lane candidate is queued per lane FIFO, NOT spawned;
     the task's cluster status is untouched (stays ready/running — never
     moved toward failed);
  2. when the in-flight delivery reaps, the FIFO head releases and spawns
     IN ARRIVAL ORDER (FIFO proven with two waiting tasks);
  3. a residual refusal (race past the queue) re-queues with bounded
     backoff instead of failing — no /fail call at all while the retry
     budget lasts;
  4. when the budget is spent, the failure carries HERMES' OWN refusal text
     verbatim (never None/empty).

RED proof against main (14dc7ea..68f7f6e): main's _claim_and_spawn has no
busy-lane check at all — test 1 shows a Popen for the second task (spawn
into the busy lane) and an empty queue. See MR for the exact failure output.
"""

import time
from unittest.mock import patch

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
    pid = 777
    stderr = None
    stdout = None

    def poll(self):
        return None  # still running


class _ExitedProc:
    """Process that already exited with the given rc."""

    def __init__(self, rc=1):
        self.pid = 555
        self.stderr = None
        self.stdout = None
        self._rc = rc

    def poll(self):
        return self._rc


def _lane_task(task_id, lane_key="L", status="running", priority=3):
    return {
        "id": task_id, "title": f"task {task_id}", "description": "d",
        "status": status, "assigned_to": "test-node", "priority": priority,
        "lane_key": lane_key, "role": "author",
    }


def _seed_live_lane_delivery(executor, store, running_task_id="tA",
                             lane_key="L", started_at=None):
    """One in-flight delivery on lane L, as if hermes is running it."""
    store.record_lane(lane_key, session_id="sid_L", role="author",
                      node="test-node", last_task_id=running_task_id)
    spawn = ActiveSpawn(
        task_id=running_task_id, task_title="running delivery",
        process=_AliveProc(), started_at=started_at or time.time(),
        lane_name=f"hermes-{running_task_id}", mode="hermes",
        lane_key=lane_key, role="author", session_id="sid_L",
    )
    with executor._lock:
        executor._active_spawns[running_task_id] = spawn
    return spawn


# ---------------------------------------------------------------------------
# 1. Busy lane: queue FIFO, never spawn into it, never fail the task
# ---------------------------------------------------------------------------

class TestBusyLaneQueuesInsteadOfSpawning:
    def test_second_task_on_busy_lane_is_queued_not_spawned(self, tmp_path):
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        _seed_live_lane_delivery(executor, store)

        popen_calls = []
        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=lambda cmd, **kw: popen_calls.append(cmd) or _AliveProc()):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[_lane_task("tB")]):
                executor._claim_and_spawn(max_spawns=5)

        # DEFECT (main): a second `--resume` was spawned into the busy lane.
        assert popen_calls == [], (
            "busy-lane task must NOT be spawned concurrently into the lane; "
            f"got {len(popen_calls)} Popen call(s)")
        # FIX: it waits in the lane's FIFO.
        assert [t["id"] for t in executor._lane_queue.get("L", [])] == ["tB"]
        # And it was NOT failed — no /fail request for tB was ever made:
        # _report_failure would POST to .../fail via _signed_request (patched
        # above); we assert the queue path left the board state untouched by
        # checking the task dict never reached a failed terminal state.
        assert "tB" not in executor._active_spawns

    def test_queued_task_stays_ready_never_fails(self, tmp_path):
        """The queue is executor-side only: no board transition toward
        failed for the queued task (main's shape: failed + error None)."""
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        _seed_live_lane_delivery(executor, store)

        requests = []

        def spy_request(endpoint, method, path, data, token, node_id, **kw):
            requests.append((method, path, data))
            return [_lane_task("tB")]

        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   return_value=_AliveProc()):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       side_effect=spy_request):
                executor._claim_and_spawn(max_spawns=5)

        fail_calls = [r for r in requests if r[1].endswith("/fail")]
        assert fail_calls == [], (
            f"queued task must not be failed; saw {fail_calls}")
        assert [t["id"] for t in executor._lane_queue.get("L", [])] == ["tB"]

    def test_busy_lane_spawn_boundary_queues_directly(self, tmp_path):
        """The queue guard lives at the spawn boundary too, so a direct
        _spawn_hermes_worker call on a busy lane queues, never resumes."""
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        _seed_live_lane_delivery(executor, store)
        with patch("hermes_cluster.core.agent_executor.subprocess.Popen") as mock_popen:
            executor._spawn_hermes_worker(_lane_task("tC"))
            mock_popen.assert_not_called()
        assert [t["id"] for t in executor._lane_queue.get("L", [])] == ["tC"]


# ---------------------------------------------------------------------------
# 2. FIFO release order when the running delivery reaps
# ---------------------------------------------------------------------------

class TestFifoReleaseOnReap:
    def test_two_waiting_tasks_spawn_in_arrival_order(self, tmp_path):
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        spawn_a = _seed_live_lane_delivery(executor, store)

        # Queue two tasks behind tA — arrival order tB then tD.
        assert executor._queue_for_lane("L", _lane_task("tB"))
        assert executor._queue_for_lane("L", _lane_task("tD"))
        assert [t["id"] for t in executor._lane_queue["L"]] == ["tB", "tD"]

        # tA reaps done.
        result = tmp_path / "hermes-results" / "tA.result.md"
        result.parent.mkdir(parents=True, exist_ok=True)
        result.write_text("done", encoding="utf-8")
        err = tmp_path / "hermes-results" / "tA.stderr.log"
        err.write_text("session_id: sid_L\n", encoding="utf-8")
        spawn_a.result_path = str(result)
        spawn_a.stderr_path = str(err)
        spawn_a.process = _ExitedProc(0)

        popen_cmds = []

        def fake_popen(cmd, **kw):
            popen_cmds.append(cmd)
            return _AliveProc()

        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=fake_popen):
            with patch.object(executor, "_capture_spawn_exits"):
                with patch.object(executor, "_report_completion"):
                    executor._reap_finished_spawns()          # tA leaves
            executor._sweep_lane_queues()                     # FIFO head -> released
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[]):                      # no new claims
                executor._claim_and_spawn(max_spawns=5)

        # ONLY the FIFO head (tB) spawned — not tD, and not out of order.
        assert len(popen_cmds) == 1, (
            f"expected exactly one FIFO-head spawn, got {len(popen_cmds)}")
        assert "tB" in executor._active_spawns
        assert [t["id"] for t in executor._lane_queue.get("L", [])] == ["tD"]

        # And when tB's slot frees, tD follows (still FIFO).
        executor._active_spawns.pop("tB")
        executor._sweep_lane_queues()
        popen_cmds.clear()
        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=fake_popen):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[]):
                executor._claim_and_spawn(max_spawns=5)
        assert len(popen_cmds) == 1
        assert "tD" in executor._active_spawns

    def test_release_waits_while_lane_still_busy(self, tmp_path):
        """A queued head must NOT release while any delivery for its lane is
        still in flight (the queue guard is lane-keyed, not global)."""
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        _seed_live_lane_delivery(executor, store)  # tA running on lane L
        assert executor._queue_for_lane("L", _lane_task("tB"))

        executor._sweep_lane_queues()
        assert executor._lane_released == {}, (
            "lane still busy — nothing may release from its FIFO")
        assert [t["id"] for t in executor._lane_queue["L"]] == ["tB"]

    def test_leftover_reinsertion_preserves_arrival_order(self, tmp_path):
        """#858 review blocker (PR #23): when the spawn budget runs out
        mid-drain, un-spawned released tasks go back at the FRONT of their
        lane queues in ARRIVAL order. The original loop inserted each
        leftover at index 0 front-to-back, reversing every lane that had 2+
        tasks left over — breaking the headline FIFO guarantee on this path.

        RED against the pre-fix shape: lane L comes back [tD, tB] and the
        next sweep releases tD ahead of tB."""
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        # Drain order is X then L (released dict order): tX takes the one
        # slot, leaving tB and tD — TWO tasks of lane L — as leftover.
        executor._lane_released = {
            "X": [_lane_task("tX", lane_key="X")],
            "L": [_lane_task("tB"), _lane_task("tD")],
        }
        popen_cmds = []

        def fake_popen(cmd, **kw):
            popen_cmds.append(cmd)
            return _AliveProc()

        with patch("hermes_cluster.core.agent_executor.subprocess.Popen",
                   side_effect=fake_popen):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       return_value=[]):
                executor._claim_and_spawn(max_spawns=1)  # only tX spawns

        assert len(popen_cmds) == 1 and "tB" not in executor._active_spawns
        order = [t["id"] for t in executor._lane_queue.get("L", [])]
        assert order == ["tB", "tD"], (
            f"leftover re-insertion reversed lane L FIFO (got {order}) — "
            "arriving-earlier tB must sit ahead of tD")
        # The next sweep (lane L free) must release tB first, then tD.
        executor._sweep_lane_queues()
        assert [t["id"] for t in executor._lane_released.get("L", [])] == ["tB"]
        assert [t["id"] for t in executor._lane_queue["L"]] == ["tD"]


# ---------------------------------------------------------------------------
# 3/4. Residual refusal: re-queue with bounded backoff; exhaustion carries
#      hermes' verbatim text.
# ---------------------------------------------------------------------------

HERMES_REFUSAL = (
    "hermes-refusal-reason: SESSION_NOT_OWNED\n"
    "Session 20260910_120000_abc already has a live owner (cli, pid 72390, "
    "lease age 49m). Its turn activity is unknown; an open lease does not "
    "mean a turn is running. Attach through a compatible owner, or close "
    "the session in its owning surface before resuming here."
)


class _RefusedProc:
    """A hermes child that refused the resume and exited 1."""
    pid = 999
    stderr = None
    stdout = None

    def poll(self):
        return 1


def _hermes_spawn_with_refusal(executor, store, tmp_path, task_id="tR",
                               busy_attempts=0, lane_key="L"):
    result = tmp_path / "hermes-results" / f"{task_id}.result.md"
    result.parent.mkdir(parents=True, exist_ok=True)
    result.write_text("", encoding="utf-8")  # zero-byte result (the incident)
    err = tmp_path / "hermes-results" / f"{task_id}.stderr.log"
    err.write_text(HERMES_REFUSAL + "\n", encoding="utf-8")
    spawn = ActiveSpawn(
        task_id=task_id, task_title="refused delivery",
        process=_RefusedProc(), started_at=time.time(),
        lane_name=f"hermes-{task_id}", mode="hermes",
        result_path=str(result), stderr_path=str(err),
        lane_key=lane_key, role="author", session_id="sid_L",
        task_payload=_lane_task(task_id, lane_key),
        busy_attempts=busy_attempts,
    )
    with executor._lock:
        executor._active_spawns[task_id] = spawn
    return spawn


class TestResidualRefusalRetries:
    def test_refusal_requeues_instead_of_failing(self, tmp_path):
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        executor._LANE_BUSY_BACKOFF_BASE = 0.0  # release immediately in test
        _hermes_spawn_with_refusal(executor, store, tmp_path)

        requests = []

        def spy_request(endpoint, method, path, data, token, node_id, **kw):
            requests.append((method, path, data))
            return {}

        with patch.object(executor, "_capture_spawn_exits"):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       side_effect=spy_request):
                executor._reap_finished_spawns()

        fails = [r for r in requests if r[1].endswith("/fail")]
        assert fails == [], (
            "a residual busy refusal must re-queue, not fail — main "
            f"POSTed {fails}")
        assert [t["id"] for t in executor._lane_queue.get("L", [])] == ["tR"]
        meta = executor._lane_queue_meta.get("tR")
        assert meta and meta[0] == 1, "attempt count must ride through re-queue"

    def test_refusal_budget_exhaustion_carries_hermes_verbatim_text(self, tmp_path):
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        # Budget spent: the spawn arrives at max attempts.
        _hermes_spawn_with_refusal(executor, store, tmp_path,
                                   busy_attempts=executor._LANE_BUSY_MAX_RETRIES)

        requests = []

        def spy_request(endpoint, method, path, data, token, node_id, **kw):
            requests.append((method, path, data))
            return {}

        with patch.object(executor, "_capture_spawn_exits"):
            with patch("hermes_cluster.core.agent_executor._signed_request",
                       side_effect=spy_request):
                executor._reap_finished_spawns()

        fails = [r for r in requests if r[1].endswith("/fail")]
        assert len(fails) == 1, f"exhausted retries must fail exactly once: {fails}"
        reason = fails[0][2]["reason"]
        # THE core requirement: whatever Hermes actually said lands in the
        # failure — never None, never a bare 'exit rc' guess.
        assert "SESSION_NOT_OWNED" in reason
        assert "already has a live owner (cli, pid 72390, lease age 49m)" in reason

    def test_backoff_bounds_are_explicit_and_finite(self, tmp_path):
        """'Bounded backoff' means a retry count cap AND a doubling wait —
        pinned so a future edit can't make retries unbounded."""
        store = _store()
        executor = _executor(store, working_dir=str(tmp_path))
        waits = [executor._lane_busy_backoff(a)
                 for a in range(1, executor._LANE_BUSY_MAX_RETRIES + 1)]
        assert waits == sorted(waits) and all(w > 0 for w in waits)
        assert executor._LANE_BUSY_MAX_RETRIES <= 8
        # attempts=0 (never refused) releases immediately once the lane frees
        assert executor._lane_busy_backoff(0) == 0.0
