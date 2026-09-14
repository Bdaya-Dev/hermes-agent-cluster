"""Agent executor — bridge from cluster task lease to a real worker run.

Polls the main node for tasks assigned (status=running, assigned_to=this node),
spawns a guarded headless worker for each task, and reports completion/failure
back to the cluster via signed API calls.

Two spawn modes (config ``agent_executor.worker``; default ``bdaya-dispatch``):
  - ``bdaya-dispatch`` — the proven npx bdaya-dispatch lane; completion is
    detected by polling ``bdaya-dispatch status --json`` (the run backgrounds
    instantly, so process exit codes are NOT the completion signal).
  - ``hermes`` — a NATIVE non-interactive hermes session
    (``hermes -p <profile> chat --query-file <brief> -Q``) that runs the task
    to completion, writes a result file, and exits non-zero on failure; this
    mode tracks the process (pid + exit code + result file) instead of lane
    status. The child's inherited STDOUT goes to ``<task>.stdout.log`` (a
    transcript) and ``<task>.result.md`` is left FREE for the lane to write
    its deliverable into — the executor never opens it (shared/claude-plugins
    #868: holding the deliverable path open made Windows refuse the lane's
    tmp-then-rename write and silently stranded whole verdicts). Lanes,
    however, deliver by PRINTING their final message; when one exits 0 with
    no result.md and a non-empty transcript, the executor promotes the
    transcript into result.md under a loud marker and reaps
    ``transcript_promoted`` — completion without a verdict-grade ``done``
    (shared/claude-plugins#871).

Restart safety (#804 note 132791): the task→lane map is persisted in the
ClusterStore SQLite (``task_spawns`` table) and reconciled on start — a lane
spawned before a crash is resumed from its record, and a task is re-spawned
only when no record exists (no duplicate worker after an executor restart).

Stateful lanes (shared/claude-plugins#847/#833, owner ruling 2026-09-10): a
task carrying ``lane_key`` joins a named stateful lane. The first task with a
new ``lane_key`` spawns a hermes session titled by the lane_key (``-c
<lane_key> --create-if-missing``); every later task with that lane_key RESUMES
the lane's live session (``--resume <session_id>``) and delivers its brief as
the next message — one hermes session per lane, cheap models (qwen3.8-flash
profile default for authors), no per-task cold starts. The lane→session map
lives in the ``lanes`` table (lane_key, session_id, profile, role, node,
created_at, last_task_id) and is re-attached on worker restart by lane_key.
Reviewer lanes (``role="reviewer"``) spawn with ``-m <hermes_reviewer_model>``
(default qwen3.7-plus, the opus tier) so the merge gate accepts their verdicts.

Busy lanes queue (shared/claude-plugins#858, defect 1): a second task for a
lane with an in-flight delivery is NEVER spawned as a concurrent ``--resume``
(hermes refuses: ``SESSION_NOT_OWNED``) — it waits in a per-lane FIFO and is
released, in arrival order, when the running delivery reaps. A residual
refusal that still races through re-queues with bounded backoff; when the
budget is spent the failure carries hermes' own refusal text verbatim —
never ``error: None``. Lane affinity (defect 2): a lane whose ``lanes`` row
names another node must not spawn here either; the task is released to the
board for the affinity-aware scheduler to re-home.

Design:
  - Worker-mode aware spawn + reap, one active spawn per lease at a time
  - Honours lease TTL — renews while the spawn is running
  - Crashed/hung spawn → /fail with reason, never a silent hang
  - Busy lane → per-lane FIFO queue, never a failing concurrent resume
  - Config-driven (worker, profile, model, hermes_profile, hermes_bin)
  - Uses the same peer-token signing as worker_connector.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import errno
import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

from .deliverable_guard import classify_non_deliverable, has_no_turn_stderr
from . import lane_cost as _lane_cost


def _npx_bin() -> str:
    """Resolve the npx launcher for subprocess.Popen without a shell.

    On Windows ``npx`` is ``npx.cmd``; CreateProcess does not apply PATHEXT, so
    spawning the bare name raises FileNotFoundError ("npx not found") even when
    Node is on PATH. shutil.which applies PATHEXT and returns the real file.
    """
    return shutil.which("npx") or "npx"


def _persisted_ids_refresh_interval(default: float = 15.0) -> float:
    """How often a running executor re-reads the persisted spawn table.

    #899: the single-instance lock and the /join 409 are the primary
    defences, but a duplicate executor can still slip in beside a live
    one (an older build without the lock, or a takeover racing the
    watchdog window). The spawn-suppression set used to be built ONCE at
    reconcile, so a record persisted by a sibling after our start was
    invisible and we re-spawned the task. A periodic refresh closes that
    window to one poll cycle; cheap (one indexed SELECT) and env-tunable
    for tests. A negative/zero value disables refreshes (pre-#899
    behaviour).
    """
    raw = os.environ.get("HERMES_CLUSTER_PERSISTED_REFRESH_S", "")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return default


def _record_attempt(record: dict) -> int:
    """#870: the delivery count carried by a persisted spawn record.

    Tolerates pre-#870 records (column absent/NULL) and non-integer junk —
    a reconcile of an old record must not crash the executor start.
    """
    try:
        return max(0, int(record.get("attempt") or 0))
    except (TypeError, ValueError):
        return 0

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class AgentExecutorConfig:
    """Configuration for the agent executor."""
    enabled: bool = False
    profile: str = "alibaba1"
    model: str = "sonnet"
    worker: str = "bdaya-dispatch"  # "bdaya-dispatch" | "hermes"
    poll_interval: float = 15.0  # seconds between poll cycles
    max_concurrent: int = 1  # max simultaneous spawns
    spawn_timeout: float = 1800.0  # max seconds per spawn (30 min)
    # Max idle seconds before a STATEFUL LANE is reaped (its lanes row cleared,
    # hermes session left intact on disk). ``spawn_timeout`` bounds a single
    # DELIVERY; ``lane_idle_timeout`` bounds how long a lane may sit idle with
    # no delivery running. Default 6 hours (six-hour default per the lead
    # requirement, shared/claude-plugins#833 2026-09-10).
    lane_idle_timeout: float = 21600.0
    working_dir: str = ""  # working directory for spawned workers
    hermes_profile: str = "default"  # hermes -p/--profile (hermes worker mode)
    hermes_bin: str = ""  # path to the hermes CLI; empty → resolve at spawn
    # Reviewer lanes pass the opus-tier model explicitly so the merge gate
    # accepts their verdicts; author lanes use the profile default (no -m).
    hermes_reviewer_model: str = "qwen3.7-plus"
    bdaya_dispatch_package: str = "@shared/bdaya-dispatch@latest"
    # #870: deliveries main may re-queue after the reap rejects a
    # NON-DELIVERABLE result body (provider error / echoed brief) before the
    # task is consumed as failed. 3 = two recovery chances after the first
    # rejection: enough to clear a transient provider blip (the fleet hit
    # two in one evening), small enough that a genuinely broken brief cannot
    # burn a node's quota looping.
    retry_limit: int = 3
    # #892: the same floor the worker connector reports against — the executor
    # REFUSES to claim any task while the volume holding working_dir (where
    # hermes-briefs/hermes-results live, i.e. the lanes dir / HERMES_HOME volume)
    # is below it. Sourced from cluster YAML node.min_free_disk_gb (owner
    # ruling: never an env var). None -> default 5.0; 0 -> guard disabled.
    min_free_disk_gb: Optional[float] = None


# ---------------------------------------------------------------------------
# Active spawn tracking
# ---------------------------------------------------------------------------

@dataclass
class ActiveSpawn:
    """Tracks a running spawn (bdaya-dispatch subprocess or native hermes)."""
    task_id: str
    task_title: str
    process: subprocess.Popen
    lease_id: str = ""
    started_at: float = 0.0
    lane_name: str = ""
    mode: str = "bdaya-dispatch"  # which worker mode spawned this (matches config.worker)
    result_path: str = ""  # hermes mode: DELIVERABLE path the lane writes; the executor NEVER opens it (#868)
    result_file: Optional[object] = None  # DEPRECATED (#868): old handle field, kept for reconciled records
    stdout_path: str = ""  # hermes mode: path of the child's inherited stdout (transcript, #868)
    stdout_file: Optional[object] = None  # open handle for the stdout log (hermes)
    stderr_path: str = ""  # hermes mode: path of the child's stderr log
    stderr_file: Optional[object] = None  # open handle for the stderr log (hermes)
    resumed: bool = False  # True when reconstructed from the persisted spawn map
    lane_key: str = ""  # stateful lane identity this delivery belongs to
    role: str = "author"  # author (profile default model) | reviewer (opus tier)
    session_id: str = ""  # hermes session id this lane maps to (lanes table)
    miss_count: int = 0  # consecutive polls where lane was absent from status
    spawn_exit_rc: Optional[int] = None  # set once spawn process exits
    spawn_exit_stderr: str = ""  # captured stderr tail on nonzero exit
    # #858 defect 1: verbatim task dict this delivery was spawned from, kept
    # so a busy-lane refusal can RE-QUEUE the same delivery (with backoff)
    # instead of failing it. Not persisted (the spawn record is for restart
    # tracking, and a refused delivery never started).
    task_payload: Optional[dict] = None
    busy_attempts: int = 0  # refusal-retries already spent by the LANE on this task
    # #870: count of PRIOR deliveries of this task observed at spawn time
    # (from the persisted record; main's task.attempts is the authority).
    # Rides through persistence so an executor restart cannot reset it.
    attempt: int = 0
    # #929: cumulative per-model token buckets for this lane's session read AT
    # SPAWN. The reap-time footnote prices the delta (now - baseline), so a
    # resumed long-lived lane's earlier tasks are not double-counted and this
    # delivery's figure is exactly its own spend. Empty dict = no baseline
    # (fresh session — everything it records belongs to this task — or the
    # store was unreadable at spawn; the footnote discloses either case).
    cost_baseline: Dict[str, Dict[str, int]] = field(default_factory=dict)


class _ResumedProcess:
    """Minimal process stand-in for a spawn reconciled from the persisted map.

    The real subprocess handle is lost when the executor restarts mid-task, so
    a resumed spawn gets this instead: it answers ``pid`` and ``poll()`` like a
    Popen (``poll()`` → None while we believe the process may still be alive,
    or a nonzero rc once we know it cannot be). Completion of a resumed lane is
    then driven by the lane-status/result-file logic, exactly like a live one.
    """

    def __init__(self, pid: int):
        self.pid = pid
        self._exited = False

    def poll(self) -> Optional[int]:
        if self._exited:
            return 0
        try:
            os.kill(self.pid, 0)
            return None
        except ProcessLookupError:
            self._exited = True
            return 0
        except PermissionError:
            return None
        except OSError as exc:
            # Windows has no ESRCH for this probe: os.kill(<dead pid>, 0)
            # raises OSError errno 22 / WinError 87 ("The parameter is
            # incorrect"), measured on Windows 11 + CPython 3.13. Swallowing
            # it as "unknown" made poll() return None forever, so a
            # reconciled spawn could never report an exit. Treat it as
            # process-gone, matching ProcessLookupError on POSIX.
            if os.name == "nt" and (
                getattr(exc, "winerror", None) == 87
                or exc.errno == errno.EINVAL
            ):
                self._exited = True
                return 0
            return None
        except Exception:
            # os.kill can raise on invalid pid kinds; treat as unknown → alive
            return None


# ---------------------------------------------------------------------------
# Signing (same algorithm as worker_connector.py)
# ---------------------------------------------------------------------------

def _resolve_peer_token(explicit: str = "") -> str:
    """Resolve the peer token for signing outbound requests."""
    env_token = os.environ.get("PEER_TOKEN", "")
    if env_token:
        return env_token
    token_path = Path.home() / ".config" / "bdaya" / "hermes-peer-token"
    if token_path.is_file():
        return token_path.read_text().strip()
    if explicit:
        return explicit
    return ""


def _sign_request(
    token: str, node_id: str, method: str, path: str, body: bytes
) -> Dict[str, str]:
    """Sign a request with HMAC-SHA256, returning auth headers."""
    if not token:
        return {}
    ts = int(time.time())
    body_hash = hashlib.sha256(body).hexdigest()
    message = f"{node_id}:{ts}:{method}:{path}:{body_hash}"
    signature = hmac.new(
        token.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    return {
        "X-Peer-Node": node_id,
        "X-Peer-Timestamp": str(ts),
        "X-Peer-Signature": signature,
    }


def _signed_request(
    endpoint: str,
    method: str,
    path: str,
    data: Optional[dict],
    token: str,
    node_id: str,
    timeout: int = 15,
) -> Optional[dict]:
    """Send a signed JSON request to the main node."""
    url = f"{endpoint}{path}"
    body = json.dumps(data).encode() if data else b""
    req = Request(url, data=body if data else None, method=method)
    req.add_header("Content-Type", "application/json")
    headers = _sign_request(token, node_id, method, path, body)
    for key, value in headers.items():
        req.add_header(key, value)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        logger.warning("signed %s %s failed: %s", method, path, e)
        return None
    except Exception as e:
        logger.warning("signed %s %s error: %s", method, path, e)
        return None


# ---------------------------------------------------------------------------
# AgentExecutor
# ---------------------------------------------------------------------------

class AgentExecutor:
    """Polls for leased tasks, spawns bdaya workers, reports results.

    Lifecycle:
        1. Construct with config, node_id, cluster_endpoint
        2. Call start() to launch the background poll thread
        3. Call stop() for clean shutdown
    """

    # Stateful-lane BUSY queue (shared/claude-plugins#858 defect 1): a task
    # arriving for a lane that already has an in-flight delivery must NOT be
    # spawned as a concurrent ``--resume`` (hermes refuses: SESSION_NOT_OWNED).
    # It waits here — FIFO per lane — and is released the moment the running
    # delivery reaps. ``_lane_queue_order`` keeps insertion order per lane so
    # the release is first-in-first-out (priority ordering across a lane is
    # the scheduler's job, before the task reaches the executor).
    _LANE_QUEUE_LIMIT = 64  # per-lane FIFO bound (executor capacity, not state)

    # Bounded backoff for a residual hermes refusal (the race where a lane
    # looked free at claim time and hermes still owns it): retries happen
    # INSIDE this delivery — the task is never failed for a busy-lane
    # refusal. Tunables are class attributes so tests can shrink the waits.
    _LANE_BUSY_MAX_RETRIES = 5
    _LANE_BUSY_BACKOFF_BASE = 2.0

    # Markers for hermes' refusal contract (cli.py -Q prints
    # ``hermes-refusal-reason: <REASON>\n<message>`` to stderr —
    # hermes_cli/active_sessions.py::format_refusal_stderr — and exits 1).
    _REFUSION_REASON_PREFIX = "hermes-refusal-reason:"
    _LANE_BUSY_REFUSALS = frozenset({
        "SESSION_NOT_OWNED",            # a live owner holds the lane session
        "MAX_CONCURRENT_SESSIONS",      # node-level session limit, transient
        "SESSION_COORDINATION_UNAVAILABLE",  # registry unreadable, transient
    })

    def __init__(
        self,
        config: AgentExecutorConfig,
        node_id: str,
        cluster_endpoint: str,
        peer_token: str = "",
        store: Optional[object] = None,
    ):
        self._config = config
        self._node_id = node_id
        self._cluster_endpoint = cluster_endpoint.rstrip("/")
        self._token = _resolve_peer_token(peer_token)
        # Optional ClusterStore/ClusterState for the persisted task->lane map
        # (#804 note 132791). Without a store the executor is stateless —
        # restart will re-spawn (superseded once a store is wired).
        self._store = store

        self._active_spawns: Dict[str, ActiveSpawn] = {}  # task_id -> spawn
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._reconciled = False
        # Persisted task-id cache (F4): reconciled once from the store, then
        # maintained in-memory by _persist_spawn/_drop_persisted_spawn so the
        # poll loop never re-reads SQLite. Reseeded by reconcile on start,
        # and re-read periodically while running (#899 — see
        # _refresh_persisted_ids).
        self._persisted_ids: set = set()
        self._persisted_refresh_s = _persisted_ids_refresh_interval()
        self._last_persisted_refresh = 0.0
        # Per-lane FIFO of task dicts waiting for the lane's in-flight
        # delivery to reap (#858 defect 1). Guarded by self._lock.
        self._lane_queue: Dict[str, List[dict]] = {}
        # Tasks released from _lane_queue when their lane's delivery reaped;
        # drained by the next _claim_and_spawn in FIFO order. Separate dict
        # (not popping straight into spawns) so release stays cheap and
        # lock-holding under _reap_finished_spawns is bounded.
        self._lane_released: Dict[str, List[dict]] = {}
        # task_id -> (busy_refusal_attempts, retry_not_before_epoch): the
        # backoff state a re-queued refusal carries (#858 defect 1). Entries
        # are pruned when the task leaves the queue system (spawn / fail).
        self._lane_queue_meta: Dict[str, tuple] = {}

        # Resolve working directory
        if not self._config.working_dir:
            # Default: D:\projects\devops-aggregate (the aggregate repo)
            default_dir = Path(r"D:\projects\devops-aggregate")
            if default_dir.exists():
                self._config.working_dir = str(default_dir)
            else:
                self._config.working_dir = str(Path.cwd())

    def start(self) -> None:
        """Start the background poll + spawn thread."""
        if self._running:
            logger.warning("agent executor already running")
            return
        self._reconcile_persisted_spawns()
        self._stop_event.clear()
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="agent-executor",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "agent executor started: node=%s profile=%s model=%s max_concurrent=%d",
            self._node_id,
            self._config.profile,
            self._config.model,
            self._config.max_concurrent,
        )

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the executor and wait for the thread to finish."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("agent executor stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active_spawns)

    def status(self) -> dict:
        """Return executor status for diagnostics."""
        with self._lock:
            spawns = []
            for tid, spawn in self._active_spawns.items():
                spawns.append({
                    "task_id": tid,
                    "task_title": spawn.task_title[:80],
                    "lane_name": spawn.lane_name,
                    "lease_id": spawn.lease_id,
                    "mode": spawn.mode,
                    "resumed": spawn.resumed,
                    "running_seconds": round(time.time() - spawn.started_at, 1),
                    "pid": spawn.process.pid,
                    "poll_alive": spawn.process.poll() is None,
                    "result_path": spawn.result_path,
                    "stdout_path": getattr(spawn, "stdout_path", ""),
                })
            return {
                "running": self._running,
                "node_id": self._node_id,
                "profile": self._config.profile,
                "model": self._config.model,
                "worker": getattr(self._config, "worker", "bdaya-dispatch"),
                "max_concurrent": self._config.max_concurrent,
                "active_spawns": len(spawns),
                "spawns": spawns,
            }

    # -------------------------------------------------------------------
    # Main poll loop
    # -------------------------------------------------------------------

    def _poll_loop(self) -> None:
        """Main loop: poll for tasks, manage spawns, report results."""
        logger.info("agent executor poll loop started")
        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except Exception:
                logger.exception("error in agent executor poll cycle")
            self._stop_event.wait(timeout=self._config.poll_interval)
        logger.info("agent executor poll loop ended")

    def _poll_once(self) -> None:
        """Single poll cycle: check results, find new tasks, renew leases."""
        # 1. Check completed/failed spawns
        self._reap_finished_spawns()

        # 1b. Reap idle stateful lanes (lead requirement, #833 2026-09-10):
        # a lane idle past lane_idle_timeout has its lanes row cleared so the
        # next task with that lane_key starts fresh (session left on disk).
        self._reap_idle_lanes()

        # 1c. #858 defect 1: release per-lane FIFO heads whose lane is now
        # idle (in-flight delivery reaped above) and whose backoff has
        # expired, so this cycle's claim pass spawns them.
        self._sweep_lane_queues()

        # 2. Renew leases for active spawns
        self._renew_leases()

        # 3. Find and spawn new tasks if capacity allows
        with self._lock:
            available_slots = self._config.max_concurrent - len(self._active_spawns)
        if available_slots > 0:
            self._claim_and_spawn(available_slots)

    # -------------------------------------------------------------------
    # Task discovery + spawning
    # -------------------------------------------------------------------


    def _is_assigned_to_me(self, assigned_to: str) -> bool:
        """The main registers nodes as ``node_<id>`` (nodes.py join) while the
        executor is configured with the bare ``<id>``; accept both spellings so a
        task the scheduler assigned to this node is actually claimed (live bug:
        cut-over task stuck ``running`` on every node, #804)."""
        if not assigned_to:
            return False
        mine = {self._node_id, f"node_{self._node_id}"}
        return assigned_to in mine or assigned_to.removeprefix("node_") == self._node_id

    def _claim_and_spawn(self, max_spawns: int) -> None:
        """Poll main for assigned tasks and spawn workers for them."""
        # #892 disk preflight: refuse the WHOLE cycle below the configured floor
        # (a full disk killed every lane this node took on 2026-09-14 05:32Z —
        # Errno 28 in concurrent_log_handler, then rc=120). One clear log line;
        # tasks stay unclaimed on the main for re-dispatch to a healthy node.
        from .disk_gate import disk_gate_blocks
        if disk_gate_blocks(self._config.working_dir or "",
                            getattr(self._config, "min_free_disk_gb", None)):
            return
        # Drain lane FIFOs the reap just released: head-of-queue tasks for
        # now-idle lanes spawn first, in arrival order (FIFO), consuming the
        # cycle's spawn budget before any newly-claimed task does.
        ready = self._drain_released_lanes()
        budget = max_spawns
        leftover = []
        for i, task in enumerate(ready):
            if budget <= 0:
                leftover = ready[i:]
                break
            self._spawn_worker(task)
            budget -= 1
        if leftover:
            # Out of executor slots this cycle: put the heads back at the
            # FRONT of their lane queues in arrival order (FIFO preserved;
            # the queued-id filter keeps the board from double-handling
            # them). Insert in REVERSE so each front-to-back pass lands the
            # earlier task ahead of the later one — inserting forward at
            # index 0 reversed each lane's order (#858 review, PR #23).
            with self._lock:
                for task in reversed(leftover):
                    lane_key = task.get("lane_key", "") or ""
                    self._lane_queue.setdefault(lane_key, []).insert(0, task)
        if budget <= 0:
            return

        # GET /api/v1/tasks from main node
        tasks = _signed_request(
            self._cluster_endpoint,
            "GET",
            "/api/v1/tasks",
            None,
            self._token,
            self._node_id,
        )
        if tasks is None:
            logger.warning("failed to fetch tasks from main")
            return

        # Filter: status=running, assigned_to=this node, not already spawning
        with self._lock:
            active_task_ids = set(self._active_spawns.keys())
            queued_task_ids = {
                t.get("id", "")
                for q in self._lane_queue.values() for t in q
            } | {
                t.get("id", "")
                for q in self._lane_released.values() for t in q
            }
        # #899: re-read the persisted spawn table periodically (not only at
        # reconcile) so a record written AFTER our start — by a sibling
        # executor that bypassed the lock, or by ourselves mid-cycle — still
        # suppresses a second spawn of the same task.
        self._refresh_persisted_ids()
        active_task_ids |= self._persisted_spawn_task_ids()

        candidates = []
        for task in tasks:
            task_id = task.get("id", "")
            status = task.get("status", "")
            assigned_to = task.get("assigned_to", "")
            if (
                status == "running"
                and self._is_assigned_to_me(assigned_to)
                and task_id not in active_task_ids
                and task_id not in queued_task_ids
            ):
                candidates.append(task)

        # Sort by priority (lower number = higher priority)
        candidates.sort(key=lambda t: t.get("priority", 3))

        # Spawn up to remaining budget; a candidate whose lane already has an
        # in-flight delivery is QUEUED per lane FIFO instead of being spawned
        # into the busy lane (#858 defect 1: a concurrent ``--resume`` is
        # refused by hermes — SESSION_NOT_OWNED — and the task used to land
        # in `failed` with `error: None`).
        for task in candidates:
            lane_key = task.get("lane_key", "") or ""
            if lane_key and self._lane_has_active_delivery(lane_key):
                self._queue_for_lane(lane_key, task)
                continue
            if budget <= 0:
                # Lane is free but the executor has no slot: leave the task
                # running/assigned on the board — the next cycle picks it up
                # (it is neither active nor queued, so the filter re-sees it).
                continue
            self._spawn_worker(task)
            budget -= 1

    def _write_brief(
        self,
        task_id: str,
        title: str,
        description: str,
        lane_key: str = "",
        role: str = "author",
        deliverable_path: str = "",
    ) -> Path:
        """Write the per-task brief file the guarded worker lane reads.

        ``deliverable_path`` (hermes mode) makes the #868/#871 delivery
        contract EXPLICIT at the one place every lane is guaranteed to read:
        printing the final message is NOT delivery — the lane must WRITE this
        exact file before exiting. (#871: lanes deliver by printing; two
        diligent reviewer lanes in a row did exactly that and were recorded
        FAILED. The executor's transcript fallback catches that now, but the
        contract is stated here so a compliant lane reaps a plain 'done',
        not a marker-stamped promotion a merge gate must refuse.)
        """
        d = Path(self._config.working_dir or ".") / "hermes-briefs"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{task_id}.md"
        lines = [
            f"## Hermes cluster task {task_id}",
            "",
            f"**Title:** {title}",
            "",
        ]
        if lane_key:
            lines += [
                f"**Stateful lane:** `{lane_key}` (**{role}** role)",
                "You are a continuing lane: this task is the next delivery in an existing "
                "lane session. Read the lane's prior context (you are resuming it), and "
                "answer THIS brief as the next message. Keep the lane's thread and "
                "decisions consistent.",
                "",
            ]
        if description.strip():
            lines += [description.strip(), ""]
        lines += [
            "### Standing lane rules",
            "- You are a headless worker spawned by the Hermes cluster executor on node "
            f"`{self._node_id}`; report blockers in your RETURN VALUE, never AskUserQuestion.",
            "- NEVER approve or merge your own work; open MRs as Draft and hand off for independent review.",
            "- Cheap models only; never print a secret value.",
            "- When done, state exactly what you produced (files, MR links, proof) in your final message.",
            "",
        ]
        if role == "author":
            # #893: the author lane hands off to an INDEPENDENT reviewer
            # itself — no lead session in the loop. Written into the SHARED
            # template (the one place every lane reads), not copy-pasted per
            # brief. RV-1: the reviewer runs in a fresh context on a
            # different lane; the author lane MUST NOT approve or merge; a
            # needs-human label stays a live hard gate.
            lines += [
                "### Author hand-off (shared/claude-plugins#893 — no lead in the loop)",
                "- When your work is done and the Draft MR/PR is open, DO NOT stop and wait for a "
                "lead to notice. Submit your own reviewer task with `kanban_cluster_submit`: "
                "`role='reviewer'`, `requires=['review']`, `lane_key='<repo>!<mr_iid>'` (the reviewer "
                "lane key), and a title that carries the MR/PR URL and the exact head sha.",
                "- Exception: a LANDING task (its brief names an existing reviewer PASS at the "
                "current head sha) merges — it does NOT dispatch another reviewer. A reviewer "
                "task only ever comes from an author lane that produced the diff.",
                "- You are the AUTHOR lane. You MUST NOT approve or merge your own MR — RV-1 is not "
                "negotiable. On a reviewer PASS at head the merge is actuated by a dedicated "
                "LANDING task: submit it with role='author' on a landing lane key "
                "(e.g. '<repo>#land-<mr_iid>', never the authoring lane key) carrying the "
                "MR URL, the head sha AND the reviewer's PASS verdict reference. Never by you, "
                "and never by the reviewer lane itself (#882's read-only gate mechanically "
                "denies a reviewer merge).",
                "",
            ]
        if role == "reviewer":
            # #893 close of the loop, same shared template (not per-brief).
            lines += [
                "### Reviewer landing hand-off (shared/claude-plugins#893)",
                "- Post your sha-pinned verdict as an MR note first (the durable oracle).",
                "- On PASS at head: you MUST NOT merge (#882 denies it). Submit the LANDING task "
                "yourself with `kanban_cluster_submit`: `role='author'`, "
                "`lane_key='<repo>#land-<mr_iid>'`, title naming the MR URL, the verified head "
                "sha, and your PASS. On NEEDS-CHANGES: do not submit landing; the verdict note "
                "is the hand-back.",
                "",
            ]
        if deliverable_path:
            lines += [
                "### Delivery contract (cluster executor)",
                f"- Your deliverable is the file `{deliverable_path}`. WRITE it "
                "explicitly with write_file before you exit — your printed final "
                "message is NOT auto-captured there (shared/claude-plugins#868).",
                "- If you exit 0 without writing it, the executor promotes your "
                "stdout transcript into that file under a visible marker and the "
                "task completes as TRANSCRIPT-PROMOTED — never a plain done, and "
                "a merge gate will refuse to treat it as a verdict "
                "(shared/claude-plugins#871). Write the file; it is one call.",
                "",
            ]
        path.write_text(chr(10).join(lines), encoding="utf-8")
        return path

    def _spawn_worker(self, task: dict) -> None:
        """Spawn a worker for the given task, dispatching by ``worker`` mode."""
        # #858 defect 2, executor-side guard: a task assigned to THIS node
        # whose lane row names a DIFFERENT node must not spawn here at all —
        # spawning would build the second session + second clone on the
        # wrong machine (the measured stray-lane incident). The scheduler's
        # affinity keeps assignments honest; this guard catches the delayed
        # case (row written after the assignment raced, or manual override).
        # The task is released back to the board with a reason so the
        # affinity-aware scheduler re-homes it to the lane's node.
        lane_key = task.get("lane_key", "") or ""
        if lane_key and self._lane_placed_elsewhere(lane_key):
            self._release_misplaced_lane_task(task, lane_key)
            return
        # getattr guards the spawn_env unit tests which construct a bare _Cfg
        # (no worker attr) → default to the legacy bdaya-dispatch mode.
        if getattr(self._config, "worker", "bdaya-dispatch") == "hermes":
            self._spawn_hermes_worker(task)
        else:
            self._spawn_bdaya_worker(task)

    def _node_ids_match(self, a: str, b: str) -> bool:
        """Nodes register as ``node_<name>`` while the executor runs with the
        bare ``<name>`` (see _is_assigned_to_me); lane rows store whichever
        spelling the recording node used. Compare both."""
        if not a or not b:
            return False
        return (a == b or a.removeprefix("node_") == b
                or b.removeprefix("node_") == a)

    def _lane_placed_elsewhere(self, lane_key: str) -> bool:
        """True when the lanes table places this lane on another node."""
        if getattr(self, "_store", None) is None:
            return False
        try:
            lane = self._store.get_lane(lane_key)
        except Exception:
            logger.exception("failed to read lane %s placement", lane_key)
            return False
        if not lane:
            return False  # unplaced lane: this node may claim it
        placed = (lane.get("node") or "").strip()
        if not placed:
            return False  # row without a node: unplaced, free to run here
        return not self._node_ids_match(placed, self._node_id)

    def _release_misplaced_lane_task(self, task: dict, lane_key: str) -> None:
        """Hand a misplaced lane task back to the board (release → ready),
        with a reason the operator can read — never spawn it here (#858)."""
        task_id = task.get("id", "")
        assigned_to = task.get("assigned_to") or self._node_id
        lane = None
        try:
            lane = self._store.get_lane(lane_key) if self._store else None
        except Exception:
            pass
        placed = (lane or {}).get("node") or "?"
        reason = (
            f"lane {lane_key} is placed on node {placed}, not {self._node_id} "
            f"— released for affinity re-home (#858)"
        )
        logger.warning("refusing spawn: %s", reason)
        _signed_request(
            self._cluster_endpoint, "POST",
            f"/api/v1/tasks/{task_id}/release",
            {"node_id": assigned_to, "reason": reason},
            self._token, self._node_id,
        )

    def _spawn_bdaya_worker(self, task: dict) -> None:
        """Spawn a bdaya-dispatch worker for the given task."""
        task_id = task.get("id", "")
        task_title = task.get("title", "")
        lane_key = task.get("lane_key", "") or ""
        role = task.get("role", "author") or "author"

        # Build the lane name (must be unique and traceable)
        lane_name = f"hermes-{task_id}"

        # bdaya-dispatch's `run` contract requires --goal (one line) TOGETHER with
        # --brief-file (an already-written file the lane reads); a bare --goal is
        # refused. The goal is the task title (one line); the brief carries the
        # full task text plus the standing lane rules.
        goal = " ".join(task_title.split())[:300] or task_id
        brief_path = self._write_brief(
            task_id, task_title, task.get("description") or "",
            lane_key=lane_key, role=role,
        )

        # Spawn: npx -y -p @shared/bdaya-dispatch bdaya-dispatch run
        #   --name <lane_name> --goal <goal> --model <model>
        cmd = [
            _npx_bin(), "-y",
            "-p", self._config.bdaya_dispatch_package,
            "bdaya-dispatch", "run",
            "--name", lane_name,
            "--goal", goal,
            "--brief-file", str(brief_path),
            "--model", self._config.model,
            "--profile", self._config.profile,
            "--require-goal",
            "--force",
        ]

        # bdaya-dispatch resolves the CALLER's cpm profile from CLAUDE_CONFIG_DIR and
        # refuses outside a .claude-profiles root ("not a cpm multi-account machine").
        # Under a service that variable is unset, so point it at the executor's own
        # profile directory unless the operator already set it.
        env = dict(os.environ)
        env.setdefault(
            "CLAUDE_CONFIG_DIR",
            str(Path.home() / ".claude-profiles" / self._config.profile),
        )

        logger.info(
            "spawning worker for task %s: lane=%s goal=%s",
            task_id, lane_name, goal[:80],
        )

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=self._config.working_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                # On Windows, create a new process group so we can kill the tree
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0,
            )
        except FileNotFoundError:
            logger.error(
                "npx not found — cannot spawn bdaya-dispatch. "
                "Ensure Node.js and npm are on PATH."
            )
            self._report_failure(task_id, "executor_error: npx not found on PATH")
            return
        except Exception as e:
            logger.error("failed to spawn worker for task %s: %s", task_id, e)
            self._report_failure(task_id, f"executor_error: {e}")
            return

        # Find the lease for this task (to track for renewal)
        lease_id = self._find_lease_for_task(task_id)

        spawn = ActiveSpawn(
            task_id=task_id,
            task_title=task_title,
            process=proc,
            lease_id=lease_id,
            started_at=time.time(),
            lane_name=lane_name,
            mode="bdaya-dispatch",
            lane_key=lane_key,
            role=role,
        )

        with self._lock:
            self._active_spawns[task_id] = spawn
        self._persist_spawn(spawn)

        logger.info(
            "spawned worker: task=%s pid=%d lane=%s",
            task_id, proc.pid, lane_name,
        )

    def _resolve_hermes_bin(self) -> str:
        """Resolve the hermes CLI binary for the hermes worker mode."""
        if self._config.hermes_bin:
            return self._config.hermes_bin
        # Shebang launcher installed by the official install script.
        candidates = [
            str(Path.home() / ".local" / "bin" / "hermes"),
            str(Path.home() / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes"),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return shutil.which("hermes") or "hermes"

    def _hermes_result_path(self, task_id: str) -> Path:
        """Directory/result file a native hermes lane writes on completion."""
        d = Path(self._config.working_dir or ".") / "hermes-results"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{task_id}.result.md"

    def _hermes_stdout_path(self, task_id: str) -> Path:
        """Transcript file for a native hermes lane's child STDOUT (#868).

        This — NOT the result file — is the path the executor opens and keeps
        for the child's lifetime so stdout inheritance works. Before #868 the
        same handle served ``<task>.result.md``: on Windows the parent's open
        handle made the lane's own tmp-then-rename write to that path fail
        with a sharing violation, so whole deliverables (a NEEDS-CHANGES
        verdict in the incident that prompted this) stranded as
        ``.hermes-tmp.*`` while a truncated stdout copy read as a pass.
        Transcript and deliverable are genuinely different things; each now
        has its own path.
        """
        d = Path(self._config.working_dir or ".") / "hermes-results"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{task_id}.stdout.log"

    @staticmethod
    def _remove_quiet(path: Path) -> None:
        """Delete a file if present; never raise. Used for spawn-time cleanup
        of a previous delivery's files — a file another (live) process holds
        open cannot be deleted on Windows and is left alone."""
        try:
            path.unlink()
        except OSError:
            pass

    @staticmethod
    def _open_stdout_log(path: Path):
        """Open the child's inherited-stdout transcript log (#868).

        Fresh spawn: truncate. A file that survives the spawn-time cleanup
        below is held open by a still-live (crashed-child) writer, in which
        case truncating would destroy its transcript and 'a' at least keeps
        the delivery's own bytes appended instead of failing mid-spawn.
        """
        if path.exists():
            return open(path, "a", encoding="utf-8")
        return open(path, "w", encoding="utf-8")

    @staticmethod
    def _stranded_tmps(results_dir: Path, since: float) -> List[Path]:
        """.hermes-tmp.* files in the results dir modified at/after ``since``.

        A lane's deliverable write stages a ``.hermes-tmp.XXXXXX`` and renames
        it over the target; on Windows the rename fails against an open file,
        so the tmp is all that survives (shared/claude-plugins#868). Modified
        within the window of the delivery that just finished — a stale temp
        from a previous run is not evidence about THIS task, and the file is
        surfaced (never deleted) either way.
        """
        out: List[Path] = []
        try:
            entries = list(results_dir.glob(".hermes-tmp.*"))
        except OSError:
            return out
        for entry in entries:
            try:
                if entry.is_file() and entry.stat().st_mtime >= since:
                    out.append(entry)
            except OSError:
                continue
        return out

    # -------------------------------------------------------------------
    # First-line diagnostic (#892): the 2026-09-14 full-disk deaths left the
    # LAST stderr line as fail_reason and nothing in the result file. The
    # runner now writes a sentinel into the task's result file BEFORE starting
    # hermes; a clean lane overwrites the path (write_file) or transcript
    # promotion replaces it, a dead lane leaves the sentinel plus appended
    # crash diagnostics. Machine-checkable first line, same contract as the
    # #871 PROMOTION_MARKER below.
    # -------------------------------------------------------------------

    RESULT_SENTINEL = (
        "<!-- LANE-STARTED: the executor wrote this before launching hermes; "
        "the lane never delivered a result. Crash diagnostics follow (if any) "
        "— NOT a verdict (shared/claude-plugins#892). -->"
    )

    def _write_result_sentinel(self, result_path: Path, task_id: str) -> bool:
        """Best-effort sentinel write; False when even this fails (disk full!).
        The spawn proceeds regardless — the sentinel is a diagnostic bonus,
        never a precondition of running a lane."""
        try:
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(
                f"{self.RESULT_SENTINEL}\n<!-- task: {task_id} -->\n",
                encoding="utf-8",
            )
            return True
        except OSError as e:
            logger.warning(
                "could not write result sentinel for task %s at %s: %s",
                task_id, result_path, e,
            )
            return False

    @staticmethod
    def _content_after_sentinel(contents: str) -> str:
        """Strip our own executor-written lines so a sentinel-only file reads
        as NO content. Without this, the #892 diagnostic would itself become a
        false-completion body at the rc==0 content gate (the pre-#870 trap):
        a file whose only lines are ours is not a lane deliverable."""
        if not contents:
            return contents
        lines = contents.splitlines()
        while lines and (
            lines[0] == AgentExecutor.RESULT_SENTINEL
            or lines[0].startswith("<!-- task:")
        ):
            lines.pop(0)
        return "\n".join(lines).strip()


    def _append_crash_diagnostics(self, spawn: "ActiveSpawn", text: str) -> None:
        """Guarantee the sentinel + crash block in the lane's result file (rc!=0
        / timeout reap) — the #892 first-line diagnostic.

        Three cases: (a) file absent (sentinel write itself failed on the full
        disk, or the lane deleted it) → write sentinel + diagnostics fresh, the
        whole point being a dead lane leaves SOMETHING; (b) first line is our
        sentinel → append the crash block; (c) any other content (the lane
        delivered) → never rewritten. Silent on any OSError: fail_reason
        carries the same detail regardless, so this write must not break the
        reap itself.
        """
        path = getattr(spawn, "result_path", "") or ""
        if not path:
            return
        try:
            p = Path(path)
            if not p.is_file():
                self._write_result_sentinel(p, getattr(spawn, "task_id", "?"))
                # fall through and append even if the sentinel write failed —
                # direct append is the last best-effort chance.
            else:
                with p.open("r", encoding="utf-8", errors="replace") as f:
                    first = f.readline()
                if not first.startswith("<!-- LANE-STARTED:"):
                    return
            with p.open("a", encoding="utf-8") as f:
                f.write(text)
        except OSError:
            pass

    # Marker line stamped at the top of a promoted result.md (#871). Loud for
    # humans, exact for machines: a merge gate can grep the first line and
    # refuse to treat the file as a verdict. Keep in sync with tests.
    PROMOTION_MARKER = (
        "<!-- TRANSCRIPT-PROMOTED: the executor copied this from the child's "
        "stdout; the lane did not write a deliverable. NOT a verdict-grade "
        "result (shared/claude-plugins#871). -->"
    )

    def _write_promoted_result(
        self, result_path: Path, stdout_path: Path,
        task_id: str, transcript_text: str,
    ) -> bool:
        """Promote a transcript into the deliverable path, VISIBLY (#871).

        Writes marker + provenance + the transcript bytes to ``result_path``
        via tmp-then-rename — the same mechanism the lane itself uses, and
        safe on Windows because after #868 the executor holds no handle on
        result.md. Returns False on any OSError; the caller must then surface
        the loss loudly, never silently.
        """
        body = (
            f"{self.PROMOTION_MARKER}\n"
            f"<!-- promoted_from: {stdout_path} task: {task_id} "
            f"promoted_at: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} -->\n\n"
            f"{transcript_text}"
        )
        tmp = result_path.with_name(f".hermes-promoted-tmp.{task_id}")
        try:
            result_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(body, encoding="utf-8")
            try:
                os.replace(tmp, result_path)
            except OSError:
                # Windows refuses a rename over a path a live process holds
                # open (the #868 mechanic). The lane may have exited leaving
                # the path to another writer — direct open('w') still lands
                # the promotion on POSIX and anywhere the path is merely
                # locked-but-writable; if THAT fails too the loss is real.
                try:
                    result_path.write_text(body, encoding="utf-8")
                finally:
                    self._remove_quiet(tmp)
            return True
        except OSError:
            self._remove_quiet(tmp)
            return False


    def _hermes_stderr_path(self, task_id: str) -> Path:
        """Directory/log file where a native hermes lane's stderr is captured.

        The session id line (``session_id: <id>``) is written to stderr on every
        exit (cli.py:4087), including success — so stderr is captured to a file,
        not a PIPE, and parsed on reap (F2). A PIPE would also deadlock a chatty
        child that floods stderr beyond the buffer while never being drained.
        """
        d = Path(self._config.working_dir or ".") / "hermes-results"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{task_id}.stderr.log"

    def _delivery_model_flag(self, role: str) -> List[str]:
        """Role → model mapping for a native hermes delivery.

        Author lanes use the profile default (no ``-m``); reviewer lanes pass
        the opus-tier model so the merge gate accepts their verdicts. The ``-m/``
        ``--model`` flag is accepted both top-level and on ``chat``
        (hermes_cli/_parser.py:126-128, :211-212); ``qwen3.7-plus`` is the
        opus-tier catalog entry (hermes_cli/models_catalog_static.py:139).
        """
        if role == "reviewer" and self._config.hermes_reviewer_model:
            return ["-m", self._config.hermes_reviewer_model]
        return []

    def _spawn_hermes_worker(self, task: dict) -> None:
        """Spawn a NATIVE non-interactive hermes session for the given task.

        Invocation (verified against hermes_cli on this node — see brief):
          hermes -p <profile> chat --query-file <brief> -Q [-m <reviewer model>]
        - ``chat --query-file`` (hermes_cli/_parser.py:194-202) reads the single
          query from a file byte-for-byte (no shell quoting), runs to
          completion, and exits.
        - ``-Q/--quiet`` (hermes_cli/_parser.py:225-226) suppresses the banner/
          spinner/tool previews; only the final response and session info are
          printed, and cli.py exits 0 on success / non-zero on failure
          (cli.py:4089-4101).
        - ``-p <profile>`` (hermes_cli/main.py:508-559) is consumed before
          parsing and sets HERMES_HOME for the chosen profile.

        Stateful lanes (task.lane_key): a task whose lane_key already has a
        live lane RESUMES that hermes session — ``--resume <session_id>``
        (hermes_cli/_parser.py:227-229; history restored into conversation by
        ``cli.setup_mixin._preload_resumed_session`` at cli.py:3633/cli_agent_setup_mixin.py:616-660,
        then the query-file is delivered as the next user message —
        cli.py:4060). Only a NEW lane_key spawns a fresh session (``-c
        <lane_key> --create-if-missing``, hermes_cli/_parser.py:235-241 /
        main.py:1359-1397). Session ids are captured from stderr on reap and
        recorded in the ``lanes`` table.

        Track: pid + exit code + result file (NOT bdaya-dispatch lane status).
        """
        task_id = task.get("id", "")
        task_title = task.get("title", "")
        lane_key = task.get("lane_key", "") or ""
        role = task.get("role", "author") or "author"

        lane_name = f"hermes-{task_id}"
        result_path = self._hermes_result_path(task_id)
        stdout_path = self._hermes_stdout_path(task_id)
        stderr_path = self._hermes_stderr_path(task_id)
        brief_path = self._write_brief(
            task_id, task_title, task.get("description") or "",
            lane_key=lane_key, role=role,
            deliverable_path=str(result_path),
        )

        # Spawn-time cleanup of a PREVIOUS delivery's files (#868): the result
        # check below is content-based, so a stale result.md from an earlier
        # run of this task id must not mark a live lane done (#851 semantics —
        # previously the open(result_path, "w") truncate provided it). If a
        # crashed child still holds its stdout/stderr logs open we cannot
        # delete them (Windows sharing violation); opening in append mode
        # below then protects the orphan's transcript from truncation, and a
        # fresh result.md is still removed so the new deliverable has a free
        # path.
        self._remove_quiet(result_path)
        self._remove_quiet(stdout_path)
        self._remove_quiet(stderr_path)
        # NOTE (#892): the executor deliberately does NOT create result.md at
        # spawn time — the #868 contract is that the deliverable path must not
        # pre-exist (a lane's tmp-then-rename can fail with WinError 183 when
        # it does, and test_agent_executor_result_file_868 proves both). The
        # first-line diagnostic instead lands HERE at reap: _reap_hermes_spawn
        # writes the sentinel+diagnostics file for a lane that died without
        # one, which is the moment we know it never delivered.


        # Stateful lane resolution: resume an existing live lane's session,
        # else this is the lane's first delivery (fresh titled session).
        resume_session_id = ""
        if lane_key:
            # Belt and braces (#858 defect 1): never resume a lane with a
            # delivery in flight on this node — hermes would refuse the
            # second writer (SESSION_NOT_OWNED). Queue it FIFO instead,
            # carrying any refusal-attempt count already spent on this task.
            if self._lane_has_active_delivery(lane_key):
                attempts = self._lane_queue_meta.get(task_id, (0, 0.0))[0]
                self._queue_for_lane(lane_key, task, attempts=attempts)
                return
            lane = self._store.get_lane(lane_key) if self._store else None
            if lane and lane.get("session_id"):
                resume_session_id = lane["session_id"]
                logger.info(
                    "lane %s has live session %s — resuming it for task %s",
                    lane_key, resume_session_id, task_id,
                )

        hermes_bin = self._resolve_hermes_bin()
        cmd = [
            hermes_bin,
            "-p", self._config.hermes_profile,
            "chat",
            "--query-file", str(brief_path),
            "-Q",
        ]
        cmd[3:3] = self._delivery_model_flag(role)
        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])
        elif lane_key:
            # First delivery of this lane: create a session titled lane_key so
            # it can be resumed by name/id later (hermes_cli/main.py:1359-1397).
            cmd.extend(["-c", lane_key, "--create-if-missing"])

        logger.info(
            "spawning native hermes worker for task %s: %s",
            task_id, " ".join(cmd),
        )

        # Hold the STDOUT + stderr transcript files open for the child's
        # lifetime (closing the parent handle too early breaks stdout
        # inheritance on Windows). The DELIVERABLE path (result.md) is never
        # opened here — #868: holding it open is what made the lane's own
        # tmp-then-rename write fail on Windows and silently stranded whole
        # deliverables.
        stdout_file = None
        stderr_file = None
        try:
            stdout_file = self._open_stdout_log(stdout_path)
            stderr_file = open(stderr_path, "w", encoding="utf-8")
            proc = subprocess.Popen(
                cmd,
                cwd=self._config.working_dir,
                # Agent final response (stdout) lands in the transcript log;
                # stderr (incl. the session_id line) goes to its own log file.
                stdout=stdout_file,
                stderr=stderr_file,
                # On Windows, create a new process group so we can kill the tree
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0,
            )
        except FileNotFoundError:
            for f in (stdout_file, stderr_file):
                if f is not None:
                    try:
                        f.close()
                    except Exception:
                        pass
            logger.error(
                "hermes not found — cannot spawn native worker (looked for %s). "
                "Ensure hermes is installed or set agent_executor.hermes_bin.",
                hermes_bin,
            )
            self._report_failure(task_id, "executor_error: hermes not found on PATH")
            return
        except Exception as e:
            for f in (stdout_file, stderr_file):
                if f is not None:
                    try:
                        f.close()
                    except Exception:
                        pass
            logger.error("failed to spawn native hermes worker for task %s: %s", task_id, e)
            self._report_failure(task_id, f"executor_error: {e}")
            return

        lease_id = self._find_lease_for_task(task_id)

        spawn = ActiveSpawn(
            task_id=task_id,
            task_title=task_title,
            process=proc,
            lease_id=lease_id,
            started_at=time.time(),
            lane_name=lane_name,
            mode="hermes",
            result_path=str(result_path),
            stdout_path=str(stdout_path),
            stdout_file=stdout_file,
            stderr_path=str(stderr_path),
            stderr_file=stderr_file,
            lane_key=lane_key,
            role=role,
            session_id=resume_session_id,
            resumed=bool(resume_session_id),
            task_payload=dict(task),  # #858: re-queue source on busy refusal
            busy_attempts=self._lane_queue_meta.get(task_id, (0, 0.0))[0],
            attempt=self._prior_attempt(task_id, task),
            # #929: snapshot the session's cumulative usage NOW (a resumed
            # lane's earlier rows belong to earlier deliveries and must not be
            # priced against this one). A fresh lane has no rows yet — empty
            # baseline, and the full session total is this task's spend.
            cost_baseline=_lane_cost.spawn_baseline(
                self._config.hermes_profile, resume_session_id
            ) if resume_session_id else {},
        )

        with self._lock:
            self._active_spawns[task_id] = spawn
        self._persist_spawn(spawn)

        logger.info(
            "spawned native hermes worker: task=%s pid=%d result=%s stdout=%s",
            task_id, proc.pid, result_path, stdout_path,
        )

    # -------------------------------------------------------------------
    # Persisted task->lane map (#804 note 132791)
    # -------------------------------------------------------------------

    def _persist_spawn(self, spawn: ActiveSpawn) -> None:
        """Persist a spawn record so a mid-task restart does not re-spawn."""
        if getattr(self, "_store", None) is None:
            return
        try:
            self._persisted_ids.add(spawn.task_id)
            self._store.record_task_spawn(
                task_id=spawn.task_id,
                mode=spawn.mode,
                job_id=spawn.lane_name,
                pid=spawn.process.pid,
                started_at=spawn.started_at,
                lease_id=spawn.lease_id,
                lane_name=spawn.lane_name,
                result_path=spawn.result_path,
                lane_key=spawn.lane_key,
                role=spawn.role,
                session_id=spawn.session_id,
                attempt=spawn.attempt,
            )
            # Reflect the lane in the stateful-lanes table whenever a lane_key
            # exists (session_id filled in later, on reap, from stderr).
            if spawn.lane_key:
                self._store.record_lane(
                    lane_key=spawn.lane_key,
                    session_id=spawn.session_id,
                    profile=self._config.hermes_profile,
                    role=spawn.role,
                    node=self._node_id,
                    last_task_id=spawn.task_id,
                )
        except Exception:
            logger.exception("failed to persist spawn record for task %s", spawn.task_id)

    def _persisted_spawn_task_ids(self) -> set:
        """Task ids with a persisted spawn record (live spawns must not re-spawn).

        Served from the in-memory cache (F4) — the store is read once at
        reconcile, then kept in sync by _persist_spawn/_drop_persisted_spawn.
        """
        return set(self._persisted_ids)

    def _reconcile_persisted_spawns(self) -> None:
        """Re-load the persisted task->lane map into the in-memory spawn table.

        On restart mid-task, the executor must resume tracking the lanes it
        already spawned — NOT spawn new ones. Records are rehydrated into
        ActiveSpawn with a process stand-in; completion is still detected by
        the mode-specific reap logic (lane status / hermes result file).
        """
        if getattr(self, "_store", None) is None:
            self._reconciled = True
            return
        if self._reconciled:
            return
        try:
            records = self._store.get_all_task_spawns()
        except Exception:
            logger.exception("failed to reconcile persisted spawn records")
            self._reconciled = True
            return
        reconstituted = 0
        # Reseed the persisted-id cache from the store (F4) so the poll loop
        # never re-reads SQLite per cycle.
        self._persisted_ids = {r.get("task_id", "") for r in records} - {""}
        with self._lock:
            for record in records:
                task_id = record.get("task_id", "")
                if not task_id or task_id in self._active_spawns:
                    continue
                if self._reattach_record_locked(record):
                    reconstituted += 1
        self._last_persisted_refresh = time.time()
        self._reconciled = True
        if reconstituted:
            logger.info(
                "reconciled %d persisted spawn(s) from store — resuming tracking, "
                "NOT re-spawning", reconstituted,
            )

    def _reattach_record_locked(self, record: dict) -> bool:
        """Rehydrate one persisted spawn record into the active-spawn table.

        Caller holds self._lock. Returns True when a spawn was reattached.
        #899: shared by reconcile-on-start and the periodic
        _refresh_persisted_ids pass, so a record that appears WHILE the
        executor runs (written by a sibling that bypassed the lock) is
        tracked against its live pid exactly like a restart-time record —
        the task never gets a second session and is reaped normally.
        """
        task_id = record.get("task_id", "")
        if not task_id or task_id in self._active_spawns:
            return False
        lane_key = record.get("lane_key") or ""
        role = record.get("role") or "author"
        session_id = record.get("session_id") or ""
        # Worker-restart re-attach by lane_key: if the record does not
        # carry a session id but the lanes table knows the lane's
        # session, recover it so a resume (not a fresh spawn) follows.
        if lane_key and not session_id and getattr(self, "_store", None) is not None:
            try:
                lane = self._store.get_lane(lane_key)
                if lane:
                    session_id = lane.get("session_id") or ""
            except Exception:
                logger.exception("failed to read lane %s during reconcile", lane_key)
        # #899: the record carries the child's pid — wrap it in the
        # OS-level liveness stand-in so the reap keeps polling the REAL
        # process (os.kill probe), not a guess.
        spawn = ActiveSpawn(
            task_id=task_id,
            task_title=record.get("job_id") or task_id,
            process=_ResumedProcess(int(record.get("pid") or 0)),
            lease_id=record.get("lease_id") or "",
            started_at=float(record.get("started_at") or time.time()),
            lane_name=record.get("lane_name") or f"hermes-{task_id}",
            mode=record.get("mode") or "bdaya-dispatch",
            result_path=record.get("result_path") or "",
            attempt=_record_attempt(record),
            stdout_path=(
                str(self._hermes_stdout_path(task_id))
                if record.get("mode") == "hermes"
                else ""
            ),
            stderr_path=(
                record.get("stderr_path")
                or str(self._hermes_stderr_path(task_id))
                if record.get("mode") == "hermes"
                else record.get("stderr_path") or ""
            ),
            resumed=True,
            lane_key=lane_key,
            role=role,
            session_id=session_id,
        )
        self._active_spawns[task_id] = spawn
        if lane_key:
            # Block a duplicate worker for the lane's task (the spawn is
            # live-tracked again, not re-delivered to the same lane).
            logger.info(
                "reattached lane %s (session %s) to task %s after restart",
                lane_key, session_id or "?", task_id,
            )
        return True

    def _refresh_persisted_ids(self) -> None:
        """Re-read the persisted spawn table while running (#899).

        Cheap guard behind the single-instance lock: any record that
        appeared after reconcile — the fingerprint of a second executor
        for this node id — is added to the suppression cache AND
        re-attached live against its pid, so this executor spawns zero
        sessions for it and the normal reap logic takes over. Throttled
        to _persisted_refresh_s (env-tunable; <0 disables for pre-#899
        behaviour).
        """
        if getattr(self, "_store", None) is None:
            return
        if self._persisted_refresh_s < 0:
            return
        now = time.time()
        if now - self._last_persisted_refresh < self._persisted_refresh_s:
            return
        self._last_persisted_refresh = now
        try:
            records = self._store.get_all_task_spawns()
        except Exception:
            logger.warning("failed to refresh persisted spawn records", exc_info=True)
            return
        fresh = {r.get("task_id", "") for r in records} - {""}
        added = fresh - self._persisted_ids
        self._persisted_ids = fresh
        if not added:
            return
        logger.warning(
            "#899: %d spawn record(s) appeared after this executor started "
            "(%s) — a second executor for node '%s' is writing them; "
            "re-attaching instead of spawning",
            len(added), sorted(added), self._node_id,
        )
        with self._lock:
            for record in records:
                if record.get("task_id", "") in added:
                    self._reattach_record_locked(record)

    def _drop_persisted_spawn(self, task_id: str) -> None:
        """Remove a terminal spawn's record so a future run may spawn again."""
        if getattr(self, "_store", None) is None:
            return
        try:
            self._persisted_ids.discard(task_id)
            self._store.delete_task_spawn(task_id)
        except Exception:
            logger.exception("failed to drop persisted spawn record for task %s", task_id)

    # -------------------------------------------------------------------
    # Reaping finished spawns
    # -------------------------------------------------------------------

    # Terminal states per bdaya-dispatch contract (verified v3.63.7):
    #   done/completed → success; stopped/failed/missing/ambiguous → terminal failure.
    #   blocked is ACTIVE (blocked-self = waiting on subagent, blocked-human = needs input).
    _TERMINAL_DONE = frozenset({"done", "completed"})
    _TERMINAL_FAIL = frozenset({"stopped", "failed", "missing", "ambiguous"})

    # Registration grace: don't fail a lane for absence until this many
    # consecutive misses OR this many seconds have elapsed.
    _MISS_GRACE_COUNT = 4  # ~60s at default 15s poll_interval
    _MISS_GRACE_SECONDS = 90.0

    def _reap_finished_spawns(self) -> None:
        """Poll lane status for active spawns and report terminal states.

        bdaya-dispatch run backgrounds the lane and exits 0 immediately, so
        process exit codes are NOT useful for completion detection. Instead,
        query ``bdaya-dispatch status --json`` and inspect the lane state.

        Contract (verified against bdaya-dispatch v3.63.7):
          - done/completed → /complete
          - stopped/failed/missing/ambiguous → /fail
          - blocked/working/running/pending → keep waiting
          - spawn_timeout is outer bound, but a done lane observed late still completes
          - query failure (not just empty result) → keep waiting (bounded by timeout)
          - lane absent from status → grace window before failing
        """
        if not self._active_spawns:
            return

        # Native hermes lanes are tracked by process exit + result file, not by
        # bdaya-dispatch lane status — skip the npx status query if every active
        # spawn is a hermes worker.
        have_bdaya_lane = any(
            s.mode != "hermes" for s in self._active_spawns.values()
        )
        lane_states = self._query_all_lane_statuses() if have_bdaya_lane else None

        # Also check spawn process exit for diagnostic info (F7)
        self._capture_spawn_exits()

        resolved = []  # (task_id, spawn, outcome, detail)
        with self._lock:
            for task_id, spawn in list(self._active_spawns.items()):
                elapsed = time.time() - spawn.started_at

                # Hermes workers: reap by pid + exit code + result file.
                if spawn.mode == "hermes":
                    self._reap_hermes_spawn(task_id, spawn, elapsed, resolved)
                    continue

                # 1. Check lane state FIRST (before timeout) — F3
                if lane_states is not None:
                    state = lane_states.get(spawn.lane_name)

                    if state is not None:
                        state_lower = state.lower()

                        # Reset miss counter on presence
                        spawn.miss_count = 0

                        if state_lower in self._TERMINAL_DONE:
                            detail = f"lane completed in {elapsed:.0f}s"
                            resolved.append((task_id, spawn, "done", detail))
                            continue
                        if state_lower in self._TERMINAL_FAIL:
                            detail = f"lane state={state} after {elapsed:.0f}s"
                            resolved.append((task_id, spawn, state_lower, detail))
                            continue
                        # else: active state (working/blocked/running/pending) → keep waiting

                    else:
                        # Lane absent from status — count misses with grace — F2
                        spawn.miss_count += 1

                        # If spawn process exited nonzero and lane never appeared,
                        # report spawn diagnostics — F7
                        if spawn.spawn_exit_rc is not None and spawn.spawn_exit_rc != 0:
                            detail = (
                                f"spawn exited rc={spawn.spawn_exit_rc}, "
                                f"lane never registered"
                            )
                            if spawn.spawn_exit_stderr:
                                detail += f": {spawn.spawn_exit_stderr[:300]}"
                            resolved.append((task_id, spawn, "spawn_failed", detail))
                            continue

                        # Grace window: don't fail until enough misses or time elapsed
                        if (spawn.miss_count >= self._MISS_GRACE_COUNT
                                and elapsed >= self._MISS_GRACE_SECONDS):
                            detail = (
                                f"lane not found after {spawn.miss_count} polls "
                                f"({elapsed:.0f}s)"
                            )
                            resolved.append((task_id, spawn, "missing", detail))
                            continue
                        # else: still within grace → keep waiting

                # 2. Timeout is outer bound for non-terminal lanes — F3
                if elapsed > self._config.spawn_timeout:
                    logger.warning(
                        "spawn timeout (%.0fs) for task %s (lane=%s)",
                        self._config.spawn_timeout,
                        task_id,
                        spawn.lane_name,
                    )
                    resolved.append((
                        task_id, spawn, "timeout",
                        f"exceeded {self._config.spawn_timeout:.0f}s",
                    ))
                    continue

                # 3. Query failure → keep waiting (bounded by timeout above)
                if lane_states is None:
                    logger.debug(
                        "status query failed, keeping spawn active: task=%s lane=%s",
                        task_id, spawn.lane_name,
                    )

        for task_id, spawn, outcome, detail in resolved:
            with self._lock:
                self._active_spawns.pop(task_id, None)
            # A terminal spawn leaves the queue system entirely — drop any
            # backoff meta it carried (fresh deliveries start at attempts 0).
            self._lane_queue_meta.pop(task_id, None)
            # #858 defect 1: a residual busy-lane refusal is re-queued with
            # bounded backoff (outside the reap lock; it mutates the queue
            # state) and must not fall through to the failure report.
            # _queue_for_lane re-seeds the meta with attempts+1.
            if outcome == "lane_busy_refused":
                if self._requeue_busy_refusal(spawn, task_id, detail):
                    continue
                # Retry budget spent: fail — but carry HERMES' OWN refusal
                # text verbatim, never a bare None (the #858 diagnosis: a
                # failure the operator cannot explain is a failure missed).
                self._drop_persisted_spawn(task_id)
                self._lane_queue_meta.pop(task_id, None)
                self._report_failure(task_id, (
                    f"lane {spawn.lane_key} busy after "
                    f"{self._LANE_BUSY_MAX_RETRIES} retries — "
                    f"hermes said: {detail}"
                ))
                continue
            # Stateful lane: capture the hermes session id from this delivery's
            # stderr into the lanes table BEFORE the record is dropped, so the
            # next task with the same lane_key resumes that session. A
            # promoted delivery is still a real, completed turn — the next
            # brief must resume the same session, so it gets the same care
            # (#871).
            if (outcome in ("done", "transcript_promoted")
                    and spawn.mode == "hermes"):
                self._touch_lane_from_spawn(spawn, task_id)
                # #929: the executor-side cost footnote. The #860 enforcement
                # hook appends the footnote to the lane's PRINTED final text —
                # which since #868 is NOT the deliverable unless it was
                # promoted — and it is dead on any node whose pinned
                # bdaya-enforcement predates #860. Reap-time, the child has
                # exited and its session's usage rows are on disk: price the
                # delta against the spawn-time baseline and append to the
                # DELIVERABLE itself, so every cluster lane result carries its
                # own credit figure regardless of hook state. Fail-open.
                self._append_cost_footnote(spawn)
            # A terminal lane's persisted record is dropped so a future run of
            # the same task may spawn again; an active lane's record survives
            # restarts and blocks a duplicate spawn.
            self._drop_persisted_spawn(task_id)
            self._lane_queue_meta.pop(task_id, None)  # done with queue state

            if outcome in ("done", "transcript_promoted"):
                # A promoted transcript COMPLETES the task — that is the #871
                # fix: no false failure, no re-dispatch loop. The
                # distinction is not lost: result.md carries a loud marker
                # and the executor logs a warning (see _write_promoted_result).
                self._report_completion(
                    task_id, detail=detail, result=self._read_result_body(spawn)
                )
            elif outcome == "non_deliverable":
                # #870: the result body was NOT a deliverable (provider/
                # transport error or an echo of the dispatched brief). The
                # task must not be consumed — re-queue under the cap so a
                # genuinely broken brief cannot loop forever. The attempt
                # count rides the spawn (authoritative across executor
                # restarts via the persisted record) — it is read BEFORE
                # _drop_persisted_spawn cleared the store row.
                self._requeue_task(
                    task_id, outcome, f"lane {spawn.lane_name}: {detail}",
                    attempt=spawn.attempt,
                )
            else:
                self._report_failure(task_id, f"lane {spawn.lane_name}: {detail}")

    # -------------------------------------------------------------------
    # Deliverable-content guard (#870)
    # -------------------------------------------------------------------

    def _brief_text_for(self, task_id: str) -> str:
        """The dispatched brief the executor itself wrote for this task.

        ``_write_brief`` puts it at ``<working_dir>/hermes-briefs/<task>.md``
        and it is deterministic from task fields; a retry rebuilds the same
        text. Absent (bdaya mode, pruned dir) → '' so the echo rule no-ops
        rather than guessing.
        """
        path = (
            Path(self._config.working_dir or ".") / "hermes-briefs"
            / f"{task_id}.md"
        )
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def _prior_attempt(self, task_id: str, task: Optional[dict] = None) -> int:
        """Deliveries already re-queued for this task (the cap's numerator).

        Authority is main's own counter: the scheduler hands the task dict
        around (GET /api/v1/tasks) and the router bumps ``attempts`` on every
        re-queued /fail, so it survives executor restarts and is shared
        across nodes. Fallback (key absent — an older main predating the
        field): the persisted spawn record left from an earlier delivery.
        No record → 0.
        """
        if task is not None and "attempts" in task:
            try:
                return max(0, int(task["attempts"]))
            except (TypeError, ValueError):
                pass
        store = getattr(self, "_store", None)
        if store is None:
            return 0
        try:
            record = store.get_task_spawn(task_id)
        except Exception:
            record = None
        try:
            return int((record or {}).get("attempt") or 0)
        except (TypeError, ValueError):
            return 0

    def _requeue_task(self, task_id: str, reason: str, detail: str,
                      attempt: int = 0) -> None:
        """Tell main this delivery failed as a non-deliverable (#870).

        Same endpoint as a plain failure (POST /fail) with ``requeue=true``:
        under the retry cap main resets the task to ready for another
        delivery; at/over the cap it consumes the task as failed so a
        genuinely broken brief cannot loop. The cap lives on the MAIN node —
        the scheduler's source of truth — so the count survives executor
        restarts and is authoritative across nodes; the executor sends the
        attempt it observed only as a courtesy, never as the authority.
        """
        limit = int(getattr(self._config, "retry_limit", 3))
        will_exceed = attempt >= limit
        body = {"reason": f"{reason}: {detail}"[:2000], "requeue": not will_exceed}
        result = _signed_request(
            self._cluster_endpoint,
            "POST",
            f"/api/v1/tasks/{task_id}/fail",
            body,
            self._token,
            self._node_id,
        )
        if result:
            if will_exceed or not result.get("requeued", False):
                logger.error(
                    "task %s consumed as failed (%s, attempt %d, cap %d): %s",
                    task_id, reason, attempt + 1, limit, detail[:200],
                )
            else:
                logger.warning(
                    "task %s re-queued (attempt %d/%d, %s): %s",
                    task_id, attempt + 1, limit, reason, detail[:200],
                )
        else:
            logger.error(
                "failed to report non-deliverable for task %s (%s) — the "
                "task will be retried after its lease expires",
                task_id, reason,
            )

    def _reap_hermes_spawn(
        self,
        task_id: str,
        spawn: ActiveSpawn,
        elapsed: float,
        resolved: List,
    ) -> None:
        """Reap a native hermes spawn by its process exit + result file.

        Completion contract (hermes ``chat --query-file ... -Q``):
          - RC 0 and a non-empty result file → done
          - RC 0 without a result file but with a non-empty transcript and no
            stranded tmp → transcript_promoted (the transcript is copied into
            result.md under a loud marker — completes the task but is never a
            verdict-grade done; shared/claude-plugins#871)
          - RC 0 with nothing to deliver → fail (agent produced nothing)
          - RC != 0 → fail with captured stderr tail
          - still running → keep waiting (bounded by spawn_timeout)
        A resumed spawn (executor restarted mid-task) drives completion off the
        result file: if it exists and is non-empty the lane finished writing
        even though the original process handle is gone.
        """
        rc = spawn.process.poll()

        # A resolved hermes spawn releases its stdout/stderr transcript
        # handles (the child has exited by then, so the close only releases
        # the parent's copies). The DELIVERABLE is never a held handle (#868).
        if rc is not None:
            for attr in ("stdout_file", "result_file", "stderr_file"):
                fh = getattr(spawn, attr, None)
                if fh is not None:
                    try:
                        fh.flush()
                        fh.close()
                    except Exception:
                        pass
                    finally:
                        setattr(spawn, attr, None)

        # The DELIVERABLE (result.md) is the primary completion signal for
        # hermes (#868): the agent writes its final response there with its
        # own write_file, then exits 0. Content still proves nothing while the
        # process is alive — the lane may be mid-write, and #851's lesson
        # (never resolve a LIVE lane as done on file content alone) carries
        # over: a lane is done only once it has EXITED cleanly with content.
        result_ok = bool(spawn.result_path) and Path(spawn.result_path).is_file()
        # Reap-time lost-deliverable check (#868): a .hermes-tmp.* in the
        # results dir written within THIS delivery's window is a deliverable
        # whose rename never landed — silent loss is the whole bug, so it is
        # surfaced loudly (and never swept up) ahead of any 'done'.
        stranded = []
        if rc is not None:
            results_dir = (
                Path(spawn.result_path).parent if spawn.result_path
                else Path(self._config.working_dir or ".") / "hermes-results"
            )
            stranded = self._stranded_tmps(results_dir, spawn.started_at)

        if rc == 0 and stranded:
            names = ", ".join(str(p) for p in stranded)
            resolved.append((
                task_id, spawn, "lost_deliverable",
                f"hermes exited rc=0 but {len(stranded)} stranded "
                f".hermes-tmp.* file(s) from this delivery were never renamed "
                f"into place (lost deliverable — shared/claude-plugins#868): "
                f"{names}",
            ))
            return

        if rc == 0 and result_ok:
            try:
                contents = Path(spawn.result_path).read_text(
                    encoding="utf-8", errors="replace"
                )
            except OSError:
                contents = ""
            # #892: drop our own pre-spawn sentinel lines BEFORE judging — a
            # file containing only the sentinel is a died-lane diagnostic, not
            # content, and must fall through to the no-deliverable paths below.
            contents = self._content_after_sentinel(contents)
            if contents.strip():
                # #870: non-empty is NOT sufficient — two production shapes
                # rode this exact gate to a fabricated 'completed': a
                # provider/transport error body and the dispatched brief
                # echoed back (a reviewer lane — an unreviewed MR passes a
                # gate that trusts the status word). The classifier is
                # conservative by design: a real deliverable may quote its
                # brief and may say the word "error".
                stderr_text = self._read_spawn_stderr(spawn) or ""
                reason = classify_non_deliverable(
                    contents,
                    self._brief_text_for(task_id),
                    is_error_response=("API failed after" in stderr_text),
                )
                if reason:
                    no_turn = has_no_turn_stderr(stderr_text)
                    detail = (
                        f"result body is not a deliverable ({reason}"
                        + ("; agent produced no turn — session restored zero "
                           "messages" if no_turn else "")
                        + f"): {contents.strip()[:200]!r}"
                    )
                    logger.error(
                        "#870 false-completed guard fired: task %s (%s) — "
                        "reaping as %s, never 'done'",
                        task_id, reason, "non_deliverable",
                    )
                    resolved.append((task_id, spawn, "non_deliverable", detail))
                    return
                if has_no_turn_stderr(stderr_text):
                    # The body passed the guards but the resumed session
                    # restored ZERO messages — the lane answered without its
                    # prior context (#870 R2-1 class). Deliverable wins over
                    # veto (a fresh execution of the same query can be real
                    # work — vetoing here would reject good lanes), but the
                    # consumer must not read the status word blind.
                    logger.warning(
                        "task %s: deliverable accepted but stderr shows "
                        "'found but has no messages' — the resumed session "
                        "had no history; verify the body used the right "
                        "lane context",
                        task_id,
                    )
                resolved.append((
                    task_id, spawn, "done",
                    f"hermes result written in {elapsed:.0f}s ({spawn.result_path})",
                ))
                return

        if rc is not None and rc != 0:
            detail = f"hermes exited rc={rc} after {elapsed:.0f}s"
            # #858 defect 1: a residual busy-lane refusal (the lane looked
            # free at spawn time; hermes still owns the session) must NOT
            # fail the task — mark it for re-queue with bounded backoff.
            # Detection is the refusal REASON contract on stderr
            # (``hermes-refusal-reason: SESSION_NOT_OWNED…``), not an rc
            # guess. The actual re-queue runs in _reap_finished_spawns after
            # the spawn-table lock is released (non-reentrant).
            if spawn.lane_key:
                try:
                    stderr_now = self._read_spawn_stderr(spawn)
                except Exception:
                    stderr_now = ""
                reason = self._refusal_reason(stderr_now)
                if reason in self._LANE_BUSY_REFUSALS:
                    resolved.append((
                        task_id, spawn, "lane_busy_refused",
                        self._refusal_text(stderr_now) or detail,
                    ))
                    return
            if spawn.spawn_exit_stderr:
                detail += f": {spawn.spawn_exit_stderr[:300]}"
            # #892: a crashed lane leaves the FIRST-LINE diagnostic in its
            # result file, not just the last stderr line in fail_reason.
            self._append_crash_diagnostics(
                spawn,
                f"\n## Crash diagnostics (written by the executor at reap)\n\n"
                f"- outcome: rc={rc} after {elapsed:.0f}s\n"
                + (f"- last stderr:\n\n```\n{spawn.spawn_exit_stderr}\n```\n"
                   if spawn.spawn_exit_stderr else "- no stderr captured\n"),
            )
            resolved.append((task_id, spawn, "spawn_failed", detail))
            return

        if rc is not None and rc == 0:
            # Exited cleanly with no deliverable. Lanes deliver by PRINTING
            # their final message; pre-#868 that print WAS result.md (the
            # inherited-stdout handle), so it reaped done. Post-#868 the print
            # lands in stdout.log and result.md must be written explicitly —
            # and diligent lanes were reaping a false FAILURE (the #871
            # specimens: two live verdicts posted to !280, both recorded
            # FAILED, feeding an unbounded re-dispatch loop).
            #
            # So the transcript is promoted to the deliverable — but NEVER
            # silently. #868's refusal still governs the !23 trap (a truncated
            # transcript whose per-section "Verdict: CORRECT" lines read as a
            # pass): (a) a stranded .hermes-tmp.* was already surfaced above
            # as lost_deliverable and returns before ever reaching here; (b)
            # promotion writes a loud machine-checkable header marker into
            # result.md; and (c) the outcome is ``transcript_promoted``, never
            # plain ``done`` — a merge gate can and must refuse to treat it
            # as a verdict (shared/claude-plugins#871).
            transcript_text = ""
            if getattr(spawn, "stdout_path", ""):
                try:
                    if Path(spawn.stdout_path).is_file():
                        transcript_text = Path(spawn.stdout_path).read_text(
                            encoding="utf-8", errors="replace"
                        )
                except OSError:
                    transcript_text = ""
            if transcript_text.strip() and spawn.result_path:
                marker_written = self._write_promoted_result(
                    Path(spawn.result_path), Path(spawn.stdout_path),
                    task_id, transcript_text,
                )
                if marker_written:
                    resolved.append((
                        task_id, spawn, "transcript_promoted",
                        f"hermes exited rc=0 after {elapsed:.0f}s without "
                        f"writing {spawn.result_path}; the executor PROMOTED "
                        f"the transcript at {spawn.stdout_path} into it with "
                        f"a visible marker — outcome transcript_promoted, NOT "
                        f"a lane verdict (shared/claude-plugins#871)",
                    ))
                    logger.warning(
                        "task %s: transcript promoted to deliverable "
                        "(lane printed its final message instead of writing "
                        "result.md) — NOT a verdict-grade 'done' (#871)",
                        task_id,
                    )
                    return
                # Promotion itself failed (e.g. path held by another live
                # writer): fall through to the loud lost-deliverable surface —
                # silent loss is still the bug #868 exists to prevent.
                resolved.append((
                    task_id, spawn, "lost_deliverable",
                    f"hermes exited rc=0 after {elapsed:.0f}s but wrote no "
                    f"deliverable at {spawn.result_path} and the transcript "
                    f"promotion write FAILED; transcript at "
                    f"{spawn.stdout_path} — do NOT treat it as the verdict "
                    f"(shared/claude-plugins#868 #871)",
                ))
                return
            if transcript_text.strip():
                resolved.append((
                    task_id, spawn, "lost_deliverable",
                    f"hermes exited rc=0 after {elapsed:.0f}s but wrote no "
                    f"deliverable at {spawn.result_path}; transcript present "
                    f"at {spawn.stdout_path} — do NOT treat it as the verdict "
                    f"(shared/claude-plugins#868)",
                ))
                return
            resolved.append((
                task_id, spawn, "no_result",
                f"hermes exited rc=0 after {elapsed:.0f}s but wrote no result",
            ))
            # #892: same class as a crash — a lane that leaves nothing gets the
            # first-line diagnostic written by the executor at reap.
            self._append_crash_diagnostics(
                spawn,
                f"\n## Crash diagnostics (written by the executor at reap)\n\n"
                f"- outcome: rc=0 after {elapsed:.0f}s but NO deliverable "
                f"(lane printed its final message without writing result.md)\n",
            )
            return

        # Timeout is the outer bound for a still-running lane. Kill the child so
        # a timed-out hermes run doesn't keep burning quota as an orphan (F1).
        if rc is None and elapsed > self._config.spawn_timeout:
            logger.warning(
                "hermes spawn timeout (%.0fs) for task %s — killing pid %s",
                self._config.spawn_timeout, task_id, getattr(spawn.process, "pid", "?"),
            )
            self._kill_spawn_process(spawn)
            # #892: a timed-out lane is a crashed lane — leave the first-line
            # diagnostic unless the lane already delivered something.
            self._append_crash_diagnostics(
                spawn,
                f"\n## Crash diagnostics (written by the executor at reap)\n\n"
                f"- outcome: killed after exceeding {self._config.spawn_timeout:.0f}s "
                f"spawn timeout\n",
            )
            resolved.append((
                task_id, spawn, "timeout",
                f"exceeded {self._config.spawn_timeout:.0f}s",
            ))

    def _kill_spawn_process(self, spawn: ActiveSpawn) -> None:
        """Best-effort terminate of a spawn's child process (tree on Windows)."""
        proc = spawn.process
        if proc is None or isinstance(proc, _ResumedProcess):
            return  # nothing to kill (reconciled record holds no live handle)
        try:
            if os.name == "nt":
                proc.kill()
            else:
                proc.terminate()
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def _capture_spawn_exits(self) -> None:
        """Check if any spawn processes have exited and capture diagnostics."""
        with self._lock:
            for spawn in self._active_spawns.values():
                if spawn.spawn_exit_rc is not None:
                    continue  # already captured
                rc = spawn.process.poll()
                if rc is not None:
                    spawn.spawn_exit_rc = rc
                    # Hermes stderr now lives in a per-spawn log file (F2), so
                    # read it for diagnostics AND for the session_id line.
                    if rc != 0 or spawn.mode == "hermes":
                        try:
                            stderr_data = self._read_spawn_stderr(spawn)
                            if stderr_data:
                                lines = stderr_data.strip().split("\n")
                                spawn.spawn_exit_stderr = "\n".join(lines[-5:])[:500]
                        except Exception:
                            pass

    def _read_spawn_stderr(self, spawn: ActiveSpawn) -> str:
        """Read a spawn's stderr: from the log file (hermes) or the PIPE."""
        if getattr(spawn, "stderr_path", ""):
            try:
                return Path(spawn.stderr_path).read_text(
                    encoding="utf-8", errors="replace"
                )
            except OSError:
                return ""
        try:
            if spawn.process.stderr:
                data = spawn.process.stderr.read() or b""
            elif spawn.process.stdout:
                data = spawn.process.stdout.read() or b""
            else:
                return ""
            if isinstance(data, bytes):
                return data.decode(errors="replace")
            return str(data)
        except Exception:
            return ""

    def _extract_session_id(self, stderr_text: str) -> str:
        """Parse the ``session_id: <id>`` line hermes prints to stderr (cli.py:4063,4087).

        Lines look like ``session_id: 20260910_120000_ab12cd`` or
        ``session_id: <id>``; return the id or '' when absent. Best-effort,
        never raises.
        """
        if not stderr_text:
            return ""
        for line in stderr_text.splitlines():
            line = line.strip()
            if line.startswith("session_id:"):
                val = line.split(":", 1)[1].strip()
                if val:
                    return val
        return ""

    def _append_cost_footnote(self, spawn: ActiveSpawn) -> None:
        """#929 — append this delivery's credit footnote to its result file.

        Runs at reap, after the child exited (its usage rows are on disk) and
        after _touch_lane_from_spawn bound spawn.session_id. Every failure
        mode is silent-by-design (fail-open): a meter that breaks lane
        delivery is worse than no meter. Idempotent via the shared marker —
        when the #860 enforcement hook already footnoted the print (and it
        became the deliverable), this pass is a no-op.
        """
        try:
            path = Path(spawn.result_path) if spawn.result_path else None
            if path is None or not path.is_file():
                return
            if not spawn.session_id:
                return
            footnote = _lane_cost.footnote_for_delivery(
                profile=self._config.hermes_profile,
                session_id=spawn.session_id,
                baseline=spawn.cost_baseline or {},
                lane_key=spawn.lane_key,
            )
            if footnote is None:
                return
            if _lane_cost.append_footnote(path, footnote):
                logger.info(
                    "#929: cost footnote appended to lane result %s (lane %s)",
                    path, spawn.lane_key or "?",
                )
        except Exception:
            logger.exception("#929: cost footnote pass failed (delivery unaffected)")

    def _touch_lane_from_spawn(self, spawn: ActiveSpawn, task_id: str) -> None:
        """Record a lane's session id + last task into the lanes table.

        Called on reap for a hermes delivery: read the stderr log for hermes'
        ``session_id`` line and persist it, so the next task with the same
        lane_key resumes that session (``--resume <session_id>``).
        """
        if not spawn.lane_key or getattr(self, "_store", None) is None:
            return
        try:
            stderr_text = self._read_spawn_stderr(spawn)
            session_id = self._extract_session_id(stderr_text)
            self._store.record_lane(
                lane_key=spawn.lane_key,
                session_id=session_id or spawn.session_id,
                profile=self._config.hermes_profile,
                role=spawn.role,
                node=self._node_id,
                last_task_id=task_id,
            )
            if session_id and session_id != spawn.session_id:
                spawn.session_id = session_id
                logger.info(
                    "lane %s now bound to hermes session %s (task %s)",
                    spawn.lane_key, session_id, task_id,
                )
        except Exception:
            logger.exception("failed to record lane session for lane %s", spawn.lane_key)

    def _reap_idle_lanes(self) -> int:
        """Reap stateful lanes idle past ``lane_idle_timeout``.

        A lane is idle when NO delivery is currently running for it AND its
        last activity (``last_active_at``, falling back to ``created_at``) is
        older than ``agent_executor.lane_idle_timeout`` (default 6 h). Reaping
        clears the ``lanes`` ROW only — the hermes session itself is left
        intact on disk (``--resume``/``-c`` can still re-attach) — so the next
        task with that lane_key starts fresh. Returns the number of lanes
        reaped.
        """
        store = getattr(self, "_store", None)
        if store is None or not getattr(store, "get_all_lanes", None):
            return 0
        timeout = getattr(self._config, "lane_idle_timeout", 21600.0)
        if timeout <= 0:
            return 0
        try:
            lanes = store.get_all_lanes()
        except Exception:
            logger.exception("failed to list lanes during idle reap")
            return 0

        # A lane with an in-flight delivery is NOT idle — keep it. Neither is
        # a lane with deliveries queued behind that delivery (#858: clearing
        # the row mid-queue would drop the session the queued briefs are
        # meant to resume).
        with self._lock:
            busy_lane_keys = {
                s.lane_key for s in self._active_spawns.values() if s.lane_key
            } | set(self._lane_queue.keys()) | set(self._lane_released.keys())

        reaped = 0
        now = time.time()
        for lane in lanes:
            lane_key = lane.get("lane_key", "")
            if not lane_key or lane_key in busy_lane_keys:
                continue
            last_active = lane.get("last_active_at") or lane.get("created_at") or 0
            try:
                idle_seconds = now - float(last_active)
            except (TypeError, ValueError):
                idle_seconds = float("inf")
            if idle_seconds <= timeout:
                continue
            try:
                if store.delete_lane(lane_key):
                    reaped += 1
                    logger.info(
                        "reaped idle stateful lane %s (idle %.0fs > %.0fs) — "
                        "lanes row cleared, hermes session left on disk",
                        lane_key, idle_seconds, timeout,
                    )
            except Exception:
                logger.exception("failed to reap idle lane %s", lane_key)
        return reaped

    def _query_all_lane_statuses(self) -> Optional[Dict[str, str]]:
        """Run ``bdaya-dispatch status --json`` and return {lane_name: state}.

        Returns ``None`` on query failure (timeout, unparseable, npx missing)
        so the caller can distinguish "no lanes" from "query broke".

        Parses stdout regardless of exit code — bdaya-dispatch exits 1 as a
        health alarm after printing valid JSON (F1).
        """
        cmd = [
            _npx_bin(), "-y",
            "-p", self._config.bdaya_dispatch_package,
            "bdaya-dispatch", "status", "--json",
        ]
        env = dict(os.environ)
        env.setdefault(
            "CLAUDE_CONFIG_DIR",
            str(Path.home() / ".claude-profiles" / self._config.profile),
        )
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=self._config.working_dir or None,
                env=env,
            )
            # Parse stdout regardless of rc — rc=1 is a health alarm, not an error (F1)
            if not result.stdout or not result.stdout.strip():
                logger.warning(
                    "bdaya-dispatch status returned no output (rc=%d)",
                    result.returncode,
                )
                return None
            try:
                data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                logger.warning("failed to parse bdaya-dispatch status JSON: %s", e)
                return None
            lanes_list = data.get("lanes")
            if not isinstance(lanes_list, list):
                logger.warning("bdaya-dispatch status 'lanes' is not a list")
                return None
            return {
                lane["lane"]: lane["state"]
                for lane in lanes_list
                if isinstance(lane, dict) and "lane" in lane and "state" in lane
            }
        except subprocess.TimeoutExpired:
            logger.warning("bdaya-dispatch status timed out")
            return None
        except FileNotFoundError:
            logger.warning("npx not found when querying lane status")
            return None

    # -------------------------------------------------------------------
    # Lease renewal
    # -------------------------------------------------------------------

    def _renew_leases(self) -> None:
        """Extend leases for active spawns to prevent TTL expiry."""
        with self._lock:
            spawns_with_leases = [
                (tid, s) for tid, s in self._active_spawns.items()
                if s.lease_id
            ]

        for task_id, spawn in spawns_with_leases:
            result = _signed_request(
                self._cluster_endpoint,
                "POST",
                f"/api/v1/leases/{spawn.lease_id}/extend",
                {},
                self._token,
                self._node_id,
            )
            if result:
                logger.debug("lease renewed: task=%s lease=%s", task_id, spawn.lease_id)
            else:
                logger.warning(
                    "lease renewal failed: task=%s lease=%s — may expire",
                    task_id, spawn.lease_id,
                )

    def _lane_has_active_delivery(self, lane_key: str) -> bool:
        """True when this node has an in-flight spawn for ``lane_key``."""
        with self._lock:
            return any(
                s.lane_key == lane_key for s in self._active_spawns.values()
            )

    def _queue_for_lane(self, lane_key: str, task: dict,
                        attempts: int = 0) -> bool:
        """Append a task to its lane's FIFO; returns False when the queue is
        full (the caller then leaves the task alone — the board still shows
        it running/assigned and a later poll can retry the queueing).

        ``attempts`` carries a busy-refusal retry count through re-queueing
        (a first queue is attempts=0). Queueing happens purely in the
        executor: the task's status on the cluster is untouched (#858: "a
        queued task stays `ready` rather than failing" — nothing here moves
        it toward `failed`)."""
        task_id = task.get("id", "")
        with self._lock:
            q = self._lane_queue.setdefault(lane_key, [])
            if len(q) >= self._LANE_QUEUE_LIMIT:
                return False
            if any(t.get("id") == task_id for t in q):
                return True  # already queued (dedupe against poll rescan)
            q.append(task)
            self._lane_queue_meta[task_id] = (
                attempts, time.time() + self._lane_busy_backoff(attempts))
        logger.info(
            "lane %s busy — queued task %s (FIFO depth %d) instead of "
            "spawning into a busy lane (#858)",
            lane_key, task_id, len(q),
        )
        return True

    def _lane_busy_backoff(self, attempts: int) -> float:
        """Bounded exponential backoff before a re-queued refusal retries.

        attempts=0 (never refused — straight busy-queue) releases as soon as
        the lane is free; each spent retry doubles the wait from base."""
        if attempts <= 0:
            return 0.0
        return self._LANE_BUSY_BACKOFF_BASE * (2 ** (attempts - 1))

    def _refusal_reason(self, stderr_text: str) -> str:
        """Parse hermes' machine-readable refusal contract from stderr.

        ``-Q`` mode prints ``hermes-refusal-reason: <REASON>`` then the human
        message (hermes_cli/active_sessions.py::format_refusal_stderr).
        Returns the REASON token ('' when stderr carries no refusal) — the
        reason is the contract; the message is for people."""
        for line in (stderr_text or "").splitlines():
            line = line.strip()
            if line.startswith(self._REFUSION_REASON_PREFIX):
                return line.split(":", 1)[1].strip()
        return ""

    def _refusal_text(self, stderr_text: str) -> str:
        """Verbatim hermes refusal (reason line + message) for the failure
        report — 'whatever Hermes actually said lands in error' (#858)."""
        out = []
        for line in (stderr_text or "").splitlines():
            s = line.strip()
            if s.startswith(self._REFUSION_REASON_PREFIX):
                out = [s]
            elif out:
                out.append(s)
        return "\n".join(out).strip()

    def _sweep_lane_queues(self) -> None:
        """Release per-lane FIFO heads that may spawn now.

        A head is released when (a) its lane has no in-flight delivery on
        this node AND (b) its busy-refusal backoff deadline has passed (for
        freshly queued tasks the deadline is immediate). Released heads go
        to ``_lane_released`` for the same cycle's claim pass to spawn in
        FIFO order.
        """
        now = time.time()
        with self._lock:
            if not self._lane_queue:
                return
            busy_lane_keys = {
                s.lane_key for s in self._active_spawns.values() if s.lane_key
            }
            moved = []
            for lane_key in list(self._lane_queue.keys()):
                if lane_key in busy_lane_keys:
                    continue  # lane still has a delivery in flight — wait
                q = self._lane_queue.get(lane_key) or []
                if not q:
                    self._lane_queue.pop(lane_key, None)
                    continue
                head = q[0]
                head_id = head.get("id", "")
                meta = self._lane_queue_meta.get(head_id)
                if meta and meta[1] > now:
                    continue  # backoff not expired for this head
                q.pop(0)
                if not q:
                    self._lane_queue.pop(lane_key, None)
                self._lane_released.setdefault(lane_key, []).append(head)
                moved.append(head_id)
        if moved:
            logger.info(
                "lane freed/backoff expired — released queued task(s) %s "
                "for spawn (#858)", ", ".join(moved),
            )

    def _requeue_busy_refusal(
        self, spawn: "ActiveSpawn", task_id: str, refusal: str,
    ) -> bool:
        """A residual hermes busy-lane refusal: re-queue (bounded backoff)
        instead of failing, and surface the verbatim refusal for the failure
        report if the retry budget is spent. Returns True when re-queued.

        'Residual' = the refusal raced the queue: the lane looked free at
        spawn time and hermes (or another surface) still owns the session.
        The retry count rides on the queue meta, so the per-lane FIFO keeps
        one consistent order even across refusals.
        """
        attempts = spawn.busy_attempts + 1
        if attempts > self._LANE_BUSY_MAX_RETRIES:
            return False  # exhausted — caller fails with the verbatim text
        # Re-queue at the TAIL of the lane FIFO with one more attempt spent:
        # fairness for whatever queued behind this delivery.
        payload = dict(spawn.task_payload or {"id": task_id})
        payload.setdefault("id", task_id)
        lane_key = spawn.lane_key
        ok = self._queue_for_lane(lane_key, payload, attempts=attempts)
        if ok:
            logger.warning(
                "lane %s refused task %s (%s) — re-queued with bounded "
                "backoff (attempt %d/%d) instead of failing (#858)",
                lane_key, task_id, refusal, attempts,
                self._LANE_BUSY_MAX_RETRIES,
            )
            self._drop_persisted_spawn(task_id)
        return ok

    def _drain_released_lanes(self) -> List[dict]:
        """Pop every released task in FIFO order (per lane) for spawning."""
        with self._lock:
            if not self._lane_released:
                return []
            drained: List[dict] = []
            for lane_key in list(self._lane_released.keys()):
                drained.extend(self._lane_released.pop(lane_key))
            return drained

    def _find_lease_for_task(self, task_id: str) -> str:
        """Find the active lease ID for a task from the main node."""
        leases = _signed_request(
            self._cluster_endpoint,
            "GET",
            "/api/v1/leases",
            None,
            self._token,
            self._node_id,
        )
        if leases is None:
            return ""
        for lease in leases:
            if (
                lease.get("task_id") == task_id
                and lease.get("node_id") == self._node_id
                and lease.get("status") == "active"
            ):
                return lease.get("id", "")
        return ""

    # -------------------------------------------------------------------
    # Reporting results back to the cluster
    # -------------------------------------------------------------------

    # #874: cap on the deliverable carried over the wire. A result is normally
    # a verdict or a short report; anything past this is a transcript dump, and
    # the tail is the part that matters (verdict lines land at the end), so the
    # HEAD is dropped and the truncation is announced in-band.
    RESULT_BODY_MAX_BYTES = 256 * 1024

    def _read_result_body(self, spawn: Any) -> Optional[str]:
        """The lane's deliverable, for transport to the main node (#874).

        Returns None when there is nothing worth carrying. Never raises: a
        completion must not fail because a result file is unreadable -- losing
        the body is bad, losing the completion is worse.
        """
        path = getattr(spawn, "result_path", "") or ""
        if not path:
            return None
        try:
            body = Path(path).read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            logger.warning("could not read result body at %s: %s", path, exc)
            return None
        # #892: a sentinel-only file is the executor's own diagnostic, never a
        # deliverable — carry nothing (same strip as the completion gate).
        body = self._content_after_sentinel(body)
        if not body.strip():
            return None
        raw = body.encode("utf-8")
        if len(raw) > self.RESULT_BODY_MAX_BYTES:
            kept = raw[-self.RESULT_BODY_MAX_BYTES:].decode("utf-8", errors="replace")
            logger.warning(
                "result body truncated for transport: task result at %s is %d bytes",
                path, len(raw),
            )
            return (
                f"[truncated: {len(raw)} bytes, kept the last "
                f"{self.RESULT_BODY_MAX_BYTES}]" + chr(10) + kept
            )
        return body

    def _report_completion(
        self, task_id: str, detail: str = "", result: Optional[str] = None
    ) -> None:
        """Mark a task as completed on the main node, carrying its deliverable.

        #874: this used to POST an empty {} and discard `detail`, so a result
        lived only on the local disk of whichever node ran the lane -- a verdict
        produced on one machine was unreadable from every other.
        """
        payload = {"result": result} if result else {}
        result_ok = _signed_request(
            self._cluster_endpoint,
            "POST",
            f"/api/v1/tasks/{task_id}/complete",
            payload,
            self._token,
            self._node_id,
        )
        if result_ok:
            logger.info(
                'task %s marked completed: %s (result: %s)',
                task_id, detail or 'ok',
                f'{len(result)} chars carried' if result else 'none',
            )
        else:
            logger.error('failed to mark task %s completed', task_id)

    def _report_failure(self, task_id: str, reason: str,
                        fallback: str = "") -> bool:
        """Mark a task as failed on the main node.

        #858: a failure must carry a real reason — the busy-lane incident
        reached operators as `failed` with `error: None`. A blank reason is
        replaced by *fallback* when the caller has one, and the report is
        REFUSED (returns False, loudly logged) when neither exists: no
        reasonless failure goes on the wire.
        """
        reason = (reason or "").strip() or (fallback or "").strip()
        if not reason:
            logger.error(
                "REFUSING reasonless failure report for task %s (#858): "
                "the caller supplied no reason and no fallback — fixing "
                "the caller is required; the task stays un-failed and "
                "visible as stuck", task_id)
            return False
        result = _signed_request(
            self._cluster_endpoint,
            "POST",
            f"/api/v1/tasks/{task_id}/fail",
            {"reason": reason},
            self._token,
            self._node_id,
        )
        if result:
            logger.info("task %s marked failed: %s", task_id, reason[:100])
        else:
            logger.error("failed to mark task %s failed", task_id)
        return bool(result)
