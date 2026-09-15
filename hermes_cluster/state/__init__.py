"""In-memory state management — replaces Go's concurrent maps + mutexes.

Thread-safe via threading.Lock (GIL helps, but we're explicit).
"""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..models import (
    BatchSyncMessage,
    Delivery,
    Delivery,
    Hook,
    Lease,
    LeaseStatus,
    Node,
    NodeStatus,
    RecoveryEvent,
    RemoteCluster,
    SchedulingDecision,
    SchedulingStats,
    SyncMessage,
    SyncEventType,
    Task,
    TaskStatus,
    EventType,
    FederationClusterStatus,
)
from ..core.scheduler import (
    ACTIVE_TASK_STATUSES,
    TERMINAL_TASK_STATUSES,
    FairScheduler,
    lane_blocked_ready_ids,
)
from ..core.lane_affinity import AffinityScheduler
from ..core.landing_gate import hold_reason as landing_hold_reason


def _generate_id(prefix: str = "") -> str:
    """Generate a random hex ID, similar to Go's generateID."""
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


class ClusterState:
    """Central in-memory state for the cluster.

    Each sub-store is protected by its own lock for fine-grained concurrency.
    """

    def __init__(self):
        import threading

        # Node registry
        self._nodes_lock = threading.Lock()
        self._nodes: Dict[str, Node] = {}
        self._on_node_online: Optional[Callable[[str], None]] = None
        self._on_capability_change: Optional[Callable[[str, List[str], List[str]], None]] = None

        # Task store
        self._tasks_lock = threading.Lock()
        self._tasks: Dict[str, Task] = {}

        # Lease manager
        self._leases_lock = threading.Lock()
        self._leases: Dict[str, Lease] = {}
        self._task_lease_index: Dict[str, str] = {}  # task_id -> lease_id
        self._lease_callback: Optional[Callable[[str, str], None]] = None

        # Sync state
        self._sync_lock = threading.Lock()
        self._sync_version: int = 0

        # Recovery log
        self._recovery_lock = threading.Lock()
        self._recovery_events: List[RecoveryEvent] = []

        # Scheduling decisions
        self._schedule_lock = threading.Lock()
        self._decisions: List[SchedulingDecision] = []
        self._max_decisions: int = 200

        # Fair scheduler (least-loaded node, round-robin ties, capacity-aware)
        # + lane-to-node affinity (#858 defect 2).
        self._fair_scheduler = AffinityScheduler()

        # Federation registry
        self._federation_lock = threading.Lock()
        self._clusters: Dict[str, RemoteCluster] = {}

        # Hook manager
        self._hooks_lock = threading.Lock()
        self._hooks: Dict[str, Hook] = {}
        self._deliveries: List[Delivery] = []
        self._max_deliveries: int = 1000

        # Config
        self._config_lock = threading.Lock()
        self._config: Optional[Dict[str, Any]] = None
        self._config_path: str = ""

        # Agent executor spawn tracking (task -> lane map)
        self._task_spawns_lock = threading.Lock()
        self._task_spawns: Dict[str, dict] = {}
        # Stateful lanes (lane_key -> hermes session map)
        self._lanes: Dict[str, dict] = {}

        # Server info
        self.started_at: datetime = datetime.utcnow()
        self.cluster_id: str = "cluster_default"
        self.node_id: str = "node_main"
        self.node_role: str = "main"

    # -----------------------------------------------------------------------
    # Node registry
    # -----------------------------------------------------------------------

    def register_node(self, node: Node) -> None:
        with self._nodes_lock:
            # #907: a re-register REPLACES the Node object, so every field the
            # caller did not carry over is silently reset — including the
            # operator's `drained` flag, which would hand work straight back to
            # a quarantined node. Drain is owner state; nothing on a worker's
            # registration path may clear it. (The SQLite store had the same
            # bug via INSERT OR REPLACE; both are fixed together.)
            existing = self._nodes.get(node.id)
            if existing is not None and getattr(existing, "drained", False):
                node.drained = True
            self._nodes[node.id] = node
        if self._on_node_online:
            try:
                self._on_node_online(node.id)
            except Exception:
                pass

    def get_node(self, node_id: str) -> Optional[Node]:
        with self._nodes_lock:
            return self._nodes.get(node_id)

    def get_all_nodes(self) -> List[Node]:
        with self._nodes_lock:
            return list(self._nodes.values())

    def update_heartbeat(self, node_id: str, load: float = 0.0,
                         disk_free_gb: Optional[float] = None,
                         status_reason: str = "",
                         cpu_load_pct: Optional[float] = None,
                         lane_count: Optional[int] = None,
                         duplicate_executor: Optional[bool] = None) -> None:
        with self._nodes_lock:
            if node_id in self._nodes:
                n = self._nodes[node_id]
                n.last_heartbeat = datetime.utcnow()
                # #892: a below-floor heartbeat (status_reason set by the
                # NodeManager) records disk but does NOT force online — that
                # unconditional force is what kept the full-disk node schedulable
                # during the 05:32Z incident. disk_free_gb None → stored value
                # kept (an older worker's beat says nothing about disk).
                # #879: the reason may now ALSO come from the health
                # self-report; the rule is identical — a non-empty reason
                # means degraded, an empty one re-forces online.
                n.status = (NodeStatus.degraded if status_reason
                            else NodeStatus.online)
                n.status_reason = status_reason
                if disk_free_gb is not None:
                    n.disk_free_gb = disk_free_gb
                # #879: record the raw health observations (None = the beat
                # carried nothing → keep the last known value, same rule as
                # disk above).
                if cpu_load_pct is not None:
                    n.cpu_load_pct = cpu_load_pct
                if lane_count is not None:
                    n.lane_count = lane_count
                if duplicate_executor is not None:
                    n.duplicate_executor = bool(duplicate_executor)
                n.load = load

    def update_capabilities(self, node_id: str, caps: List[str]) -> None:
        with self._nodes_lock:
            if node_id in self._nodes:
                old_caps = self._nodes[node_id].capabilities
                self._nodes[node_id].capabilities = caps
                if self._on_capability_change:
                    try:
                        self._on_capability_change(node_id, old_caps, caps)
                    except Exception:
                        pass

    def set_drained(self, node_id: str, drained: bool) -> None:
        """Drain or un-drain a node (#907) — OPERATOR state, never worker state.

        A drained node is handed NOTHING by the scheduler, including a task
        with an empty ``requires``. It is a field of its own rather than an
        empty ``capabilities`` list because ``register_node``'s re-join path
        rewrites capabilities from the worker's local config, so a drain
        expressed that way is erased by the node's next check-in — measured
        2026-09-15, which orphaned the 9 tasks requiring the stripped
        capability.
        """
        with self._nodes_lock:
            if node_id in self._nodes:
                self._nodes[node_id].drained = bool(drained)

    def update_max_concurrent(self, node_id: str, max_concurrent: int) -> None:
        """Update a node's concurrency ceiling (re-join may re-declare it)."""
        with self._nodes_lock:
            if node_id in self._nodes:
                self._nodes[node_id].max_concurrent = max(0, int(max_concurrent))

    def update_instance_token(self, node_id: str, instance_token: str) -> None:
        """#899: record which executor instance currently owns this node id
        (a re-join re-declares it; the /join gate reads it)."""
        with self._nodes_lock:
            if node_id in self._nodes:
                self._nodes[node_id].instance_token = instance_token or ""

    def node_count(self) -> int:
        with self._nodes_lock:
            return len(self._nodes)

    def online_count(self) -> int:
        with self._nodes_lock:
            return sum(1 for n in self._nodes.values() if n.status == NodeStatus.online)

    def set_on_node_online(self, fn: Callable[[str], None]) -> None:
        self._on_node_online = fn

    def set_on_capability_change(self, fn: Callable[[str, List[str], List[str]], None]) -> None:
        self._on_capability_change = fn

    def set_node_status(self, node_id: str, status: NodeStatus,
                        reason: str = "") -> None:
        """Set a node's status (online/degraded/offline) and why (#892)."""
        with self._nodes_lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = status
                self._nodes[node_id].status_reason = reason if status != NodeStatus.online else ""

    def append_timeline(self, event) -> None:
        """Append a timeline event (non-critical, silently ignored)."""
        pass

    # -----------------------------------------------------------------------
    # Task store
    # -----------------------------------------------------------------------

    def create_task(
        self,
        task_id: str,
        title: str,
        requires: List[str],
        priority: int = 3,
        lane_key: str = "",
        role: str = "author",
        description: str = "",
        issues: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> Task:
        now = datetime.utcnow()
        task = Task(
            id=task_id,
            title=title,
            requires=requires,
            depends_on=list(depends_on or []),
            priority=priority,
            # #905: a dep-held task stays pending — trigger_pending_tasks
            # (called by the router right after) is the single promotion gate.
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            version=1,
            lane_key=lane_key,
            role=role,
            description=description,
            issues=list(issues or []),
        )
        with self._tasks_lock:
            self._tasks[task_id] = task
        return task

    def create_tasks_batch(self, plans: List[Dict[str, Any]]) -> List[Task]:
        """Create MANY tasks atomically — one bundle per grouped-intake PHASE-2
        plan, all published or none (PR#43 review finding 3).

        ``plans``: dicts of ``create_task`` kwargs (task_id, title, requires,
        priority, lane_key, role, description, issues). Every Task is built
        BEFORE any state mutation, so an exception raised while assembling a
        later row leaves the store untouched; publishing is one update under
        the lock. Depends-on-free rows are promoted to ready inline, matching
        ``create_task``-followed-by-``trigger_pending_tasks``.
        """
        now = datetime.utcnow()
        tasks: List[Task] = []
        for plan in plans:
            tasks.append(Task(
                id=plan["task_id"],
                title=plan["title"],
                requires=list(plan.get("requires") or []),
                depends_on=list(plan.get("depends_on") or []),
                priority=plan.get("priority", 3),
                status=TaskStatus.ready if not plan.get("depends_on")
                else TaskStatus.pending,
                created_at=now,
                updated_at=now,
                version=1,
                lane_key=plan.get("lane_key", ""),
                role=plan.get("role", "author"),
                description=plan.get("description", ""),
                issues=list(plan.get("issues") or []),
            ))
        with self._tasks_lock:
            for task in tasks:
                self._tasks[task.id] = task
        return tasks

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._tasks_lock:
            return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[Task]:
        with self._tasks_lock:
            return list(self._tasks.values())

    def set_task_result(self, task_id: str, result: Optional[str]) -> bool:
        """Record a lane's deliverable on the task (#874). Returns True if stored.

        Deliberately separate from set_task_status: a result can be recorded
        without touching the state machine, and a blank result is a no-op rather
        than an overwrite. Blank matters -- #870's old success gate was
        `contents.strip()`, where any non-whitespace byte counted as a
        deliverable; the inverse must hold here.
        """
        if result is None or not str(result).strip():
            return False
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            task.result = str(result)
            task.updated_at = datetime.utcnow()
            task.version += 1
            return True

    def set_task_status(self, task_id: str, status: TaskStatus, fail_reason: str = "") -> bool:
        """Set task status. Returns True on success.

        Terminal-state invariant (B1 fix): completed/failed/cancelled/cancel_requested
        are terminal — once a task reaches one of these states, it cannot transition
        back to a non-terminal state (except cancel_requested → cancelled, which is
        the worker-acknowledgement path in the two-phase cancel protocol).
        """
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            # Terminal states (B1): enforce terminality. The only allowed transition
            # FROM a terminal state is cancel_requested → cancelled (worker ack).
            _terminal = {
                TaskStatus.completed,
                TaskStatus.failed,
                TaskStatus.cancelled,
                TaskStatus.cancel_requested,
            }
            if task.status in _terminal:
                # Allow the two-phase cancel ack: cancel_requested → cancelled
                if task.status == TaskStatus.cancel_requested and status == TaskStatus.cancelled:
                    pass  # allow transition
                else:
                    return False  # reject transition to non-terminal
            task.status = status
            task.updated_at = datetime.utcnow()
            task.version += 1
            if fail_reason:
                task.fail_reason = fail_reason
            return True

    def unassign_task(self, task_id: str) -> bool:
        """Atomically clear assigned_to and set status to ready.

        This is the proper way to unassign a task — it avoids the race
        condition of separate set_task_status + direct attribute access.
        Returns True if the task existed and was not terminal (B1).

        Lease guard (#833): a task whose lease is still active is never
        unassigned. A lease is exclusive ownership — recovery revokes the
        lease first, then calls this; the guard makes the invariant hold
        even for callers that would otherwise move a running task out from
        under a live worker.
        """
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            # Terminal-state guard (B1): do not revive a terminal task
            if task.status in TERMINAL_TASK_STATUSES:
                return False
            # Live-lease guard: a worker owns this task — do not make it
            # schedulable again while its lease is alive.
            if self.get_lease_by_task(task_id) is not None:
                return False
            task.assigned_to = None
            task.status = TaskStatus.ready
            task.updated_at = datetime.utcnow()
            task.version += 1
            return True

    def unblock_task(self, task_id: str) -> bool:
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            if task.status == TaskStatus.blocked:
                task.status = TaskStatus.pending
                task.updated_at = datetime.utcnow()
                task.version += 1
                return True
            return False

    def set_task_ballot(self, task_id: str, ballot: Optional[dict]) -> bool:
        """Record a decision ballot on the task (#894). None clears it."""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            task.ballot = ballot
            task.updated_at = datetime.utcnow()
            task.version += 1
            return True

    def set_task_cancel_requeue(self, task_id: str, requeue: bool) -> bool:
        """Record the cancel re-queue INTENT on the task row (#911).

        Called by the /cancel handler BEFORE the status flip so any reader
        that sees `cancelled` also sees the intent that authorized it.
        True = release the members to the next grouped cycle; False = hold
        them back (intentional consolidation cancel).
        """
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            task.cancel_requeue = bool(requeue)
            task.updated_at = datetime.utcnow()
            task.version += 1
            return True

    def unblock_to_ready(self, task_id: str) -> bool:
        """/answer path (#894): a blocked task with an ANSWERED ballot returns
        to ``ready`` (not ``pending`` like legacy unblock) so its lane-affinity
        re-dispatches to the SAME worker holding the lane session."""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if task is None or task.status != TaskStatus.blocked:
                return False
            task.status = TaskStatus.ready
            task.updated_at = datetime.utcnow()
            task.version += 1
            return True

    def requeue_task(self, task_id: str, reason: str = "") -> bool:
        """#870: a delivery whose result body was not a deliverable — the
        task returns to ``ready`` for another attempt instead of being
        consumed as ``failed``. The reporting worker's lease is revoked
        first (its claim is over), assigned_to is cleared, and ``attempts``
        is bumped — main owns the counter so the retry cap survives executor
        restarts and node handoffs. Terminal tasks are never revived (B1
        invariant holds; the caller checks the cap BEFORE asking for this).

        Returns True when the task was actually re-queued.
        """
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            if task.status in TERMINAL_TASK_STATUSES:
                return False
            if task.attempts >= getattr(self, "task_retry_limit", 3):
                return False
            task.attempts += 1
            task.assigned_to = None
            task.status = TaskStatus.ready
            task.updated_at = datetime.utcnow()
            task.version += 1
            if reason:
                task.fail_reason = reason
        # Lease revocation outside the tasks lock (own lock) — same
        # ordering every other lease caller uses.
        lease = self.get_lease_by_task(task_id)
        if lease:
            self.revoke_lease(lease.id)
        return True

    def set_dependencies(self, task_id: str, depends_on: List[str]) -> bool:
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            self._tasks[task_id].depends_on = depends_on
            self._tasks[task_id].updated_at = datetime.utcnow()
            self._tasks[task_id].version += 1
            return True

    def get_dependents(self, task_id: str) -> List[str]:
        """Get all task IDs that depend on the given task."""
        with self._tasks_lock:
            return [
                tid for tid, t in self._tasks.items()
                if task_id in t.depends_on
            ]

    def get_trigger_chain(self, task_id: str, max_depth: int = 10) -> List[str]:
        """Get the chain of tasks triggered by completing the given task."""
        chain: List[str] = []
        visited: set = set()

        def _traverse(tid: str, depth: int):
            if depth >= max_depth or tid in visited:
                return
            visited.add(tid)
            dependents = self.get_dependents(tid)
            for dep_id in dependents:
                chain.append(dep_id)
                _traverse(dep_id, depth + 1)

        _traverse(task_id, 0)
        return chain

    def get_workflow_graph(self) -> Dict[str, Any]:
        """Build a dependency graph from all tasks."""
        with self._tasks_lock:
            nodes = []
            edges = []
            for tid, task in self._tasks.items():
                nodes.append({
                    "id": tid,
                    "title": task.title,
                    "status": task.status.value if isinstance(task.status, TaskStatus) else task.status,
                    "priority": task.priority,
                })
                for dep_id in task.depends_on:
                    edges.append({"from": dep_id, "to": tid})
            return {"nodes": nodes, "edges": edges}

    def task_counts(self) -> Dict[str, int]:
        """Count tasks by status."""
        counts = {
            "total": 0, "ready": 0, "running": 0,
            "completed": 0, "failed": 0, "pending": 0,
            "blocked": 0, "cancel_requested": 0, "cancelled": 0,
        }
        with self._tasks_lock:
            for task in self._tasks.values():
                counts["total"] += 1
                status_val = task.status.value if isinstance(task.status, TaskStatus) else task.status
                if status_val in counts:
                    counts[status_val] += 1
        return counts

    # -----------------------------------------------------------------------
    # Lease manager
    # -----------------------------------------------------------------------

    def create_lease(self, task_id: str, node_id: str, ttl: timedelta) -> Optional[Lease]:
        lease_id = _generate_id("lease")
        now = datetime.utcnow()
        lease = Lease(
            id=lease_id,
            task_id=task_id,
            node_id=node_id,
            created_at=now,
            expires_at=now + ttl,
            status=LeaseStatus.active,
        )
        with self._leases_lock:
            self._leases[lease_id] = lease
            self._task_lease_index[task_id] = lease_id
        return lease

    def revoke_lease(self, lease_id: str) -> bool:
        with self._leases_lock:
            if lease_id not in self._leases:
                return False
            lease = self._leases[lease_id]
            lease.status = LeaseStatus.revoked
            # Remove from task index
            if lease.task_id in self._task_lease_index:
                del self._task_lease_index[lease.task_id]
            return True

    def get_active_leases(self) -> List[Lease]:
        now = datetime.utcnow()
        with self._leases_lock:
            active = []
            for lease in self._leases.values():
                if lease.status == LeaseStatus.active and lease.expires_at > now:
                    active.append(lease)
                elif lease.status == LeaseStatus.active and lease.expires_at <= now:
                    lease.status = LeaseStatus.expired
                    # Trigger expiry callback
                    if self._lease_callback:
                        try:
                            self._lease_callback(lease.task_id, lease.node_id)
                        except Exception:
                            pass
            return active

    def set_lease_callback(self, fn: Callable[[str, str], None]) -> None:
        self._lease_callback = fn

    def get_lease_by_task(self, task_id: str) -> Optional[Lease]:
        """Get the active lease for a given task, if any."""
        if not task_id:
            return None
        with self._leases_lock:
            # Check task index first
            lease_id = self._task_lease_index.get(task_id)
            if lease_id and lease_id in self._leases:
                lease = self._leases[lease_id]
                if lease.status == LeaseStatus.active:
                    return lease
            # Fallback: scan all leases
            for lease in self._leases.values():
                if lease.task_id == task_id and lease.status == LeaseStatus.active:
                    return lease
            return None

    def get_expired_leases(self) -> List[Lease]:
        """Get all expired leases.

        Public API — avoids accessing internal _leases/_leases_lock directly.
        """
        with self._leases_lock:
            return [
                lease for lease in self._leases.values()
                if lease.status == LeaseStatus.expired
            ]

    def _active_leased_task_ids(self) -> set:
        """Task IDs holding a not-yet-expired ACTIVE lease (read-only).

        Deliberately does NOT call ``get_active_leases()``: that method
        marks expired leases and fires the expiry callback (recovery) as a
        side effect, which the scheduler must not trigger.
        """
        now = datetime.utcnow()
        with self._leases_lock:
            return {
                lease.task_id
                for lease in self._leases.values()
                if lease.status is LeaseStatus.active and lease.expires_at > now
            }

    # -----------------------------------------------------------------------
    # Sync state
    # -----------------------------------------------------------------------

    def handle_sync_message(self, msg: SyncMessage) -> bool:
        """Apply a sync message. Returns True if applied.

        N1 fix: existing-task branch routes status writes through set_task_status
        (inherits terminality guard) and gates on per-task version, not only the
        global sync counter. This prevents a stale remote from reviving a terminal task.
        """
        with self._sync_lock:
            if msg.version <= self._sync_version:
                return False
            self._sync_version = msg.version

        if not msg.task_state:
            return True

        task_id = msg.task_state.task_id
        valid_statuses = [s.value for s in TaskStatus]

        with self._tasks_lock:
            if task_id not in self._tasks:
                # Create new task from sync (no terminality concern for new tasks)
                status = TaskStatus(msg.task_state.status) if msg.task_state.status in valid_statuses else TaskStatus.pending
                self._tasks[task_id] = Task(
                    id=task_id,
                    title=msg.task_state.title,
                    status=status,
                    assigned_to=msg.task_state.assigned_to,
                    version=msg.task_state.version,
                )
                return True

            # Existing task: N1 fix — gate on per-task version
            task = self._tasks[task_id]
            if msg.task_state.version <= task.version:
                return True  # stale task data; global counter already advanced

            # Parse the incoming status
            if msg.task_state.status not in valid_statuses:
                return True  # unknown status value, skip

            new_status = TaskStatus(msg.task_state.status)

        # Release _tasks_lock before calling set_task_status (it acquires it internally)
        # N1 fix: route through set_task_status to inherit terminality guard
        accepted = self.set_task_status(task_id, new_status)
        if accepted:
            with self._tasks_lock:
                t = self._tasks[task_id]
                if msg.task_state.assigned_to:
                    t.assigned_to = msg.task_state.assigned_to
                # Ensure version tracks the sync (set_task_status already bumped it)
                if msg.task_state.version > t.version:
                    t.version = msg.task_state.version
        return True

    def handle_batch_sync(self, batch: BatchSyncMessage) -> int:
        """Apply a batch of sync messages. Returns count applied."""
        count = 0
        for msg in batch.messages:
            if self.handle_sync_message(msg):
                count += 1
        return count

    def sync_version(self) -> int:
        with self._sync_lock:
            return self._sync_version

    # -----------------------------------------------------------------------
    # Recovery log
    # -----------------------------------------------------------------------

    def append_recovery_event(self, event: RecoveryEvent) -> None:
        with self._recovery_lock:
            if not event.id:
                event.id = _generate_id("recovery")
            event.timestamp = datetime.utcnow()
            self._recovery_events.append(event)

    def get_recovery_events(self) -> List[RecoveryEvent]:
        with self._recovery_lock:
            return list(self._recovery_events)

    def recovery_stats(self) -> Dict[str, Any]:
        with self._recovery_lock:
            total = len(self._recovery_events)
            by_action: Dict[str, int] = {}
            for e in self._recovery_events:
                by_action[e.action] = by_action.get(e.action, 0) + 1
            return {"total": total, "by_action": by_action}

    def trigger_recovery(self, node_id: str) -> None:
        """Notify that a node went offline — trigger recovery."""
        event = RecoveryEvent(
            id=_generate_id("recovery"),
            node_id=node_id,
            action="reschedule",
            status="completed",
            message=f"Node {node_id} went offline, rescheduling tasks",
        )
        self.append_recovery_event(event)

    # -----------------------------------------------------------------------
    # Scheduling decisions
    # -----------------------------------------------------------------------

    def record_decision(self, decision: SchedulingDecision) -> None:
        with self._schedule_lock:
            self._decisions.append(decision)
            if len(self._decisions) > self._max_decisions:
                self._decisions = self._decisions[-self._max_decisions:]

    def get_decisions(self) -> List[SchedulingDecision]:
        with self._schedule_lock:
            return list(self._decisions)

    def get_schedule_stats(self) -> SchedulingStats:
        with self._schedule_lock:
            total = len(self._decisions)
            by_priority: Dict[int, int] = {}
            for d in self._decisions:
                by_priority[d.priority] = by_priority.get(d.priority, 0) + 1
            return SchedulingStats(
                total_decisions=total,
                decisions_by_priority=by_priority,
                last_decisions=self._decisions[-10:] if self._decisions else [],
            )

    def trigger_pending_tasks(self) -> int:
        """Promote pending tasks with all dependencies met to ready. Returns count promoted.

        #913: a LANDING task (lane '<repo>#land-<n>' or 'LAND'-titled) is
        promoted ONLY if the board's latest reviewer verdict on its artifact
        is an un-superseded PASS pinning a sha the landing names — a
        rejected or verdict-less review never auto-spawns a merge. The hold
        records WHY in fail_reason; operators keep the ungated /advance.
        """
        promoted = 0
        with self._tasks_lock:
            snapshot = list(self._tasks.values())
            for task in self._tasks.values():
                if task.status != TaskStatus.pending:
                    continue
                if not task.depends_on:
                    ready = True
                else:
                    ready = all(
                        self._tasks.get(dep_id) is not None
                        and self._tasks[dep_id].status == TaskStatus.completed
                        for dep_id in task.depends_on
                    )
                if not ready:
                    continue
                reason = landing_hold_reason(snapshot, task)
                if reason is not None:
                    if task.fail_reason != reason:
                        task.fail_reason = reason
                        task.updated_at = datetime.utcnow()
                    continue
                task.status = TaskStatus.ready
                task.updated_at = datetime.utcnow()
                promoted += 1
        return promoted

    def schedule_pending(self) -> int:
        """Assign ready tasks to capable online nodes. Returns count scheduled.

        Fairness fix (#833): routes through the shared planner so the node
        with the fewest active tasks wins and ties rotate round-robin —
        previously the first capability-matching node took everything.
        """
        return len(self.schedule_pending_detailed())

    def schedule_pending_detailed(self) -> List[Dict[str, Any]]:
        """Assign ready tasks to the least-loaded capable online node.

        Returns the NEW assignments made by this call (one dict per task:
        ``task_id``/``task_title``/``node_id``/``priority``). The trigger
        endpoint uses this so ``POST /schedule/trigger`` reports only the
        work it actually scheduled — not every running task in the cluster.

        Rules (see ``hermes_cluster/core/scheduler.py``):
          - only ONLINE nodes
          - fewest active tasks, ties -> round-robin (least-recently picked)
          - never exceed a node's ``max_concurrent`` (0 = unlimited)
          - never (re-)assign a task whose lease is still active
        """
        new_assignments: List[Dict[str, Any]] = []

        with self._nodes_lock:
            online_nodes = [
                n for n in self._nodes.values() if n.status == NodeStatus.online
            ]
        if not online_nodes:
            return new_assignments

        # Tasks that still hold an active lease must not be re-assigned.
        leased_task_ids = self._active_leased_task_ids()

        # Lane-to-node affinity (#858 defect 2): snapshot lane placements
        # (lane_key -> owning node) up front; lanes live under their own
        # lock, so read them BEFORE acquiring _tasks_lock (lock order:
        # _task_spawns_lock alone, then _tasks_lock — never nested).
        with self._task_spawns_lock:
            lane_nodes = {
                key: (rec.get("node") or "")
                for key, rec in self._lanes.items()
            }

        with self._tasks_lock:
            # LFP-1 cardinality guard (#762): ready tasks whose lane already
            # has an active/earlier sibling are held back for a later tick.
            lane_blocked = lane_blocked_ready_ids(self._tasks.values())
            # Per-node active load from tasks currently assigned to each node.
            active_counts: Dict[str, int] = {}
            for t in self._tasks.values():
                if t.status in ACTIVE_TASK_STATUSES and t.assigned_to:
                    active_counts[t.assigned_to] = (
                        active_counts.get(t.assigned_to, 0) + 1
                    )

            # Sort ready tasks by priority (1=highest first), then creation time.
            ready_tasks = sorted(
                [
                    t for t in self._tasks.values()
                    if t.status == TaskStatus.ready and t.id not in leased_task_ids
                    and t.id not in lane_blocked
                ],
                key=lambda t: (t.priority, t.created_at),
            )

            for task in ready_tasks:
                node, pinned = self._fair_scheduler.choose_pinned(
                    task.requires, online_nodes, active_counts,
                    pinned_node_id=lane_nodes.get(task.lane_key, "")
                    if task.lane_key else "",
                )
                if node is None:
                    # Pinned lane whose node is offline/degraded/at capacity
                    # — PARK (stays ready; never re-home a lane, #858) — or
                    # no candidate has spare capacity for an unpinned task:
                    # leave the task ready for a later trigger.
                    continue

                task.status = TaskStatus.running
                task.assigned_to = node.id
                task.updated_at = datetime.utcnow()
                task.version += 1
                active_counts[node.id] = active_counts.get(node.id, 0) + 1
                self._fair_scheduler.mark_picked(node.id)

                # Record decision
                decision = SchedulingDecision(
                    task_id=task.id,
                    task_title=task.title,
                    priority=task.priority,
                    node_id=node.id,
                    score=1.0,
                    reason=("lane_affinity_pinned" if pinned
                            else "least_loaded_capability_match"),
                )
                self.record_decision(decision)

                new_assignments.append({
                    "task_id": task.id,
                    "task_title": task.title,
                    "node_id": node.id,
                    "priority": task.priority,
                })

        return new_assignments

    # -----------------------------------------------------------------------
    # Federation registry
    # -----------------------------------------------------------------------

    def register_federation_cluster(self, cluster_id: str, name: str, endpoint: str) -> RemoteCluster:
        now = datetime.utcnow()
        cluster = RemoteCluster(
            id=cluster_id,
            name=name,
            endpoint=endpoint,
            status=FederationClusterStatus.available,
            registered_at=now,
            last_ping=now,
        )
        with self._federation_lock:
            self._clusters[cluster_id] = cluster
        return cluster

    def remove_federation_cluster(self, cluster_id: str) -> bool:
        with self._federation_lock:
            if cluster_id in self._clusters:
                del self._clusters[cluster_id]
                return True
            return False

    def get_federation_clusters(self) -> List[RemoteCluster]:
        with self._federation_lock:
            return list(self._clusters.values())

    def get_federation_cluster(self, cluster_id: str) -> Optional[RemoteCluster]:
        with self._federation_lock:
            return self._clusters.get(cluster_id)

    # -----------------------------------------------------------------------
    # Hook manager
    # -----------------------------------------------------------------------

    def register_hook(self, url: str, events: List[EventType], secret: str = "") -> Hook:
        hook_id = _generate_id("hook")
        now = datetime.utcnow()
        hook = Hook(
            id=hook_id,
            url=url,
            events=events,
            secret=secret or None,
            active=True,
            created_at=now,
            updated_at=now,
        )
        with self._hooks_lock:
            self._hooks[hook_id] = hook
        return hook

    def deregister_hook(self, hook_id: str) -> bool:
        with self._hooks_lock:
            if hook_id in self._hooks:
                del self._hooks[hook_id]
                return True
            return False

    def list_hooks(self) -> List[Hook]:
        with self._hooks_lock:
            # Return hooks without secrets
            return [h.model_copy(update={"secret": None}) for h in self._hooks.values()]

    def get_hook_deliveries(self, hook_id: str) -> List[Delivery]:
        with self._hooks_lock:
            return [d for d in self._deliveries if d.hook_id == hook_id]

    def add_delivery(self, delivery: Delivery) -> None:
        with self._hooks_lock:
            self._deliveries.append(delivery)
            if len(self._deliveries) > self._max_deliveries:
                self._deliveries = self._deliveries[-self._max_deliveries:]

    # -----------------------------------------------------------------------
    # Agent executor spawn tracking (task -> lane map)
    # -----------------------------------------------------------------------

    def record_task_spawn(
        self,
        task_id: str,
        mode: str = "bdaya-dispatch",
        job_id: str = "",
        pid: int = 0,
        started_at: float = 0.0,
        lease_id: str = "",
        lane_name: str = "",
        result_path: str = "",
        lane_key: str = "",
        role: str = "author",
        session_id: str = "",
        attempt: int = 0,
    ) -> None:
        """Persist a task spawn record (in-memory mirror of ClusterStore)."""
        with self._task_spawns_lock:
            self._task_spawns[task_id] = {
                "task_id": task_id,
                "mode": mode,
                "job_id": job_id,
                "pid": pid,
                "started_at": started_at,
                "lease_id": lease_id,
                "lane_name": lane_name,
                "result_path": result_path,
                "lane_key": lane_key,
                "role": role,
                "session_id": session_id,
                "attempt": attempt,
            }

    def get_task_spawn(self, task_id: str) -> Optional[dict]:
        with self._task_spawns_lock:
            record = self._task_spawns.get(task_id)
            return dict(record) if record else None

    def get_all_task_spawns(self) -> List[dict]:
        with self._task_spawns_lock:
            return [dict(r) for r in self._task_spawns.values()]

    def delete_task_spawn(self, task_id: str) -> bool:
        with self._task_spawns_lock:
            return self._task_spawns.pop(task_id, None) is not None

    # -----------------------------------------------------------------------
    # Stateful lanes (lane_key -> hermes session map)
    # -----------------------------------------------------------------------

    def record_lane(self, lane_key: str, session_id: str = "", profile: str = "",
                    role: str = "author", node: str = "", created_at: Optional[float] = None,
                    last_active_at: Optional[float] = None,
                    last_task_id: str = "") -> None:
        """Upsert a lane record by lane_key (first created_at always wins).

        ``last_active_at`` is refreshed on every upsert (default: now) so the
        executor's idle reaper can measure the lane's idle time.
        """
        with self._task_spawns_lock:
            existing = self._lanes.get(lane_key)
            effective_created = (
                existing.get("created_at") if existing
                else (created_at if created_at is not None else time.time()))
            effective_active = last_active_at if last_active_at is not None else time.time()
            self._lanes[lane_key] = {
                "lane_key": lane_key,
                "session_id": session_id,
                "profile": profile,
                "role": role,
                "node": node,
                "created_at": effective_created,
                "last_active_at": effective_active,
                "last_task_id": last_task_id,
            }

    def get_lane(self, lane_key: str) -> Optional[dict]:
        with self._task_spawns_lock:
            record = self._lanes.get(lane_key)
            return dict(record) if record else None

    def get_all_lanes(self) -> List[dict]:
        with self._task_spawns_lock:
            return [dict(r) for r in self._lanes.values()]

    def touch_lane_last_task(self, lane_key: str, task_id: str) -> None:
        if not lane_key:
            return
        with self._task_spawns_lock:
            lane = self._lanes.get(lane_key)
            if lane:
                lane["last_task_id"] = task_id

    def delete_lane(self, lane_key: str) -> bool:
        with self._task_spawns_lock:
            return self._lanes.pop(lane_key, None) is not None

    # -----------------------------------------------------------------------
    # Config
    # -----------------------------------------------------------------------

    def get_config(self) -> Optional[Dict[str, Any]]:
        with self._config_lock:
            return self._config

    def set_config(self, config: Dict[str, Any]) -> None:
        with self._config_lock:
            self._config = config

    def get_config_path(self) -> str:
        return self._config_path

    def set_config_path(self, path: str) -> None:
        self._config_path = path

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    def get_summary(self) -> Dict[str, Any]:
        task_counts = self.task_counts()
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "role": self.node_role,
            "nodes": {"total": self.node_count(), "online": self.online_count()},
            "tasks": task_counts,
            "leases": {"active": len(self.get_active_leases())},
            "sync_version": self.sync_version(),
            "uptime_seconds": int((datetime.utcnow() - self.started_at).total_seconds()),
        }
