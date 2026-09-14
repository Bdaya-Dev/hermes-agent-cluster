"""Persistent SQLite storage layer — drop-in replacement for in-memory ClusterState.

Uses WAL mode for concurrent reads. Thread-safe via sqlite3's serialized
access and explicit threading.Lock for compound operations.

Schema mirrors the in-memory ClusterState but persists to a single .db file.
"""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..models import (
    BatchSyncMessage,
    Delivery,
    EventType,
    FederationClusterStatus,
    Hook,
    Lease,
    LeaseStatus,
    Node,
    NodeStatus,
    RecoveryEvent,
    RemoteCluster,
    SchedulingDecision,
    SchedulingStats,
    SyncEventType,
    SyncMessage,
    Task,
    TaskStatus,
)
from ..core.scheduler import (
    ACTIVE_TASK_STATUSES,
    TERMINAL_TASK_STATUSES,
    FairScheduler,
    lane_blocked_ready_ids,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_id(prefix: str = "") -> str:
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


def _dt_to_str(dt: datetime) -> str:
    """ISO format string for SQLite storage."""
    if dt is None:
        return ""
    return dt.isoformat()


def _str_to_dt(s: str) -> datetime:
    """Parse ISO string back to datetime."""
    if not s:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return datetime.utcnow()


def _json_dumps(obj: Any) -> str:
    """Serialize to JSON string for SQLite TEXT columns."""
    if obj is None:
        return "[]"
    return json.dumps(obj, default=str)


def _json_loads(s: str) -> Any:
    """Deserialize JSON string."""
    if not s:
        return []
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    capabilities TEXT DEFAULT '[]',
    status TEXT DEFAULT 'online',
    last_heartbeat TEXT,
    load REAL DEFAULT 0.0,
    max_concurrent INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    requires TEXT DEFAULT '[]',
    depends_on TEXT DEFAULT '[]',
    priority INTEGER DEFAULT 3,
    status TEXT DEFAULT 'pending',
    assigned_to TEXT,
    created_at TEXT,
    updated_at TEXT,
    version INTEGER DEFAULT 0,
    fail_reason TEXT,
    attempts INTEGER DEFAULT 0,
    result TEXT,
    -- #762 grouped intake: bundle brief + restart-safe membership.
    description TEXT DEFAULT '',
    issues TEXT DEFAULT '[]'
);

-- Stateful lanes: one row per lane_key, keyed to the hermes session it owns.
-- A task whose lane_key has a live row RESUMES that hermes session instead of
-- spawning a fresh one (shared/claude-plugins#847/#833; PR#16 stateful-lanes).
-- last_active_at (epoch seconds) records the lane's most recent activity
-- (delivery spawn or completion); the executor reaps a lane idle past
-- agent_executor.lane_idle_timeout by clearing this row (PR#16 round 1).
CREATE TABLE IF NOT EXISTS lanes (
    lane_key TEXT PRIMARY KEY,
    session_id TEXT DEFAULT '',
    profile TEXT DEFAULT '',
    role TEXT DEFAULT 'author',
    node TEXT DEFAULT '',
    created_at REAL NOT NULL,
    last_active_at REAL DEFAULT 0,
    last_task_id TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    created_at TEXT,
    expires_at TEXT,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    sender_node TEXT DEFAULT '',
    task_state TEXT,
    event_type TEXT DEFAULT 'task_created',
    timestamp INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id TEXT PRIMARY KEY,
    task_id TEXT DEFAULT '',
    node_id TEXT DEFAULT '',
    action TEXT DEFAULT '',
    status TEXT DEFAULT '',
    message TEXT,
    timestamp TEXT
);

CREATE TABLE IF NOT EXISTS scheduling_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    task_title TEXT NOT NULL,
    priority INTEGER DEFAULT 3,
    node_id TEXT NOT NULL,
    score REAL DEFAULT 0.0,
    reason TEXT DEFAULT '',
    timestamp TEXT
);

CREATE TABLE IF NOT EXISTS federation_clusters (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT DEFAULT 'available',
    registered_at TEXT,
    last_ping TEXT,
    ping_latency REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    events TEXT DEFAULT '[]',
    secret TEXT,
    active INTEGER DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    event_type TEXT DEFAULT '',
    payload TEXT DEFAULT '{}',
    status TEXT DEFAULT 'delivered',
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Agent executor spawn tracking (task -> lane map, persisted across restarts).
-- Reconciles on executor start so a mid-task restart does not re-spawn a lane
-- already tracking the task (#804 note 132791).
CREATE TABLE IF NOT EXISTS task_spawns (
    task_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'bdaya-dispatch',
    job_id TEXT DEFAULT '',
    pid INTEGER DEFAULT 0,
    started_at REAL NOT NULL,
    lease_id TEXT DEFAULT '',
    lane_name TEXT DEFAULT '',
    result_path TEXT DEFAULT '',
    lane_key TEXT DEFAULT '',
    role TEXT DEFAULT 'author',
    session_id TEXT DEFAULT '',
    attempt INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_leases_task ON leases(task_id);
CREATE INDEX IF NOT EXISTS idx_leases_status ON leases(status);
CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_hook ON deliveries(hook_id);
CREATE INDEX IF NOT EXISTS idx_recovery_node ON recovery_events(node_id);
CREATE INDEX IF NOT EXISTS idx_task_spawns_started ON task_spawns(started_at);
"""


# ---------------------------------------------------------------------------
# ClusterStore
# ---------------------------------------------------------------------------

class ClusterStore:
    """Persistent SQLite-backed store for cluster state.

    Drop-in replacement for ClusterState with the same public API.
    All writes go through a lock; reads use the WAL-mode connection
    which allows concurrent readers.
    """

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the store.

        Args:
            db_path: Path to SQLite database file. Use ":memory:" for
                     in-memory (useful for tests).
        """
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

        # Callbacks (same as ClusterState)
        self._on_node_online: Optional[Callable[[str], None]] = None
        self._on_capability_change: Optional[Callable[[str, List[str], List[str]], None]] = None
        self._lease_callback: Optional[Callable[[str, str], None]] = None

        # Config cache (loaded from kv_store)
        self._config: Optional[Dict[str, Any]] = None
        self._config_path: str = ""

        # Server info
        self.started_at: datetime = datetime.utcnow()
        self.cluster_id: str = "cluster_default"
        self.node_id: str = "node_main"
        self.node_role: str = "main"

        # Max limits (matching ClusterState)
        self._max_decisions: int = 200
        self._max_deliveries: int = 1000

        # Fair scheduler (least-loaded node, round-robin ties, capacity-aware)
        self._fair_scheduler = FairScheduler()

        self._init_db()

    def _init_db(self) -> None:
        """Initialize database connection and schema."""
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        # Enable WAL for concurrent reads
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA_SQL)
        self._migrate_schema()
        self._conn.commit()

    def _migrate_schema(self) -> None:
        """Best-effort ALTER TABLE migrations for schema drift on old DBs.

        ADDITIVE BOTH-BODY MERGE (PR#16 + PR#17, round-1): the PR#16 side
        predates the stateful-lane columns (lane_key/role on tasks) and the
        ``lanes`` table; the PR#17 side predates ``nodes.max_concurrent``
        (scheduler fairness, #833). Both migration bodies are kept: every
        migration is idempotent — the column/table existence check makes
        re-runs no-ops, and failures are logged rather than failing the whole
        store open.
        """
        # PR#16 (stateful lanes): lane_key/role columns + lanes table.
        for table, column, ddl in (
            ("tasks", "lane_key", "ALTER TABLE tasks ADD COLUMN lane_key TEXT DEFAULT ''"),
            ("tasks", "role", "ALTER TABLE tasks ADD COLUMN role TEXT DEFAULT 'author'"),
            ("tasks", "result", "ALTER TABLE tasks ADD COLUMN result TEXT"),
            ("task_spawns", "lane_key", "ALTER TABLE task_spawns ADD COLUMN lane_key TEXT DEFAULT ''"),
            ("task_spawns", "role", "ALTER TABLE task_spawns ADD COLUMN role TEXT DEFAULT 'author'"),
            ("task_spawns", "session_id", "ALTER TABLE task_spawns ADD COLUMN session_id TEXT DEFAULT ''"),
            # #870: main-side retry cap — re-queue counter, and the delivery
            # count carried on the spawn record.
            ("tasks", "attempts", "ALTER TABLE tasks ADD COLUMN attempts INTEGER DEFAULT 0"),
            ("task_spawns", "attempt", "ALTER TABLE task_spawns ADD COLUMN attempt INTEGER DEFAULT 0"),
            # #762 grouped intake: bundle brief + membership columns.
            ("tasks", "description", "ALTER TABLE tasks ADD COLUMN description TEXT DEFAULT ''"),
            ("tasks", "issues", "ALTER TABLE tasks ADD COLUMN issues TEXT DEFAULT '[]'"),
        ):
            try:
                cols = [r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")]
                if column not in cols:
                    self._conn.execute(ddl)
            except Exception as e:
                logger.warning("schema migration skipped (%s.%s): %s", table, column, e)
        # PR#17 (scheduler fairness): nodes.max_concurrent (#833).
        try:
            node_cols = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(nodes)").fetchall()
            }
            if "max_concurrent" not in node_cols:
                self._conn.execute(
                    "ALTER TABLE nodes ADD COLUMN max_concurrent INTEGER DEFAULT 0"
                )
        except Exception as e:
            logger.warning("nodes.max_concurrent migration skipped: %s", e)
        # PR#16 (stateful lanes): lanes table + idle-reap activity clock.
        try:
            rows = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='lanes'"
            ).fetchall()
            if not rows:
                self._conn.execute(
                    """CREATE TABLE IF NOT EXISTS lanes (
                        lane_key TEXT PRIMARY KEY,
                        session_id TEXT DEFAULT '',
                        profile TEXT DEFAULT '',
                        role TEXT DEFAULT 'author',
                        node TEXT DEFAULT '',
                        created_at REAL NOT NULL,
                        last_active_at REAL DEFAULT 0,
                        last_task_id TEXT DEFAULT ''
                    )"""
                )
            else:
                # Idle-lane reaping needs a last-activity clock (round 1).
                cols = [r[1] for r in self._conn.execute(
                    "PRAGMA table_info(lanes)"
                ).fetchall()]
                if "last_active_at" not in cols:
                    self._conn.execute(
                        "ALTER TABLE lanes ADD COLUMN last_active_at REAL DEFAULT 0"
                    )
        except Exception as e:
            logger.warning("lanes table migration skipped: %s", e)

    @contextmanager
    def _tx(self):
        """Transaction context manager — wraps operations in BEGIN/COMMIT."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    # -------------------------------------------------------------------
    # Node registry
    # -------------------------------------------------------------------

    def register_node(self, node: Node) -> None:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO nodes
                   (id, name, capabilities, status, last_heartbeat, load, max_concurrent)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (node.id, node.name, _json_dumps(node.capabilities),
                 node.status.value, _dt_to_str(node.last_heartbeat), node.load,
                 node.max_concurrent),
            )
        if self._on_node_online:
            try:
                self._on_node_online(node.id)
            except Exception:
                pass

    def get_node(self, node_id: str) -> Optional[Node]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_node(row)

    def get_all_nodes(self) -> List[Node]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM nodes").fetchall()
        return [self._row_to_node(r) for r in rows]

    def update_heartbeat(self, node_id: str, load: float = 0.0) -> None:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET last_heartbeat = ?, status = ?, load = ? WHERE id = ?",
                (_dt_to_str(now), NodeStatus.online.value, load, node_id),
            )

    def update_capabilities(self, node_id: str, caps: List[str]) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT capabilities FROM nodes WHERE id = ?", (node_id,)
            ).fetchone()
            old_caps = _json_loads(row["capabilities"]) if row else []

        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET capabilities = ? WHERE id = ?",
                (_json_dumps(caps), node_id),
            )

        if self._on_capability_change:
            try:
                self._on_capability_change(node_id, old_caps, caps)
            except Exception:
                pass

    def update_max_concurrent(self, node_id: str, max_concurrent: int) -> None:
        """Update a node's concurrency ceiling (re-join may re-declare it)."""
        value = max(0, int(max_concurrent))
        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET max_concurrent = ? WHERE id = ?",
                (value, node_id),
            )

    def node_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) as c FROM nodes").fetchone()
            return row["c"]

    def online_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) as c FROM nodes WHERE status = ?",
                (NodeStatus.online.value,),
            ).fetchone()
            return row["c"]

    def set_on_node_online(self, fn: Callable[[str], None]) -> None:
        self._on_node_online = fn

    def set_on_capability_change(self, fn: Callable[[str, List[str], List[str]], None]) -> None:
        self._on_capability_change = fn

    def set_node_status(self, node_id: str, status: NodeStatus) -> None:
        """Set a node's status (online/degraded/offline)."""
        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET status = ? WHERE id = ?",
                (status.value, node_id),
            )

    def append_timeline(self, event) -> None:
        """Append a timeline event (non-critical, silently ignored)."""
        pass

    def _row_to_node(self, row: sqlite3.Row) -> Node:
        return Node(
            id=row["id"],
            name=row["name"],
            capabilities=_json_loads(row["capabilities"]),
            status=NodeStatus(row["status"]),
            last_heartbeat=_str_to_dt(row["last_heartbeat"]),
            load=row["load"],
            max_concurrent=row["max_concurrent"] if "max_concurrent" in row.keys() else 0,
        )

    # -------------------------------------------------------------------
    # Task store
    # -------------------------------------------------------------------

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
    ) -> Task:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO tasks
                   (id, title, requires, depends_on, priority, status, created_at, updated_at, version, lane_key, role, description, issues)
                   VALUES (?, ?, ?, '[]', ?, ?, ?, ?, 1, ?, ?, ?, ?)""",
                (task_id, title, _json_dumps(requires), priority,
                 TaskStatus.pending.value, _dt_to_str(now), _dt_to_str(now),
                 lane_key, role, description, _json_dumps(list(issues or []))),
            )
        # Promote to ready immediately if no dependencies (matching ClusterState behavior)
        # The original in-memory store returns by reference so the caller
        # sees the promotion after trigger_pending_tasks(). SQLite returns
        # a copy, so we must promote here for API compatibility.
        # create_task always creates with empty depends_on, so promote immediately
        with self._tx() as conn:
            conn.execute(
                """UPDATE tasks SET status = ?, updated_at = ?
                   WHERE id = ? AND status = ?""",
                (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                 task_id, TaskStatus.pending.value),
            )
        return self.get_task(task_id) or Task(
            id=task_id, title=title, requires=requires, priority=priority,
            status=TaskStatus.pending, created_at=now, updated_at=now, version=1,
            lane_key=lane_key, role=role,
            description=description, issues=list(issues or []),
        )

    def create_tasks_batch(self, plans: List[Dict[str, Any]]) -> List[Task]:
        """Create MANY tasks atomically — one bundle per grouped-intake
        PHASE-2 plan, all published or none (PR#43 review finding 3).

        ``ClusterStore.create_task`` opens one transaction PER task, so a
        crash between two PHASE-2 writes stranded a partial batch on disk
        (the hosted main runs this class). Here every insert+promote runs
        inside ONE ``_tx``: the context manager's except-branch rolls back
        the whole batch on any exception, and the single commit publishes
        all rows together. Same per-row semantics as ``create_task``
        (INSERT OR IGNORE + status-guarded pending->ready promotion); the
        kick (trigger/schedule) stays in the caller so the batch is pure
        persistence.
        """
        now = datetime.utcnow()
        now_s = _dt_to_str(now)
        with self._tx() as conn:
            for plan in plans:
                conn.execute(
                    """INSERT OR IGNORE INTO tasks
                       (id, title, requires, depends_on, priority, status, created_at, updated_at, version, lane_key, role, description, issues)
                       VALUES (?, ?, ?, '[]', ?, ?, ?, ?, 1, ?, ?, ?, ?)""",
                    (plan["task_id"], plan["title"],
                     _json_dumps(list(plan.get("requires") or [])),
                     plan.get("priority", 3),
                     TaskStatus.pending.value, now_s, now_s,
                     plan.get("lane_key", ""), plan.get("role", "author"),
                     plan.get("description", ""),
                     _json_dumps(list(plan.get("issues") or []))),
                )
            # Promote ONLY this batch's rows (no dependencies at creation,
            # mirroring create_task's per-id guarded UPDATE — a global
            # status-filter UPDATE would wrongly promote dependency-held
            # pending tasks created by other callers).
            ids = [plan["task_id"] for plan in plans]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"""UPDATE tasks SET status = ?, updated_at = ?
                    WHERE id IN ({placeholders}) AND status = ?""",
                [TaskStatus.ready.value, now_s] + ids
                + [TaskStatus.pending.value],
            )
        out: List[Task] = []
        for plan in plans:
            task = self.get_task(plan["task_id"])
            if task is None:  # INSERT OR IGNORE hit an existing id
                continue
            out.append(task)
        return out

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)

    def get_all_tasks(self) -> List[Task]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM tasks").fetchall()
        return [self._row_to_task(r) for r in rows]

    def set_task_result(self, task_id: str, result: Optional[str]) -> bool:
        """Persist a lane's deliverable on the task row (#874).

        Separate from set_task_status so a result can be recorded without
        touching the state machine, and so an empty/None result is a no-op
        rather than an overwrite of something already stored.
        """
        if result is None or not str(result).strip():
            return False
        now = datetime.utcnow()
        with self._tx() as conn:
            cur = conn.execute(
                """UPDATE tasks SET result = ?, updated_at = ?, version = version + 1
                   WHERE id = ?""",
                (str(result), _dt_to_str(now), task_id),
            )
        return cur.rowcount > 0

    def set_task_status(
        self, task_id: str, status: TaskStatus, fail_reason: str = ""
    ) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            if fail_reason:
                result = conn.execute(
                    """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1,
                       fail_reason = ? WHERE id = ?""",
                    (status.value, _dt_to_str(now), fail_reason, task_id),
                )
            else:
                result = conn.execute(
                    """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1
                       WHERE id = ?""",
                    (status.value, _dt_to_str(now), task_id),
                )
            return result.rowcount > 0

    def unassign_task(self, task_id: str) -> bool:
        """Atomically clear assigned_to and set status to ready.

        Guarded to match ``ClusterState.unassign_task``:
          - a terminal task is never revived;
          - a task whose lease is still active is never unassigned — the
            lease is exclusive ownership and only recovery/expiry may move
            the task again (#833).
        """
        now = datetime.utcnow()
        terminal_ids = [s.value for s in TERMINAL_TASK_STATUSES]
        placeholders = ",".join("?" for _ in terminal_ids)
        with self._tx() as conn:
            result = conn.execute(
                f"""UPDATE tasks SET assigned_to = NULL, status = ?,
                    updated_at = ?, version = version + 1
                    WHERE id = ?
                      AND status NOT IN ({placeholders})
                      AND NOT EXISTS (
                          SELECT 1 FROM leases
                          WHERE leases.task_id = tasks.id
                            AND leases.status = ?
                            AND leases.expires_at > ?
                      )""",
                (TaskStatus.ready.value, _dt_to_str(now), task_id,
                 *terminal_ids, LeaseStatus.active.value, _dt_to_str(now)),
            )
            return result.rowcount > 0

    def unblock_task(self, task_id: str) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            result = conn.execute(
                """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1
                   WHERE id = ? AND status = ?""",
                (TaskStatus.pending.value, _dt_to_str(now), task_id, TaskStatus.blocked.value),
            )
            return result.rowcount > 0

    def requeue_task(self, task_id: str, reason: str = "") -> bool:
        """#870: return a task to 'ready' for another delivery attempt after
        a worker reported a NON-DELIVERABLE result body. Atomic guarded
        UPDATE mirrors unassign_task (terminal tasks never revive) and adds
        the cap check + attempts bump in the same statement. The task's
        active leases are revoked first — the delivery is over and the
        live-lease guard would otherwise refuse the un-assign.

        Returns True when the task was actually re-queued.
        """
        now = datetime.utcnow()
        limit = int(getattr(self, "task_retry_limit", 3))
        terminal_ids = [s.value for s in TERMINAL_TASK_STATUSES]
        placeholders = ",".join("?" for _ in terminal_ids)
        # revoke live leases for this task before the guarded update (same
        # status/expiry predicate as get_active_leases' marking).
        with self._tx() as conn:
            conn.execute(
                f"""UPDATE leases SET status = ?
                    WHERE task_id = ? AND status = ? AND expires_at > ?""",
                (LeaseStatus.revoked.value, task_id,
                 LeaseStatus.active.value, _dt_to_str(now)),
            )
            result = conn.execute(
                f"""UPDATE tasks
                    SET status = ?, assigned_to = NULL, updated_at = ?,
                        version = version + 1, attempts = attempts + 1,
                        fail_reason = COALESCE(?, fail_reason)
                    WHERE id = ?
                      AND status NOT IN ({placeholders})
                      AND COALESCE(attempts, 0) < ?""",
                (TaskStatus.ready.value, _dt_to_str(now),
                 reason or None, task_id, *terminal_ids, limit),
            )
            return result.rowcount > 0

    def set_dependencies(self, task_id: str, depends_on: List[str]) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            result = conn.execute(
                """UPDATE tasks SET depends_on = ?, updated_at = ?, version = version + 1
                   WHERE id = ?""",
                (_json_dumps(depends_on), _dt_to_str(now), task_id),
            )
            # If deps added and task was ready, demote to pending until deps are met
            if depends_on and result.rowcount > 0:
                conn.execute(
                    """UPDATE tasks SET status = ? WHERE id = ? AND status = ?""",
                    (TaskStatus.pending.value, task_id, TaskStatus.ready.value),
                )
            return result.rowcount > 0

    def get_dependents(self, task_id: str) -> List[str]:
        """Get all task IDs that depend on the given task."""
        with self._lock:
            rows = self._conn.execute("SELECT id, depends_on FROM tasks").fetchall()
        return [
            row["id"]
            for row in rows
            if task_id in _json_loads(row["depends_on"])
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
        with self._lock:
            rows = self._conn.execute("SELECT * FROM tasks").fetchall()
        nodes = []
        edges = []
        for row in rows:
            nodes.append({
                "id": row["id"],
                "title": row["title"],
                "status": row["status"],
                "priority": row["priority"],
            })
            for dep_id in _json_loads(row["depends_on"]):
                edges.append({"from": dep_id, "to": row["id"]})
        return {"nodes": nodes, "edges": edges}

    def task_counts(self) -> Dict[str, int]:
        """Count tasks by status."""
        counts = {
            "total": 0, "ready": 0, "running": 0,
            "completed": 0, "failed": 0, "pending": 0,
        }
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) as c FROM tasks GROUP BY status"
            ).fetchall()
        for row in rows:
            counts["total"] += row["c"]
            status_val = row["status"]
            if status_val in counts:
                counts[status_val] += row["c"]
        return counts

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        try:
            lane_key = row["lane_key"]
        except (KeyError, IndexError):
            lane_key = ""
        try:
            role = row["role"]
        except (KeyError, IndexError):
            role = "author"
        try:
            attempts = int(row["attempts"] or 0)
        except (KeyError, IndexError, TypeError, ValueError):
            attempts = 0
        keys = row.keys()
        return Task(
            id=row["id"],
            title=row["title"],
            requires=_json_loads(row["requires"]),
            depends_on=_json_loads(row["depends_on"]),
            priority=row["priority"],
            status=TaskStatus(row["status"]),
            assigned_to=row["assigned_to"],
            created_at=_str_to_dt(row["created_at"]),
            updated_at=_str_to_dt(row["updated_at"]),
            version=row["version"],
            fail_reason=row["fail_reason"],
            result=(row["result"] if "result" in keys else None),
            attempts=attempts,
            lane_key=lane_key,
            role=role,
            description=(row["description"] or "") if "description" in keys else "",
            issues=(_json_loads(row["issues"])
                    if "issues" in keys and row["issues"] else []),
        )

    # -------------------------------------------------------------------
    # Lease manager
    # -------------------------------------------------------------------

    def create_lease(self, task_id: str, node_id: str, ttl: timedelta) -> Optional[Lease]:
        lease_id = _generate_id("lease")
        now = datetime.utcnow()
        expires = now + ttl
        lease = Lease(
            id=lease_id,
            task_id=task_id,
            node_id=node_id,
            created_at=now,
            expires_at=expires,
            status=LeaseStatus.active,
        )
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO leases (id, task_id, node_id, created_at, expires_at, status)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (lease_id, task_id, node_id, _dt_to_str(now),
                 _dt_to_str(expires), LeaseStatus.active.value),
            )
        return lease

    def revoke_lease(self, lease_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "UPDATE leases SET status = ? WHERE id = ?",
                (LeaseStatus.revoked.value, lease_id),
            )
            return result.rowcount > 0

    def get_active_leases(self) -> List[Lease]:
        now = datetime.utcnow()
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT * FROM leases WHERE status = ?",
                (LeaseStatus.active.value,),
            ).fetchall()

        active = []
        expired_ids = []
        for row in rows:
            lease = self._row_to_lease(row)
            if lease.expires_at > now:
                active.append(lease)
            else:
                lease.status = LeaseStatus.expired
                expired_ids.append(lease.id)
                if self._lease_callback:
                    try:
                        self._lease_callback(lease.task_id, lease.node_id)
                    except Exception:
                        pass

        # Mark expired leases
        if expired_ids:
            with self._tx() as conn:
                placeholders = ",".join("?" for _ in expired_ids)
                conn.execute(
                    f"UPDATE leases SET status = ? WHERE id IN ({placeholders})",
                    [LeaseStatus.expired.value] + expired_ids,
                )

        return active

    def set_lease_callback(self, fn: Callable[[str, str], None]) -> None:
        self._lease_callback = fn

    def get_lease_by_task(self, task_id: str) -> Optional[Lease]:
        """Get the active lease for a given task, if any."""
        if not task_id:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM leases WHERE task_id = ? AND status = ?",
                (task_id, LeaseStatus.active.value),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_lease(row)

    def get_expired_leases(self) -> List[Lease]:
        """Get all expired leases.

        Public API — avoids accessing internal _leases/_leases_lock directly.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM leases WHERE status = ?",
                (LeaseStatus.expired.value,),
            ).fetchall()
        return [self._row_to_lease(r) for r in rows]

    def _row_to_lease(self, row: sqlite3.Row) -> Lease:
        return Lease(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            created_at=_str_to_dt(row["created_at"]),
            expires_at=_str_to_dt(row["expires_at"]),
            status=LeaseStatus(row["status"]),
        )

    # -------------------------------------------------------------------
    # Sync state
    # -------------------------------------------------------------------

    def _sync_version(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(version) as v FROM sync_log"
            ).fetchone()
            return row["v"] if row and row["v"] else 0

    def sync_version(self) -> int:
        return self._sync_version()

    def handle_sync_message(self, msg: SyncMessage) -> bool:
        with self._lock:
            current = self._sync_version()
            if msg.version <= current:
                return False

        with self._tx() as conn:
            # Record sync message
            task_state_json = msg.task_state.model_dump_json() if msg.task_state else None
            conn.execute(
                """INSERT INTO sync_log (version, sender_node, task_state, event_type, timestamp)
                   VALUES (?, ?, ?, ?, ?)""",
                (msg.version, msg.sender_node, task_state_json,
                 msg.event_type.value, msg.timestamp),
            )

            # Apply task state if present
            if msg.task_state:
                existing = conn.execute(
                    "SELECT id FROM tasks WHERE id = ?", (msg.task_state.task_id,)
                ).fetchone()
                if existing:
                    conn.execute(
                        """UPDATE tasks SET status = ?, assigned_to = ?, version = ?,
                           updated_at = ? WHERE id = ?""",
                        (msg.task_state.status, msg.task_state.assigned_to,
                         msg.task_state.version, _dt_to_str(datetime.utcnow()),
                         msg.task_state.task_id),
                    )
                else:
                    conn.execute(
                        """INSERT INTO tasks
                           (id, title, status, assigned_to, version, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (msg.task_state.task_id, msg.task_state.title,
                         msg.task_state.status, msg.task_state.assigned_to,
                         msg.task_state.version,
                         _dt_to_str(datetime.utcnow()), _dt_to_str(datetime.utcnow())),
                    )
        return True

    def handle_batch_sync(self, batch: BatchSyncMessage) -> int:
        count = 0
        for msg in batch.messages:
            if self.handle_sync_message(msg):
                count += 1
        return count

    # -------------------------------------------------------------------
    # Recovery log
    # -------------------------------------------------------------------

    def append_recovery_event(self, event: RecoveryEvent) -> None:
        if not event.id:
            event.id = _generate_id("recovery")
        now = datetime.utcnow()
        event.timestamp = now
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO recovery_events (id, task_id, node_id, action, status, message, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (event.id, event.task_id, event.node_id, event.action,
                 event.status, event.message, _dt_to_str(now)),
            )

    def get_recovery_events(self) -> List[RecoveryEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM recovery_events ORDER BY timestamp"
            ).fetchall()
        return [self._row_to_recovery_event(r) for r in rows]

    def recovery_stats(self) -> Dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT action, COUNT(*) as c FROM recovery_events GROUP BY action"
            ).fetchall()
            total = sum(r["c"] for r in rows)
        by_action = {r["action"]: r["c"] for r in rows}
        return {"total": total, "by_action": by_action}

    def trigger_recovery(self, node_id: str) -> None:
        event = RecoveryEvent(
            id=_generate_id("recovery"),
            node_id=node_id,
            action="reschedule",
            status="completed",
            message=f"Node {node_id} went offline, rescheduling tasks",
        )
        self.append_recovery_event(event)

    def _row_to_recovery_event(self, row: sqlite3.Row) -> RecoveryEvent:
        return RecoveryEvent(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            action=row["action"],
            status=row["status"],
            message=row["message"],
            timestamp=_str_to_dt(row["timestamp"]),
        )

    # -------------------------------------------------------------------
    # Scheduling decisions
    # -------------------------------------------------------------------

    def record_decision(self, decision: SchedulingDecision) -> None:
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO scheduling_decisions
                   (task_id, task_title, priority, node_id, score, reason, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (decision.task_id, decision.task_title, decision.priority,
                 decision.node_id, decision.score, decision.reason,
                 _dt_to_str(decision.timestamp)),
            )
            # Trim old decisions
            count = conn.execute(
                "SELECT COUNT(*) as c FROM scheduling_decisions"
            ).fetchone()["c"]
            if count > self._max_decisions:
                conn.execute(
                    """DELETE FROM scheduling_decisions WHERE id NOT IN
                       (SELECT id FROM scheduling_decisions ORDER BY id DESC LIMIT ?)""",
                    (self._max_decisions,),
                )

    def get_decisions(self) -> List[SchedulingDecision]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM scheduling_decisions ORDER BY id"
            ).fetchall()
        return [self._row_to_decision(r) for r in rows]

    def get_schedule_stats(self) -> SchedulingStats:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM scheduling_decisions"
            ).fetchall()
        total = len(rows)
        by_priority: Dict[int, int] = {}
        decisions = []
        for row in rows:
            d = self._row_to_decision(row)
            decisions.append(d)
            by_priority[d.priority] = by_priority.get(d.priority, 0) + 1

        return SchedulingStats(
            total_decisions=total,
            decisions_by_priority=by_priority,
            last_decisions=decisions[-10:] if decisions else [],
        )

    def trigger_pending_tasks(self) -> int:
        """Promote pending tasks with all dependencies met to ready."""
        promoted = 0
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT id, depends_on FROM tasks WHERE status = ?",
                (TaskStatus.pending.value,),
            ).fetchall()

            for row in rows:
                depends = _json_loads(row["depends_on"])
                if not depends:
                    conn.execute(
                        """UPDATE tasks SET status = ?, updated_at = ?
                           WHERE id = ? AND status = ?""",
                        (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                         row["id"], TaskStatus.pending.value),
                    )
                    promoted += 1
                else:
                    all_done = True
                    for dep_id in depends:
                        dep_row = conn.execute(
                            "SELECT status FROM tasks WHERE id = ?", (dep_id,)
                        ).fetchone()
                        if dep_row is None or dep_row["status"] != TaskStatus.completed.value:
                            all_done = False
                            break
                    if all_done:
                        conn.execute(
                            """UPDATE tasks SET status = ?, updated_at = ?
                               WHERE id = ? AND status = ?""",
                            (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                             row["id"], TaskStatus.pending.value),
                        )
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
        ``task_id``/``task_title``/``node_id``/``priority``) so the trigger
        endpoint reports only new work, not every running task.

        Rules mirror the in-memory ``ClusterState.schedule_pending_detailed``
        (see ``hermes_cluster/core/scheduler.py``): online nodes only, least
        active tasks first with round-robin ties, ``max_concurrent`` never
        exceeded, active leases never re-assigned.
        """
        new_assignments: List[Dict[str, Any]] = []
        now = datetime.utcnow()
        with self._tx() as conn:
            node_rows = conn.execute(
                "SELECT * FROM nodes WHERE status = ?",
                (NodeStatus.online.value,),
            ).fetchall()
            if not node_rows:
                return new_assignments
            online_nodes = [self._row_to_node(r) for r in node_rows]

            # Per-node active load from tasks currently assigned to each node.
            active_counts: Dict[str, int] = {}
            active_statuses = [s.value for s in ACTIVE_TASK_STATUSES]
            active_ph = ",".join("?" for _ in active_statuses)
            for row in conn.execute(
                f"""SELECT assigned_to, COUNT(*) AS c FROM tasks
                    WHERE status IN ({active_ph}) AND assigned_to IS NOT NULL
                    GROUP BY assigned_to""",
                active_statuses,
            ).fetchall():
                if row["assigned_to"]:
                    active_counts[row["assigned_to"]] = row["c"]

            # Tasks that still hold an active lease must not be re-assigned.
            leased = {
                r["task_id"]
                for r in conn.execute(
                    "SELECT task_id FROM leases WHERE status = ? AND expires_at > ?",
                    (LeaseStatus.active.value, _dt_to_str(now)),
                ).fetchall()
            }

            # LFP-1 cardinality guard (#762): a ready task whose lane key has
            # an ACTIVE sibling (or an earlier queued sibling) must wait —
            # exactly one live sitting per lane, same invariant the
            # bdaya-enforcement lane_key_guard DENIES at spawn time.
            all_rows = conn.execute("SELECT * FROM tasks").fetchall()
            lane_blocked = lane_blocked_ready_ids(
                [self._row_to_task(r) for r in all_rows])

            ready_tasks = [
                self._row_to_task(r)
                for r in conn.execute(
                    """SELECT * FROM tasks WHERE status = ?
                       ORDER BY priority, created_at""",
                    (TaskStatus.ready.value,),
                ).fetchall()
                if r["id"] not in leased and r["id"] not in lane_blocked
            ]

            for task in ready_tasks:
                node = self._fair_scheduler.choose(
                    task.requires, online_nodes, active_counts
                )
                if node is None:
                    # No candidate with spare capacity: leave ready for a later trigger.
                    continue

                conn.execute(
                    """UPDATE tasks SET status = ?, assigned_to = ?,
                       updated_at = ?, version = version + 1
                       WHERE id = ?""",
                    (TaskStatus.running.value, node.id, _dt_to_str(now), task.id),
                )
                active_counts[node.id] = active_counts.get(node.id, 0) + 1
                self._fair_scheduler.mark_picked(node.id)

                decision = SchedulingDecision(
                    task_id=task.id,
                    task_title=task.title,
                    priority=task.priority,
                    node_id=node.id,
                    score=1.0,
                    reason="least_loaded_capability_match",
                )
                # Inline record_decision to avoid nested _tx()
                conn.execute(
                    """INSERT INTO scheduling_decisions
                       (task_id, task_title, priority, node_id, score, reason, timestamp)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (decision.task_id, decision.task_title, decision.priority,
                     decision.node_id, decision.score, decision.reason,
                     _dt_to_str(decision.timestamp)),
                )
                # Trim old decisions within same transaction
                count = conn.execute(
                    "SELECT COUNT(*) as c FROM scheduling_decisions"
                ).fetchone()["c"]
                if count > self._max_decisions:
                    conn.execute(
                        """DELETE FROM scheduling_decisions WHERE id NOT IN
                           (SELECT id FROM scheduling_decisions ORDER BY id DESC LIMIT ?)""",
                        (self._max_decisions,),
                    )

                new_assignments.append({
                    "task_id": task.id,
                    "task_title": task.title,
                    "node_id": node.id,
                    "priority": task.priority,
                })

        return new_assignments

    def _row_to_decision(self, row: sqlite3.Row) -> SchedulingDecision:
        return SchedulingDecision(
            task_id=row["task_id"],
            task_title=row["task_title"],
            priority=row["priority"],
            node_id=row["node_id"],
            score=row["score"],
            reason=row["reason"],
            timestamp=_str_to_dt(row["timestamp"]),
        )

    # -------------------------------------------------------------------
    # Federation registry
    # -------------------------------------------------------------------

    def register_federation_cluster(
        self, cluster_id: str, name: str, endpoint: str
    ) -> RemoteCluster:
        now = datetime.utcnow()
        cluster = RemoteCluster(
            id=cluster_id,
            name=name,
            endpoint=endpoint,
            status=FederationClusterStatus.available,
            registered_at=now,
            last_ping=now,
        )
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO federation_clusters
                   (id, name, endpoint, status, registered_at, last_ping, ping_latency)
                   VALUES (?, ?, ?, ?, ?, ?, 0.0)""",
                (cluster_id, name, endpoint, FederationClusterStatus.available.value,
                 _dt_to_str(now), _dt_to_str(now)),
            )
        return cluster

    def remove_federation_cluster(self, cluster_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM federation_clusters WHERE id = ?", (cluster_id,)
            )
            return result.rowcount > 0

    def get_federation_clusters(self) -> List[RemoteCluster]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM federation_clusters").fetchall()
        return [self._row_to_cluster(r) for r in rows]

    def get_federation_cluster(self, cluster_id: str) -> Optional[RemoteCluster]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM federation_clusters WHERE id = ?", (cluster_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_cluster(row)

    def _row_to_cluster(self, row: sqlite3.Row) -> RemoteCluster:
        return RemoteCluster(
            id=row["id"],
            name=row["name"],
            endpoint=row["endpoint"],
            status=FederationClusterStatus(row["status"]),
            registered_at=_str_to_dt(row["registered_at"]),
            last_ping=_str_to_dt(row["last_ping"]),
            ping_latency=row["ping_latency"],
        )

    # -------------------------------------------------------------------
    # Hook manager
    # -------------------------------------------------------------------

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
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO hooks (id, url, events, secret, active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 1, ?, ?)""",
                (hook_id, url, _json_dumps([e.value for e in events]),
                 secret or None, _dt_to_str(now), _dt_to_str(now)),
            )
        return hook

    def deregister_hook(self, hook_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM hooks WHERE id = ?", (hook_id,)
            )
            return result.rowcount > 0

    def list_hooks(self) -> List[Hook]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM hooks").fetchall()
        hooks = []
        for row in rows:
            h = self._row_to_hook(row)
            # Hide secrets (matching ClusterState behavior)
            hooks.append(h.model_copy(update={"secret": None}))
        return hooks

    def get_hook_deliveries(self, hook_id: str) -> List[Delivery]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries WHERE hook_id = ?", (hook_id,)
            ).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    def add_delivery(self, delivery: Delivery) -> None:
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO deliveries
                   (id, hook_id, event_type, payload, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (delivery.id, delivery.hook_id, delivery.event_type,
                 _json_dumps(delivery.payload), delivery.status,
                 _dt_to_str(delivery.created_at)),
            )
            # Trim old deliveries
            count = conn.execute(
                "SELECT COUNT(*) as c FROM deliveries"
            ).fetchone()["c"]
            if count > self._max_deliveries:
                conn.execute(
                    """DELETE FROM deliveries WHERE id NOT IN
                       (SELECT id FROM deliveries ORDER BY created_at DESC LIMIT ?)""",
                    (self._max_deliveries,),
                )

    def _row_to_hook(self, row: sqlite3.Row) -> Hook:
        return Hook(
            id=row["id"],
            url=row["url"],
            events=[EventType(e) for e in _json_loads(row["events"])],
            secret=row["secret"],
            active=bool(row["active"]),
            created_at=_str_to_dt(row["created_at"]),
            updated_at=_str_to_dt(row["updated_at"]),
        )

    def _row_to_delivery(self, row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"],
            hook_id=row["hook_id"],
            event_type=row["event_type"],
            payload=_json_loads(row["payload"]),
            status=row["status"],
            created_at=_str_to_dt(row["created_at"]),
        )

    # -------------------------------------------------------------------
    # Agent executor spawn tracking (task -> lane map)
    # -------------------------------------------------------------------

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
        """Persist a task spawn record (upsert by task_id)."""
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO task_spawns
                   (task_id, mode, job_id, pid, started_at, lease_id, lane_name,
                    result_path, lane_key, role, session_id, attempt)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (task_id, mode, job_id, pid, started_at, lease_id, lane_name,
                 result_path, lane_key, role, session_id, attempt),
            )

    def get_task_spawn(self, task_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM task_spawns WHERE task_id = ?", (task_id,)
            ).fetchone()
        return dict(row) if row else None

    def get_all_task_spawns(self) -> List[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM task_spawns").fetchall()
        return [dict(r) for r in rows]

    def delete_task_spawn(self, task_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM task_spawns WHERE task_id = ?", (task_id,)
            )
            return result.rowcount > 0

    # -------------------------------------------------------------------
    # Stateful lanes (lane_key -> hermes session map)
    # -------------------------------------------------------------------

    def record_lane(
        self,
        lane_key: str,
        session_id: str = "",
        profile: str = "",
        role: str = "author",
        node: str = "",
        created_at: Optional[float] = None,
        last_active_at: Optional[float] = None,
        last_task_id: str = "",
    ) -> None:
        """Upsert a lane record by lane_key (first created_at always wins).

        ``last_active_at`` is refreshed on every upsert (default: now) so the
        executor's idle reaper can measure the lane's idle time.
        """
        with self._tx() as conn:
            row = conn.execute(
                "SELECT created_at FROM lanes WHERE lane_key = ?", (lane_key,)
            ).fetchone()
            effective_created = (
                row["created_at"]
                if row
                else (created_at if created_at is not None else time.time())
            )
            effective_active = last_active_at if last_active_at is not None else time.time()
            conn.execute(
                """INSERT OR REPLACE INTO lanes
                   (lane_key, session_id, profile, role, node, created_at,
                    last_active_at, last_task_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (lane_key, session_id, profile, role, node,
                 effective_created, effective_active, last_task_id),
            )

    def get_lane(self, lane_key: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM lanes WHERE lane_key = ?", (lane_key,)
            ).fetchone()
        return dict(row) if row else None

    def get_all_lanes(self) -> List[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM lanes").fetchall()
        return [dict(r) for r in rows]

    def touch_lane_last_task(self, lane_key: str, task_id: str) -> None:
        """Update the last task delivered to a lane; no-op when lane absent."""
        if not lane_key:
            return
        with self._tx() as conn:
            conn.execute(
                "UPDATE lanes SET last_task_id = ? WHERE lane_key = ?",
                (task_id, lane_key),
            )

    def delete_lane(self, lane_key: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM lanes WHERE lane_key = ?", (lane_key,)
            )
            return result.rowcount > 0

    # -------------------------------------------------------------------
    # Config
    # -------------------------------------------------------------------

    def get_config(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM kv_store WHERE key = 'cluster_config'"
            ).fetchone()
        if row is None:
            return self._config
        self._config = _json_loads(row["value"])
        return self._config

    def set_config(self, config: Dict[str, Any]) -> None:
        self._config = config
        with self._tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('cluster_config', ?)",
                (_json_dumps(config),),
            )

    def get_config_path(self) -> str:
        return self._config_path

    def set_config_path(self, path: str) -> None:
        self._config_path = path

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------

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
