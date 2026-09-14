"""Async Postgres ClusterStore backend (asyncpg) — shared/claude-plugins#829.

Drop-in async counterpart of ``ClusterStore`` (SQLite): same tables, the same
public API surface, the same transactional guarantees — but safe for CONCURRENT
MULTI-NODE access, which is the whole point of moving cluster state to Postgres
(see #804 design note 132531: cloud main + workers share one DB).

Schema notes (SQLite -> Postgres mapping, design note table):
  * TEXT columns holding ISO datetimes -> TIMESTAMPTZ. The SQLite store wrote
    naive ``datetime.utcnow().isoformat()`` strings; those are read back as
    naive too, so the Postgres store normalises every timestamp to
    *tz-aware UTC* on write (``asyncpg`` refuses naive datetimes into
    TIMESTAMPTZ) and returns tz-aware UTC on read — ordering and comparisons
    are identical across backends.
  * INTEGER PRIMARY KEY AUTOINCREMENT -> BIGSERIAL PRIMARY KEY.
  * REAL (floats, epoch seconds) -> DOUBLE PRECISION.
  * JSON-in-TEXT columns stay TEXT: the SQLite store round-trips
    ``json.dumps``/``loads`` for ``capabilities``/``requires``/``depends_on``/
    ``payload``/``events``/``task_state`` and callers pass/expect Python lists
    parsed via ``_json_loads`` — TEXT keeps both backends' row mappers shared.
  * ``kv_store.key`` is a Postgres reserved word -> always double-quoted.
  * ``INSERT OR REPLACE`` -> ``INSERT ... ON CONFLICT (pk) DO UPDATE``.

Isolation & the places SQLite's single-writer lock was load-bearing:
  * All transactions run at READ COMMITTED (Postgres default). Where SQLite
    relied on its process RLock to make a read-then-write sequence atomic,
    the port uses EITHER a single atomic statement (upserts, guarded
    UPDATEs — e.g. ``create_task``'s status-guarded promotion,
    ``unassign_task``'s terminal+live-lease predicate, ``update_capabilities``'
    locked-CTE ``UPDATE ... RETURNING``, which also preserves the
    capability-change callback's old-caps argument without a separate read)
    OR the explicit
    transaction helper ``_txn`` for multi-statement sequences that must see
    their own writes (``record_decision``/``add_delivery`` insert+trim,
    ``handle_sync_message`` gate+append+apply — Postgres gives data-modifying
    CTEs writing and re-reading the SAME table no visibility guarantee, so
    sequential statements in one transaction are the correct port of the
    SQLite ``_tx`` blocks).
  * ``schedule_pending_detailed`` is the one place that genuinely needs a
    consistent snapshot across nodes: SQLite serialised it implicitly via the
    process-wide RLock (single writer). Here it takes
    ``pg_advisory_xact_lock(HERMES_SCHEDULE_LOCK)`` for the duration of the
    transaction so two cluster-mains/retries cannot assign the same ready
    task twice. Within-node callers are additionally serialised by an
    asyncio.Lock (the advisory lock is per-session; the store multiplexes a
    pool).
  * ``create_lease``: SQLite deliberately TOLERATED overlapping "active"
    leases for one task — the lease manager's ``extend()`` creates the
    replacement lease BEFORE revoking the old one, so a schema-level "one
    active lease per task" guard would deadlock renewal. The Postgres port
    keeps that tolerance exactly (equivalent guarantees, no stricter, no
    looser): the anti-double-scheduling guard is behavioural, not schema —
    ``schedule_pending_detailed`` refuses to assign a task that still holds
    an unexpired active lease, under the advisory lock above.
  * ``record_lane``: SQLite's select-then-replace inside ``_tx`` was atomic
    only because of the process lock. Here created_at-preservation is folded
    into the upsert (``COALESCE``/``EXCLUDED`` read from the conflict target).
  * The two counters in ``get_active_leases``+``schedule_pending_detailed``
    expire-marking remain side-effecting reads, mirroring the SQLite store
    (lease_manager documents it) — but each marking is one atomic UPDATE.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
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
    SyncMessage,
    SyncEventType,
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

# Advisory-lock keys shared by every process touching the same DB
# ("HE"++"RM"+... as big-endian bytes — arbitrary, stable).
HERMES_SCHEDULE_LOCK = 0x4845524D45535F1
HERMES_SYNC_LOCK = 0x4845524D45535F2

# ---------------------------------------------------------------------------
# Schema (idempotent — safe to run at every startup, #829 req. 3)
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    capabilities TEXT DEFAULT '[]',
    status TEXT DEFAULT 'online',
    last_heartbeat TIMESTAMPTZ,
    load DOUBLE PRECISION DEFAULT 0.0,
    max_concurrent INTEGER DEFAULT 0,
    disk_free_gb DOUBLE PRECISION,
    status_reason TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    requires TEXT DEFAULT '[]',
    depends_on TEXT DEFAULT '[]',
    priority INTEGER DEFAULT 3,
    status TEXT DEFAULT 'pending',
    assigned_to TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    version INTEGER DEFAULT 0,
    fail_reason TEXT,
    attempts INTEGER DEFAULT 0,
    lane_key TEXT DEFAULT '',
    role TEXT DEFAULT 'author',
    result TEXT,
    -- #762 grouped intake: bundle brief + restart-safe membership.
    description TEXT DEFAULT '',
    issues TEXT DEFAULT '[]'
);

-- Stateful lanes: one row per lane_key, keyed to the hermes session it owns.
CREATE TABLE IF NOT EXISTS lanes (
    lane_key TEXT PRIMARY KEY,
    session_id TEXT DEFAULT '',
    profile TEXT DEFAULT '',
    role TEXT DEFAULT 'author',
    node TEXT DEFAULT '',
    created_at DOUBLE PRECISION NOT NULL,
    last_active_at DOUBLE PRECISION DEFAULT 0,
    last_task_id TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS sync_log (
    id BIGSERIAL PRIMARY KEY,
    version INTEGER NOT NULL,
    sender_node TEXT DEFAULT '',
    task_state TEXT,
    event_type TEXT DEFAULT 'task_created',
    timestamp BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id TEXT PRIMARY KEY,
    task_id TEXT DEFAULT '',
    node_id TEXT DEFAULT '',
    action TEXT DEFAULT '',
    status TEXT DEFAULT '',
    message TEXT,
    timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS scheduling_decisions (
    id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    task_title TEXT NOT NULL,
    priority INTEGER DEFAULT 3,
    node_id TEXT NOT NULL,
    score DOUBLE PRECISION DEFAULT 0.0,
    reason TEXT DEFAULT '',
    timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS federation_clusters (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT DEFAULT 'available',
    registered_at TIMESTAMPTZ,
    last_ping TIMESTAMPTZ,
    ping_latency DOUBLE PRECISION DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    events TEXT DEFAULT '[]',
    secret TEXT,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    event_type TEXT DEFAULT '',
    payload TEXT DEFAULT '{}',
    status TEXT DEFAULT 'delivered',
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS kv_store (
    "key" TEXT PRIMARY KEY,
    value TEXT
);

-- Agent executor spawn tracking (task -> lane map, persisted across restarts).
CREATE TABLE IF NOT EXISTS task_spawns (
    task_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'bdaya-dispatch',
    job_id TEXT DEFAULT '',
    pid BIGINT DEFAULT 0,
    started_at DOUBLE PRECISION NOT NULL,
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
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalise a datetime for TIMESTAMPTZ: naive values are UTC (the SQLite
    store wrote utcnow() strings). asyncpg rejects naive datetimes."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _json_dumps(obj: Any) -> str:
    if obj is None:
        return "[]"
    return json.dumps(obj, default=str)


def _json_loads(s) -> Any:
    if s is None or s == "":
        return []
    if isinstance(s, (list, dict)):  # in case a JSONB column is ever used
        return s
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return []


def _rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# PostgresClusterStore
# ---------------------------------------------------------------------------

class PostgresClusterStore:
    """Async Postgres-backed cluster store — API-compatible with ClusterStore.

    All public methods are coroutines. ``SyncPostgresStore`` (below) wraps
    this class with a sync facade matching ClusterStore/ClusterState call
    sites one-for-one, so app.py, routers, and the lease/recovery/sync
    managers work unchanged against either backend.
    """

    def __init__(self, dsn: str, *, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._dsn = dsn  # SECRET — never logged
        self._loop = loop
        self._pool = None
        self._schedule_lock = asyncio.Lock()  # in-process serialiser for the
        #   advisory-locked schedule transaction (advisory locks are per-session).

        # Callbacks (same as ClusterStore)
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

        self._max_decisions: int = 200
        self._max_deliveries: int = 1000
        self._fair_scheduler = FairScheduler()

    # -------------------------------------------------------------------
    # Lifecycle / plumbing
    # -------------------------------------------------------------------

    async def connect(self) -> "PostgresClusterStore":
        """Create the connection pool and apply the schema idempotently."""
        import asyncpg

        if self._pool is not None:
            return self
        # NOTE: never log the DSN — it embeds the password.
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)
            # Idempotent column drift for databases created before #870
            # (CREATE TABLE IF NOT EXISTS never touches an existing table):
            # the main-side re-queue counter and the per-spawn delivery count.
            # #892 adds the disk-preflight columns to `nodes` the same way.
            await conn.execute(
                "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS disk_free_gb DOUBLE PRECISION")
            await conn.execute(
                "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS status_reason TEXT DEFAULT ''")
            await conn.execute(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0")
            # #874: the lane deliverable, so a result outlives the node that made it.
            await conn.execute(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS result TEXT")
            await conn.execute(
                "ALTER TABLE task_spawns ADD COLUMN IF NOT EXISTS attempt INTEGER DEFAULT 0")
            # #762 grouped intake: bundle brief + membership columns.
            await conn.execute(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''")
            await conn.execute(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS issues TEXT DEFAULT '[]'")
        logger.info("PostgresClusterStore: connected, schema ensured")
        return self

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            raise RuntimeError("PostgresClusterStore.connect() has not been awaited")
        return self._pool

    def _fetch(self, sql: str, *args):
        return self.pool.execute(sql, *args)

    async def _exec_status_rowcount(self, sql: str, *args) -> int:
        """asyncpg execute() returns a status string like 'UPDATE 3'."""
        status = await self.pool.execute(sql, *args)
        try:
            return int(status.rsplit(" ", 1)[-1])
        except (ValueError, IndexError):
            return 0

    async def _row(self, sql: str, *args):
        return await self.pool.fetchrow(sql, *args)

    async def _all(self, sql: str, *args):
        return await self.pool.fetch(sql, *args)

    def set_on_node_online(self, fn: Callable[[str], None]) -> None:
        self._on_node_online = fn

    def set_on_capability_change(self, fn: Callable[[str, List[str], List[str]], None]) -> None:
        self._on_capability_change = fn

    def set_lease_callback(self, fn: Callable[[str, str], None]) -> None:
        self._lease_callback = fn

    def get_config_path(self) -> str:
        return self._config_path

    def set_config_path(self, path: str) -> None:
        self._config_path = path

    def append_timeline(self, event) -> None:  # parity: no-op
        pass

    async def truncate_all(self) -> None:
        """DROP + re-create the schema. TEST/DEV ONLY (parity suites need a
        clean DB between cases; production must never call this)."""
        drop = "".join(
            f"DROP TABLE IF EXISTS {t} CASCADE;\n"
            for t in ("nodes", "tasks", "lanes", "leases", "sync_log",
                      "recovery_events", "scheduling_decisions",
                      "federation_clusters", "hooks", "deliveries",
                      "kv_store", "task_spawns")
        )
        async with self._txn() as conn:
            await conn.execute(drop)
            await conn.execute(_SCHEMA_SQL)

    def _txn(self):
        """Async context: one pool connection in one transaction.

        Compound write sequences (insert+promote, insert+trim, gate+insert+
        apply) run here so later statements see earlier ones — Postgres
        data-modifying CTEs that re-scan the same table have undefined
        visibility, so an explicit transaction is the correct port of the
        SQLite store's ``_tx`` blocks.
        """

        class _Tx:
            def __init__(self, outer):
                self._outer = outer
                self.conn = None
                self._stack = None

            async def __aenter__(self):
                self.conn = await self._outer.pool.acquire()
                self._stack = self.conn.transaction()
                await self._stack.start()
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                if exc_type is None:
                    await self._stack.commit()
                else:
                    await self._stack.rollback()
                await self._outer.pool.release(self.conn)
                return False

        return _Tx(self)

    # -------------------------------------------------------------------
    # Node registry
    # -------------------------------------------------------------------

    async def register_node(self, node: Node) -> None:
        await self._fetch(
            """INSERT INTO nodes (id, name, capabilities, status, last_heartbeat, load, max_concurrent,
                 disk_free_gb, status_reason)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (id) DO UPDATE SET
                 name = EXCLUDED.name,
                 capabilities = EXCLUDED.capabilities,
                 status = EXCLUDED.status,
                 last_heartbeat = EXCLUDED.last_heartbeat,
                 load = EXCLUDED.load,
                 max_concurrent = EXCLUDED.max_concurrent,
                 disk_free_gb = EXCLUDED.disk_free_gb,
                 status_reason = EXCLUDED.status_reason""",
            node.id, node.name, _json_dumps(node.capabilities),
            node.status.value, _aware(node.last_heartbeat), float(node.load),
            int(node.max_concurrent),
            node.disk_free_gb, getattr(node, "status_reason", ""),
        )
        if self._on_node_online:
            try:
                self._on_node_online(node.id)
            except Exception:
                pass

    async def get_node(self, node_id: str) -> Optional[Node]:
        row = await self._row("SELECT * FROM nodes WHERE id = $1", node_id)
        return self._row_to_node(row) if row else None

    async def get_all_nodes(self) -> List[Node]:
        rows = await self._all("SELECT * FROM nodes")
        return [self._row_to_node(r) for r in rows]

    async def update_heartbeat(self, node_id: str, load: float = 0.0,
                               disk_free_gb: Optional[float] = None,
                               status_reason: str = "") -> None:
        # #892: status follows the caller's verdict — NodeManager passes
        # status_reason when the reported disk is below the floor; the beat
        # refreshes the clock WITHOUT forcing online (the unconditional force
        # is what kept the full-disk node schedulable during the 2026-09-14
        # incident). Empty reason = the pre-#892 force-online. disk_free_gb
        # None (older worker) leaves the stored column untouched.
        status = NodeStatus.degraded.value if status_reason else NodeStatus.online.value
        if disk_free_gb is None:
            await self._fetch(
                "UPDATE nodes SET last_heartbeat = $1, status = $2, load = $3, "
                "status_reason = $4 WHERE id = $5",
                _utcnow(), status, load, status_reason, node_id,
            )
        else:
            await self._fetch(
                "UPDATE nodes SET last_heartbeat = $1, status = $2, load = $3, "
                "disk_free_gb = $4, status_reason = $5 WHERE id = $6",
                _utcnow(), status, load, float(disk_free_gb), status_reason, node_id,
            )

    async def update_capabilities(self, node_id: str, caps: List[str]) -> None:
        # ONE atomic statement (round-2 review of 1043352). The previous
        # SELECT-then-UPDATE lost concurrent updates: A read ["tooling"],
        # B read ["tooling"], A wrote [...,"planning"], B wrote
        # [...,"reviewing"] — "planning" gone, and the scheduler strands
        # work on a node whose stored capabilities no longer match its
        # declared ones. SQLite was safe only via its process RLock;
        # cross-node Postgres does not serialise two statements.
        #
        # Shape chosen: UPDATE ... RETURNING with the predecessor read from
        # a locked (FOR UPDATE) CTE, diff computed from the RETURNED row.
        # Why this over the other two shapes the reviewer offered:
        #   * Dropping the old-caps callback is NOT an option — cluster_core
        #     wires _on_capability_change to trigger_pending+schedule and
        #     the parity suite asserts the exact (node, old, new) args on
        #     BOTH backends, so behaviour must be kept.
        #   * An advisory lock would serialise MORE than needed (all nodes'
        #     updates behind one global key) to protect what a plain row
        #     lock already covers: the CTE's FOR UPDATE takes the row lock,
        #     so racing writers queue per-row, and at READ COMMITTED the
        #     blocked statement re-reads the LATEST committed row — the
        #     returned old_caps is the true predecessor, never a stale
        #     snapshot (verified empirically: session B's old_caps equals
        #     session A's committed new_caps — regression test:
        #     tests_v3/test_store_caps_race_pg.py).
        # Unknown node: the UPDATE matches nothing, RETURNING yields no row,
        # and no callback fires. The SQLite store fires with old=[] for any
        # node_id, but every in-app caller (node_manager) checks existence
        # first, so this divergence is unreachable; the parity test only
        # exercises existing nodes.
        row = await self.pool.fetchrow(
            """WITH prev AS (
                   SELECT capabilities AS old_caps FROM nodes
                   WHERE id = $2
                   FOR UPDATE
               )
               UPDATE nodes SET capabilities = $1
               FROM prev
               WHERE nodes.id = $2
               RETURNING prev.old_caps AS old_caps,
                         nodes.capabilities AS new_caps""",
            _json_dumps(caps), node_id,
        )
        if row is None or not self._on_capability_change:
            return
        old_caps = _json_loads(row["old_caps"])
        # Fire unconditionally on a successful write, exactly like the
        # SQLite store (which does not diff-gate), with the true
        # predecessor and the stored successor.
        try:
            self._on_capability_change(node_id, old_caps, caps)
        except Exception:
            pass

    async def update_max_concurrent(self, node_id: str, max_concurrent: int) -> None:
        await self._fetch(
            "UPDATE nodes SET max_concurrent = $1 WHERE id = $2",
            max(0, int(max_concurrent)), node_id,
        )

    async def node_count(self) -> int:
        row = await self._row("SELECT COUNT(*) AS c FROM nodes")
        return row["c"]

    async def online_count(self) -> int:
        row = await self._row(
            "SELECT COUNT(*) AS c FROM nodes WHERE status = $1",
            NodeStatus.online.value,
        )
        return row["c"]

    async def set_node_status(self, node_id: str, status: NodeStatus,
                              reason: str = "") -> None:
        await self._fetch(
            "UPDATE nodes SET status = $1, status_reason = $2 WHERE id = $3",
            status.value,
            reason if status != NodeStatus.online else "",
            node_id,
        )

    def _row_to_node(self, row) -> Node:
        hb = row["last_heartbeat"]
        keys = row.keys()
        return Node(
            id=row["id"],
            name=row["name"],
            capabilities=_json_loads(row["capabilities"]),
            status=NodeStatus(row["status"]),
            last_heartbeat=hb if hb else datetime.utcnow(),
            load=row["load"],
            max_concurrent=row["max_concurrent"] or 0,
            # #892: a DB created before the drift ALTERs lacks the columns —
            # treat as never-reported/blank (identical semantics to an older
            # worker that never sends disk_free_gb).
            disk_free_gb=row["disk_free_gb"] if "disk_free_gb" in keys else None,
            status_reason=row["status_reason"] if "status_reason" in keys else "",
        )

    # -------------------------------------------------------------------
    # Task store
    # -------------------------------------------------------------------

    async def create_task(
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
        now = _utcnow()
        # One transaction: insert-if-absent then promote pending->ready
        # (no dependencies at creation, mirroring the SQLite store). These
        # MUST be sequential statements — sibling data-modifying CTEs in one
        # query do not see each other's rows on Postgres. The status-guarded
        # UPDATE keeps concurrent create/demote writers correct.
        async with self._txn() as conn:
            await conn.execute(
                """INSERT INTO tasks
                   (id, title, requires, depends_on, priority, status,
                    created_at, updated_at, version, lane_key, role,
                    description, issues)
                   VALUES ($1, $2, $3, '[]', $4, $5, $6, $6, 1, $7, $8, $9, $10)
                   ON CONFLICT (id) DO NOTHING""",
                task_id, title, _json_dumps(requires), priority,
                TaskStatus.pending.value, now, lane_key, role,
                description, _json_dumps(list(issues or [])),
            )
            await conn.execute(
                """UPDATE tasks SET status = $1, updated_at = $2
                   WHERE id = $3 AND status = $4""",
                TaskStatus.ready.value, now, task_id, TaskStatus.pending.value,
            )
            row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
        return self._row_to_task(row)

    async def create_tasks_batch(self, plans: List[Dict[str, Any]]) -> List[Task]:
        """Create MANY tasks atomically — one bundle per grouped-intake
        PHASE-2 plan, all published or none (PR#43 review finding 3).

        Structural twin of ``ClusterStore.create_tasks_batch``: ONE ``_txn()``
        wraps the ENTIRE loop (per-plan ``create_task`` commits one
        transaction each, so a crash between two writes stranded a partial
        batch — and the hosted main runs this class). asyncpg rolls the whole
        transaction back on any exception escaping the block. Statements run
        on the transaction ``conn`` (pool-level ``self._fetch`` would
        auto-commit on checkout and make the transaction decorative). The
        per-row insert+guarded-promote semantics are identical to
        ``create_task``; the kick (trigger/schedule) stays in the caller.
        """
        now = _utcnow()
        async with self._txn() as conn:
            for plan in plans:
                await conn.execute(
                    """INSERT INTO tasks
                       (id, title, requires, depends_on, priority, status,
                        created_at, updated_at, version, lane_key, role,
                        description, issues)
                       VALUES ($1, $2, $3, '[]', $4, $5, $6, $6, 1, $7, $8, $9, $10)
                       ON CONFLICT (id) DO NOTHING""",
                    plan["task_id"], plan["title"],
                    _json_dumps(list(plan.get("requires") or [])),
                    plan.get("priority", 3),
                    TaskStatus.pending.value, now,
                    plan.get("lane_key", ""), plan.get("role", "author"),
                    plan.get("description", ""),
                    _json_dumps(list(plan.get("issues") or [])),
                )
            # Promote ONLY this batch's rows (no dependencies at creation,
            # mirroring create_task's status-guarded UPDATE).
            ids = [plan["task_id"] for plan in plans]
            if ids:
                await conn.execute(
                    f"""UPDATE tasks SET status = $1, updated_at = $2
                        WHERE id = ANY($3::text[]) AND status = $4""",
                    TaskStatus.ready.value, now, ids,
                    TaskStatus.pending.value,
                )
            rows = await conn.fetch(
                "SELECT * FROM tasks WHERE id = ANY($1::text[])", ids)
        return [self._row_to_task(r) for r in rows]

    async def get_task(self, task_id: str) -> Optional[Task]:
        row = await self._row("SELECT * FROM tasks WHERE id = $1", task_id)
        return self._row_to_task(row) if row else None

    async def get_all_tasks(self) -> List[Task]:
        rows = await self._all("SELECT * FROM tasks")
        return [self._row_to_task(r) for r in rows]

    async def set_task_result(self, task_id: str, result: Optional[str]) -> bool:
        """Persist a lane's deliverable on the task row (#874). No-op on empty."""
        if result is None or not str(result).strip():
            return False
        async with self._pool.acquire() as conn:
            res = await conn.execute(
                """UPDATE tasks SET result = $1, updated_at = $2, version = version + 1
                   WHERE id = $3""",
                str(result), datetime.now(timezone.utc), task_id,
            )
        return res.endswith("1")

    async def set_task_status(
        self, task_id: str, status: TaskStatus, fail_reason: str = ""
    ) -> bool:
        now = _utcnow()
        if fail_reason:
            n = await self._exec_status_rowcount(
                """UPDATE tasks SET status = $1, updated_at = $2,
                   version = version + 1, fail_reason = $3 WHERE id = $4""",
                status.value, now, fail_reason, task_id,
            )
        else:
            n = await self._exec_status_rowcount(
                """UPDATE tasks SET status = $1, updated_at = $2,
                   version = version + 1 WHERE id = $3""",
                status.value, now, task_id,
            )
        return n > 0

    async def unassign_task(self, task_id: str) -> bool:
        """Atomic guarded un-assign (same predicate as the SQLite store):
        terminal tasks never revive; a task with a live active lease is never
        unassigned. Single statement -> no read-modify-write race across nodes."""
        now = _utcnow()
        terminal_ids = [s.value for s in TERMINAL_TASK_STATUSES]
        n = await self._exec_status_rowcount(
            """UPDATE tasks SET assigned_to = NULL, status = $1,
                updated_at = $2, version = version + 1
                WHERE id = $3
                  AND status <> ALL($4::text[])
                  AND NOT EXISTS (
                      SELECT 1 FROM leases
                      WHERE leases.task_id = tasks.id
                        AND leases.status = $5
                        AND leases.expires_at > $2
                  )""",
            TaskStatus.ready.value, now, task_id, terminal_ids,
            LeaseStatus.active.value,
        )
        return n > 0

    async def unblock_task(self, task_id: str) -> bool:
        n = await self._exec_status_rowcount(
            """UPDATE tasks SET status = $1, updated_at = $2, version = version + 1
               WHERE id = $3 AND status = $4""",
            TaskStatus.pending.value, _utcnow(), task_id, TaskStatus.blocked.value,
        )
        return n > 0

    async def requeue_task(self, task_id: str, reason: str = "") -> bool:
        """#870: return a task to 'ready' for another delivery attempt after
        a worker reported a NON-DELIVERABLE result body. Mirrors the SQLite
        store: lease revocation + guarded UPDATE in ONE transaction (the
        guarded UPDATE carries the terminal check and the attempts<cap check
        atomically, so two racing reporters cannot both requeue past the cap).

        Returns True when the task was actually re-queued.
        """
        limit = int(getattr(self, "task_retry_limit", 3))
        now = _utcnow()
        terminal_ids = [s.value for s in TERMINAL_TASK_STATUSES]
        async with self._txn() as conn:
            await conn.execute(
                """UPDATE leases SET status = $1
                   WHERE task_id = $2 AND status = $3 AND expires_at > $4""",
                LeaseStatus.revoked.value, task_id,
                LeaseStatus.active.value, now,
            )
            status = await conn.execute(
                """UPDATE tasks
                   SET status = $1, assigned_to = NULL, updated_at = $2,
                       version = version + 1, attempts = COALESCE(attempts, 0) + 1,
                       fail_reason = COALESCE($5, fail_reason)
                   WHERE id = $3
                     AND status <> ALL($4::text[])
                     AND COALESCE(attempts, 0) < $6""",
                TaskStatus.ready.value, now, task_id, terminal_ids,
                reason or None, limit,
            )
        return status != "UPDATE 0"

    async def set_dependencies(self, task_id: str, depends_on: List[str]) -> bool:
        now = _utcnow()
        async with self._txn() as conn:
            status = await conn.execute(
                """UPDATE tasks SET depends_on = $1, updated_at = $2,
                       version = version + 1
                   WHERE id = $3""",
                _json_dumps(depends_on), now, task_id,
            )
            found = status != "UPDATE 0"
            # If deps added and task was ready, demote to pending until deps met
            # (sequential statement inside the same transaction — the CTE form
            # would not see the UPDATE above).
            if depends_on and found:
                await conn.execute(
                    """UPDATE tasks SET status = $1
                       WHERE id = $2 AND status = $3""",
                    TaskStatus.pending.value, task_id, TaskStatus.ready.value,
                )
        return found

    async def get_dependents(self, task_id: str) -> List[str]:
        rows = await self._all(
            "SELECT id, depends_on FROM tasks WHERE depends_on LIKE $1",
            f'%"{task_id}"%',
        )
        return [r["id"] for r in rows if task_id in _json_loads(r["depends_on"])]

    async def get_trigger_chain(self, task_id: str, max_depth: int = 10) -> List[str]:
        chain: List[str] = []
        visited: set = set()

        async def _traverse(tid: str, depth: int):
            if depth >= max_depth or tid in visited:
                return
            visited.add(tid)
            for dep_id in await self.get_dependents(tid):
                chain.append(dep_id)
                await _traverse(dep_id, depth + 1)

        await _traverse(task_id, 0)
        return chain

    async def get_workflow_graph(self) -> Dict[str, Any]:
        rows = await self._all("SELECT * FROM tasks")
        nodes, edges = [], []
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

    async def task_counts(self) -> Dict[str, int]:
        counts = {
            "total": 0, "ready": 0, "running": 0,
            "completed": 0, "failed": 0, "pending": 0,
        }
        rows = await self._all(
            "SELECT status, COUNT(*) AS c FROM tasks GROUP BY status"
        )
        for row in rows:
            counts["total"] += row["c"]
            if row["status"] in counts:
                counts[row["status"]] += row["c"]
        return counts

    def _row_to_task(self, row) -> Task:
        created = row["created_at"] or datetime.utcnow()
        updated = row["updated_at"] or datetime.utcnow()
        return Task(
            id=row["id"],
            title=row["title"],
            requires=_json_loads(row["requires"]),
            depends_on=_json_loads(row["depends_on"]),
            priority=row["priority"],
            status=TaskStatus(row["status"]),
            assigned_to=row["assigned_to"],
            created_at=created.replace(tzinfo=None) if created.tzinfo else created,
            updated_at=updated.replace(tzinfo=None) if updated.tzinfo else updated,
            version=row["version"],
            fail_reason=row["fail_reason"],
            result=(row["result"] if "result" in row else None),
            attempts=int(row["attempts"] or 0) if "attempts" in row.keys() else 0,
            lane_key=row["lane_key"] or "",
            role=row["role"] or "author",
            description=(row["description"] or "") if "description" in row.keys() else "",
            issues=(_json_loads(row["issues"])
                    if "issues" in row.keys() and row["issues"] else []),
        )

    # -------------------------------------------------------------------
    # Lease manager
    # -------------------------------------------------------------------

    async def create_lease(self, task_id: str, node_id: str, ttl: timedelta) -> Optional[Lease]:
        lease_id = f"lease_{secrets.token_hex(8)}"
        now = _utcnow()
        expires = now + ttl
        # Mirrors the SQLite store: no "one active lease per task" constraint —
        # lease_manager.extend() creates the replacement BEFORE revoking the
        # old lease, so a momentary overlap of unexpired actives is EXPECTED.
        # (See module docstring; the anti-double-scheduling guard lives in
        # schedule_pending_detailed under the advisory lock.)
        await self._fetch(
            """INSERT INTO leases (id, task_id, node_id, created_at, expires_at, status)
               VALUES ($1, $2, $3, $4, $5, $6)""",
            lease_id, task_id, node_id, now, expires, LeaseStatus.active.value,
        )
        return Lease(
            id=lease_id, task_id=task_id, node_id=node_id,
            created_at=now.replace(tzinfo=None), expires_at=expires.replace(tzinfo=None),
            status=LeaseStatus.active,
        )

    async def revoke_lease(self, lease_id: str) -> bool:
        n = await self._exec_status_rowcount(
            "UPDATE leases SET status = $1 WHERE id = $2",
            LeaseStatus.revoked.value, lease_id,
        )
        return n > 0

    async def get_active_leases(self) -> List[Lease]:
        now = _utcnow()
        rows = await self._all(
            "SELECT * FROM leases WHERE status = $1", LeaseStatus.active.value,
        )
        active, expired_ids = [], []
        for row in rows:
            lease = self._row_to_lease(row)
            exp = _aware(row["expires_at"])
            if exp > now:
                active.append(lease)
            else:
                lease.status = LeaseStatus.expired
                expired_ids.append(lease.id)
                if self._lease_callback:
                    try:
                        self._lease_callback(lease.task_id, lease.node_id)
                    except Exception:
                        pass

        if expired_ids:
            # One atomic marking statement — safe if another node raced it.
            await self._fetch(
                """UPDATE leases SET status = $1
                   WHERE id = ANY($2::text[]) AND status = $3""",
                LeaseStatus.expired.value, expired_ids, LeaseStatus.active.value,
            )
        return active

    async def get_lease_by_task(self, task_id: str) -> Optional[Lease]:
        if not task_id:
            return None
        row = await self._row(
            "SELECT * FROM leases WHERE task_id = $1 AND status = $2 LIMIT 1",
            task_id, LeaseStatus.active.value,
        )
        return self._row_to_lease(row) if row else None

    async def get_expired_leases(self) -> List[Lease]:
        rows = await self._all(
            "SELECT * FROM leases WHERE status = $1", LeaseStatus.expired.value,
        )
        return [self._row_to_lease(r) for r in rows]

    def _row_to_lease(self, row) -> Lease:
        def _naive(dt):
            return dt.replace(tzinfo=None) if dt and dt.tzinfo else dt
        return Lease(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            created_at=_naive(row["created_at"]) or datetime.utcnow(),
            expires_at=_naive(row["expires_at"]) or datetime.utcnow(),
            status=LeaseStatus(row["status"]),
        )

    # -------------------------------------------------------------------
    # Sync state
    # -------------------------------------------------------------------

    async def sync_version(self) -> int:
        row = await self._row("SELECT COALESCE(MAX(version), 0) AS v FROM sync_log")
        return row["v"]

    async def handle_sync_message(self, msg: SyncMessage) -> bool:
        """Version-gated LWW apply — exact SQLite semantics, made cross-node.

        The SQLite store serialised the "msg.version > MAX(version)" check
        with the append+apply via its process RLock. Two Postgres nodes could
        both pass that check concurrently, so the whole check->append->apply
        sequence runs under a transaction holding an advisory lock — the
        same tool used for scheduling. The "reject version <= current" rule
        (older AND equal versions) is preserved literally.
        """
        task_state_json = msg.task_state.model_dump_json() if msg.task_state else None
        now = _utcnow()
        async with self._txn() as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1)", HERMES_SYNC_LOCK,
            )
            row = await conn.fetchrow("SELECT COALESCE(MAX(version), 0) AS v FROM sync_log")
            if msg.version <= row["v"]:
                return False
            await conn.execute(
                """INSERT INTO sync_log (version, sender_node, task_state, event_type, timestamp)
                   VALUES ($1, $2, $3, $4, $5)""",
                msg.version, msg.sender_node, task_state_json,
                msg.event_type.value, msg.timestamp,
            )
            if msg.task_state:
                await conn.execute(
                    """INSERT INTO tasks (id, title, status, assigned_to, version,
                                          created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $6)
                       ON CONFLICT (id) DO UPDATE SET
                         status = EXCLUDED.status,
                         assigned_to = EXCLUDED.assigned_to,
                         version = EXCLUDED.version,
                         updated_at = EXCLUDED.updated_at""",
                    msg.task_state.task_id, msg.task_state.title,
                    msg.task_state.status, msg.task_state.assigned_to,
                    msg.task_state.version, now,
                )
        return True

    async def handle_batch_sync(self, batch: BatchSyncMessage) -> int:
        count = 0
        for msg in batch.messages:
            if await self.handle_sync_message(msg):
                count += 1
        return count

    # -------------------------------------------------------------------
    # Recovery log
    # -------------------------------------------------------------------

    async def append_recovery_event(self, event: RecoveryEvent) -> None:
        if not event.id:
            event.id = f"recovery_{secrets.token_hex(8)}"
        now = _utcnow()
        event.timestamp = now
        await self._fetch(
            """INSERT INTO recovery_events (id, task_id, node_id, action, status, message, timestamp)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (id) DO NOTHING""",
            event.id, event.task_id, event.node_id, event.action,
            event.status, event.message, now,
        )

    async def get_recovery_events(self) -> List[RecoveryEvent]:
        rows = await self._all("SELECT * FROM recovery_events ORDER BY timestamp")
        return [self._row_to_recovery_event(r) for r in rows]

    async def recovery_stats(self) -> Dict[str, Any]:
        rows = await self._all(
            "SELECT action, COUNT(*) AS c FROM recovery_events GROUP BY action"
        )
        total = sum(r["c"] for r in rows)
        return {"total": total, "by_action": {r["action"]: r["c"] for r in rows}}

    async def trigger_recovery(self, node_id: str) -> None:
        await self.append_recovery_event(RecoveryEvent(
            id=f"recovery_{secrets.token_hex(8)}",
            node_id=node_id,
            action="reschedule",
            status="completed",
            message=f"Node {node_id} went offline, rescheduling tasks",
        ))

    def _row_to_recovery_event(self, row) -> RecoveryEvent:
        ts = row["timestamp"]
        return RecoveryEvent(
            id=row["id"], task_id=row["task_id"], node_id=row["node_id"],
            action=row["action"], status=row["status"], message=row["message"],
            timestamp=(ts.replace(tzinfo=None) if ts and ts.tzinfo else ts)
            or datetime.utcnow(),
        )

    # -------------------------------------------------------------------
    # Scheduling decisions
    # -------------------------------------------------------------------

    async def record_decision(self, decision: SchedulingDecision) -> None:
        # Insert + trim as sequential statements in ONE transaction (mirrors
        # the SQLite _tx block). A single multi-CTE form would have undefined
        # visibility: sibling CTEs writing and reading the same table do not
        # see each other's effects on Postgres.
        async with self._txn() as conn:
            await conn.execute(
                """INSERT INTO scheduling_decisions
                   (task_id, task_title, priority, node_id, score, reason, timestamp)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                decision.task_id, decision.task_title, decision.priority,
                decision.node_id, decision.score, decision.reason,
                _aware(decision.timestamp),
            )
            await conn.execute(
                """DELETE FROM scheduling_decisions
                   WHERE id NOT IN (
                       SELECT id FROM scheduling_decisions
                       ORDER BY id DESC LIMIT $1
                   )""",
                self._max_decisions,
            )

    async def get_decisions(self) -> List[SchedulingDecision]:
        rows = await self._all("SELECT * FROM scheduling_decisions ORDER BY id")
        return [self._row_to_decision(r) for r in rows]

    async def get_schedule_stats(self) -> SchedulingStats:
        rows = await self._all("SELECT * FROM scheduling_decisions")
        decisions = [self._row_to_decision(r) for r in rows]
        by_priority: Dict[int, int] = {}
        for d in decisions:
            by_priority[d.priority] = by_priority.get(d.priority, 0) + 1
        return SchedulingStats(
            total_decisions=len(decisions),
            decisions_by_priority=by_priority,
            last_decisions=decisions[-10:],
        )

    # -------------------------------------------------------------------
    # Promotion / scheduling
    # -------------------------------------------------------------------

    async def trigger_pending_tasks(self) -> int:
        """Promote pending tasks whose dependencies are all completed.

        Mirrors the SQLite store statement-for-statement (Python-side JSON
        parse — legacy rows can carry non-JSON depends_on — and a
        status-guarded UPDATE per promotion, so a concurrent writer cannot
        double-promote: the guard makes the loser a no-op). The read of
        depends_on/dependency status is advisory like SQLite's was under its
        lock: a promotion racing a dep-completion still lands on a valid
        state machine transition, and the scheduler re-runs this on every
        trigger tick.
        """
        promoted = 0
        rows = await self._all(
            "SELECT id, depends_on FROM tasks WHERE status = $1",
            TaskStatus.pending.value,
        )
        now = _utcnow()
        for row in rows:
            depends = _json_loads(row["depends_on"])
            if not depends:
                ok = True
            else:
                statuses = await self._all(
                    "SELECT id, status FROM tasks WHERE id = ANY($1::text[])",
                    depends,
                )
                by_id = {s["id"]: s["status"] for s in statuses}
                ok = all(
                    by_id.get(d) == TaskStatus.completed.value for d in depends
                )
            if ok:
                n = await self._exec_status_rowcount(
                    """UPDATE tasks SET status = $1, updated_at = $2
                       WHERE id = $3 AND status = $4""",
                    TaskStatus.ready.value, now, row["id"],
                    TaskStatus.pending.value,
                )
                promoted += n
        return promoted

    async def schedule_pending(self) -> int:
        return len(await self.schedule_pending_detailed())

    async def schedule_pending_detailed(self) -> List[Dict[str, Any]]:
        """Assign ready tasks to the least-loaded capable online node.

        Cross-node safety: the whole snapshot->assign sequence runs under
        pg_advisory_xact_lock, replacing the role SQLite's single-writer
        process lock played. Two cluster mains cannot interleave a stale
        load snapshot and double-assign a ready task.
        """
        new_assignments: List[Dict[str, Any]] = []
        now = _utcnow()
        async with self._schedule_lock:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)", HERMES_SCHEDULE_LOCK,
                    )
                    node_rows = await conn.fetch(
                        "SELECT * FROM nodes WHERE status = $1",
                        NodeStatus.online.value,
                    )
                    if not node_rows:
                        return new_assignments
                    online_nodes = [self._row_to_node(r) for r in node_rows]

                    active_counts: Dict[str, int] = {}
                    active_statuses = [s.value for s in ACTIVE_TASK_STATUSES]
                    for row in await conn.fetch(
                        """SELECT assigned_to, COUNT(*) AS c FROM tasks
                           WHERE status = ANY($1::text[]) AND assigned_to IS NOT NULL
                           GROUP BY assigned_to""",
                        active_statuses,
                    ):
                        if row["assigned_to"]:
                            active_counts[row["assigned_to"]] = row["c"]

                    leased = {
                        r["task_id"]
                        for r in await conn.fetch(
                            """SELECT task_id FROM leases
                               WHERE status = $1 AND expires_at > $2""",
                            LeaseStatus.active.value, now,
                        )
                    }

                    # LFP-1 cardinality guard (#762) — see cluster_store for
                    # the full rationale: never assign a ready task whose
                    # lane key already has an active/earlier sibling.
                    all_rows = await conn.fetch("SELECT * FROM tasks")
                    lane_blocked = lane_blocked_ready_ids(
                        [self._row_to_task(r) for r in all_rows])

                    ready_tasks = [
                        self._row_to_task(r)
                        for r in await conn.fetch(
                            "SELECT * FROM tasks WHERE status = $1 "
                            "ORDER BY priority, created_at",
                            TaskStatus.ready.value,
                        )
                        if r["id"] not in leased and r["id"] not in lane_blocked
                    ]

                    for task in ready_tasks:
                        node = self._fair_scheduler.choose(
                            task.requires, online_nodes, active_counts,
                        )
                        if node is None:
                            continue

                        # Status-guarded write (round-2 audit): the snapshot
                        # above and this UPDATE sit under the schedule
                        # advisory lock, but a DIFFERENT writer path — a
                        # cancel or terminal set_task_status on another node
                        # — does not take that lock, so without the guard a
                        # task cancelled between snapshot and UPDATE would be
                        # resurrected to running here. SQLite could not hit
                        # this (one process RLock serialised schedule and
                        # cancel); Postgres multi-node can. A guard miss
                        # makes this writer a no-op — exactly the shape used
                        # by create_task/trigger_pending.
                        status = await conn.execute(
                            """UPDATE tasks SET status = $1, assigned_to = $2,
                               updated_at = $3, version = version + 1
                               WHERE id = $4 AND status = $5""",
                            TaskStatus.running.value, node.id, now, task.id,
                            TaskStatus.ready.value,
                        )
                        if status == "UPDATE 0":
                            continue
                        active_counts[node.id] = active_counts.get(node.id, 0) + 1
                        self._fair_scheduler.mark_picked(node.id)

                        await conn.execute(
                            """INSERT INTO scheduling_decisions
                               (task_id, task_title, priority, node_id, score, reason, timestamp)
                               VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                            task.id, task.title, task.priority, node.id,
                            1.0, "least_loaded_capability_match", _utcnow(),
                        )
                        new_assignments.append({
                            "task_id": task.id,
                            "task_title": task.title,
                            "node_id": node.id,
                            "priority": task.priority,
                        })
        return new_assignments

    def _row_to_decision(self, row) -> SchedulingDecision:
        ts = row["timestamp"]
        return SchedulingDecision(
            task_id=row["task_id"], task_title=row["task_title"],
            priority=row["priority"], node_id=row["node_id"],
            score=row["score"], reason=row["reason"],
            timestamp=(ts.replace(tzinfo=None) if ts and ts.tzinfo else ts)
            or datetime.utcnow(),
        )

    # -------------------------------------------------------------------
    # Federation registry
    # -------------------------------------------------------------------

    async def register_federation_cluster(
        self, cluster_id: str, name: str, endpoint: str
    ) -> RemoteCluster:
        now = _utcnow()
        await self._fetch(
            """INSERT INTO federation_clusters
               (id, name, endpoint, status, registered_at, last_ping, ping_latency)
               VALUES ($1, $2, $3, $4, $5, $5, 0.0)
               ON CONFLICT (id) DO UPDATE SET
                 name = EXCLUDED.name,
                 endpoint = EXCLUDED.endpoint,
                 status = EXCLUDED.status,
                 registered_at = EXCLUDED.registered_at,
                 last_ping = EXCLUDED.last_ping,
                 ping_latency = 0.0""",
            cluster_id, name, endpoint,
            FederationClusterStatus.available.value, now,
        )
        return RemoteCluster(
            id=cluster_id, name=name, endpoint=endpoint,
            status=FederationClusterStatus.available,
            registered_at=now.replace(tzinfo=None), last_ping=now.replace(tzinfo=None),
        )

    async def remove_federation_cluster(self, cluster_id: str) -> bool:
        n = await self._exec_status_rowcount(
            "DELETE FROM federation_clusters WHERE id = $1", cluster_id,
        )
        return n > 0

    async def get_federation_clusters(self) -> List[RemoteCluster]:
        rows = await self._all("SELECT * FROM federation_clusters")
        return [self._row_to_cluster(r) for r in rows]

    async def get_federation_cluster(self, cluster_id: str) -> Optional[RemoteCluster]:
        row = await self._row(
            "SELECT * FROM federation_clusters WHERE id = $1", cluster_id,
        )
        return self._row_to_cluster(row) if row else None

    def _row_to_cluster(self, row) -> RemoteCluster:
        def _naive(dt):
            return dt.replace(tzinfo=None) if dt and dt.tzinfo else dt
        return RemoteCluster(
            id=row["id"], name=row["name"], endpoint=row["endpoint"],
            status=FederationClusterStatus(row["status"]),
            registered_at=_naive(row["registered_at"]) or datetime.utcnow(),
            last_ping=_naive(row["last_ping"]) or datetime.utcnow(),
            ping_latency=row["ping_latency"],
        )

    # -------------------------------------------------------------------
    # Hook manager
    # -------------------------------------------------------------------

    async def register_hook(self, url: str, events: List[EventType], secret: str = "") -> Hook:
        hook_id = f"hook_{secrets.token_hex(8)}"
        now = _utcnow()
        await self._fetch(
            """INSERT INTO hooks (id, url, events, secret, active, created_at, updated_at)
               VALUES ($1, $2, $3, $4, 1, $5, $5)""",
            hook_id, url, _json_dumps([e.value for e in events]),
            secret or None, now,
        )
        return Hook(
            id=hook_id, url=url, events=events, secret=secret or None,
            active=True, created_at=now.replace(tzinfo=None),
            updated_at=now.replace(tzinfo=None),
        )

    async def deregister_hook(self, hook_id: str) -> bool:
        n = await self._exec_status_rowcount(
            "DELETE FROM hooks WHERE id = $1", hook_id,
        )
        return n > 0

    async def list_hooks(self) -> List[Hook]:
        rows = await self._all("SELECT * FROM hooks")
        hooks = []
        for row in rows:
            h = self._row_to_hook(row)
            hooks.append(h.model_copy(update={"secret": None}))
        return hooks

    async def get_hook_deliveries(self, hook_id: str) -> List[Delivery]:
        rows = await self._all(
            "SELECT * FROM deliveries WHERE hook_id = $1", hook_id,
        )
        return [self._row_to_delivery(r) for r in rows]

    async def add_delivery(self, delivery: Delivery) -> None:
        async with self._txn() as conn:
            await conn.execute(
                """INSERT INTO deliveries (id, hook_id, event_type, payload, status, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (id) DO UPDATE SET
                     hook_id = EXCLUDED.hook_id,
                     event_type = EXCLUDED.event_type,
                     payload = EXCLUDED.payload,
                     status = EXCLUDED.status,
                     created_at = EXCLUDED.created_at""",
                delivery.id, delivery.hook_id, delivery.event_type,
                _json_dumps(delivery.payload),
                delivery.status.value if hasattr(delivery.status, "value")
                else str(delivery.status),
                _aware(delivery.created_at),
            )
            # Trim as a sequential statement in the SAME transaction (the
            # same-table CTE form has undefined visibility — see record_decision).
            await conn.execute(
                """DELETE FROM deliveries
                   WHERE id NOT IN (
                       SELECT id FROM deliveries
                       ORDER BY created_at DESC NULLS LAST, id DESC LIMIT $1
                   )""",
                self._max_deliveries,
            )

    def _row_to_hook(self, row) -> Hook:
        def _naive(dt):
            return dt.replace(tzinfo=None) if dt and dt.tzinfo else dt
        return Hook(
            id=row["id"], url=row["url"],
            events=[EventType(e) for e in _json_loads(row["events"])],
            secret=row["secret"], active=bool(row["active"]),
            created_at=_naive(row["created_at"]) or datetime.utcnow(),
            updated_at=_naive(row["updated_at"]) or datetime.utcnow(),
        )

    def _row_to_delivery(self, row) -> Delivery:
        ts = row["created_at"]
        return Delivery(
            id=row["id"], hook_id=row["hook_id"],
            event_type=row["event_type"],
            payload=_json_loads(row["payload"]),
            status=row["status"],
            created_at=(ts.replace(tzinfo=None) if ts and ts.tzinfo else ts)
            or datetime.utcnow(),
        )

    # -------------------------------------------------------------------
    # Agent executor spawn tracking
    # -------------------------------------------------------------------

    async def record_task_spawn(
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
        await self._fetch(
            """INSERT INTO task_spawns
               (task_id, mode, job_id, pid, started_at, lease_id, lane_name,
                result_path, lane_key, role, session_id, attempt)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
               ON CONFLICT (task_id) DO UPDATE SET
                 mode = EXCLUDED.mode, job_id = EXCLUDED.job_id,
                 pid = EXCLUDED.pid, started_at = EXCLUDED.started_at,
                 lease_id = EXCLUDED.lease_id, lane_name = EXCLUDED.lane_name,
                 result_path = EXCLUDED.result_path, lane_key = EXCLUDED.lane_key,
                 role = EXCLUDED.role, session_id = EXCLUDED.session_id,
                 attempt = EXCLUDED.attempt""",
            task_id, mode, job_id, pid, started_at, lease_id, lane_name,
            result_path, lane_key, role, session_id, attempt,
        )

    async def get_task_spawn(self, task_id: str) -> Optional[dict]:
        row = await self._row(
            "SELECT * FROM task_spawns WHERE task_id = $1", task_id,
        )
        return dict(row) if row else None

    async def get_all_task_spawns(self) -> List[dict]:
        rows = await self._all("SELECT * FROM task_spawns")
        return _rows_to_dicts(rows)

    async def delete_task_spawn(self, task_id: str) -> bool:
        n = await self._exec_status_rowcount(
            "DELETE FROM task_spawns WHERE task_id = $1", task_id,
        )
        return n > 0

    # -------------------------------------------------------------------
    # Stateful lanes
    # -------------------------------------------------------------------

    async def record_lane(
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
        """Upsert a lane (first created_at always wins) — ATOMICALLY: the
        SQLite variant did SELECT-then-REPLACE inside its lock; here the
        conflict-target expression COALESCEs the stored created_at, so a
        concurrent writer cannot make created_at regress."""
        now = time.time()
        await self._fetch(
            """INSERT INTO lanes
               (lane_key, session_id, profile, role, node, created_at,
                last_active_at, last_task_id)
               VALUES ($1, $2, $3, $4, $5, COALESCE($6, EXTRACT(EPOCH FROM now())), $7, $8)
               ON CONFLICT (lane_key) DO UPDATE SET
                 session_id = EXCLUDED.session_id,
                 profile = EXCLUDED.profile,
                 role = EXCLUDED.role,
                 node = EXCLUDED.node,
                 created_at = COALESCE(lanes.created_at, EXCLUDED.created_at),
                 last_active_at = EXCLUDED.last_active_at,
                 last_task_id = EXCLUDED.last_task_id""",
            lane_key, session_id, profile, role, node, created_at,
            last_active_at if last_active_at is not None else now, last_task_id,
        )

    async def get_lane(self, lane_key: str) -> Optional[dict]:
        row = await self._row(
            "SELECT * FROM lanes WHERE lane_key = $1", lane_key,
        )
        return dict(row) if row else None

    async def get_all_lanes(self) -> List[dict]:
        rows = await self._all("SELECT * FROM lanes")
        return _rows_to_dicts(rows)

    async def touch_lane_last_task(self, lane_key: str, task_id: str) -> None:
        if not lane_key:
            return
        await self._fetch(
            "UPDATE lanes SET last_task_id = $1 WHERE lane_key = $2",
            task_id, lane_key,
        )

    async def delete_lane(self, lane_key: str) -> bool:
        n = await self._exec_status_rowcount(
            "DELETE FROM lanes WHERE lane_key = $1", lane_key,
        )
        return n > 0

    # -------------------------------------------------------------------
    # Config
    # -------------------------------------------------------------------

    async def get_config(self) -> Optional[Dict[str, Any]]:
        row = await self._row(
            'SELECT value FROM kv_store WHERE "key" = $1', "cluster_config",
        )
        if row is None:
            return self._config
        self._config = _json_loads(row["value"])
        return self._config

    async def set_config(self, config: Dict[str, Any]) -> None:
        self._config = config
        await self._fetch(
            """INSERT INTO kv_store ("key", value) VALUES ('cluster_config', $1)
               ON CONFLICT ("key") DO UPDATE SET value = EXCLUDED.value""",
            _json_dumps(config),
        )

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------

    async def get_summary(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "role": self.node_role,
            "nodes": {"total": await self.node_count(),
                      "online": await self.online_count()},
            "tasks": await self.task_counts(),
            "leases": {"active": len(await self.get_active_leases())},
            "sync_version": await self.sync_version(),
            "uptime_seconds": int((datetime.utcnow() - self.started_at).total_seconds()),
        }


# ---------------------------------------------------------------------------
# Sync facade — so the existing sync call sites (routers, managers, the store
# test-suite) can drive PostgresClusterStore WITHOUT being async: every public
# coroutine is submitted to a dedicated event-loop thread and awaited there,
# presenting exactly the ClusterStore (SQLite) API. This is what
# state.factory.create_store returns for backend=postgres.
# ---------------------------------------------------------------------------

class _LoopThread:
    """A private asyncio loop running on its own daemon thread."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self.loop.run_forever, name="pg-cluster-store", daemon=True
        )
        self._thread.start()

    def run(self, coro, timeout: Optional[float] = 120.0):
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=10)


class SyncPostgresStore:
    """Drop-in sync API over PostgresClusterStore (see module docstring).

    Async coroutines are bridged through a dedicated loop thread; plain
    attributes (cluster_id, node_id, node_role, started_at, callbacks)
    delegate straight to the underlying store, mirroring how ClusterState /
    ClusterStore are configured by app.py.
    """

    _SKIP_ATTRS = {"_store", "_bridge"}

    def __init__(self, dsn: str, *, connect_timeout: float = 30.0):
        store = PostgresClusterStore(dsn=dsn)
        bridge = _LoopThread()
        object.__setattr__(self, "_store", store)
        object.__setattr__(self, "_bridge", bridge)
        bridge.run(store.connect(), timeout=connect_timeout)

    # -- attribute delegation ------------------------------------------

    def __getattr__(self, name: str):
        if name in self._SKIP_ATTRS:
            raise AttributeError(name)
        store = object.__getattribute__(self, "_store")
        attr = getattr(store, name)
        if asyncio.iscoroutinefunction(attr):
            bridge = object.__getattribute__(self, "_bridge")

            def _sync_caller(*args, **kwargs):
                return bridge.run(attr(*args, **kwargs))

            _sync_caller.__name__ = name
            return _sync_caller
        return attr

    def __setattr__(self, name: str, value: Any):
        if name in self._SKIP_ATTRS:
            object.__setattr__(self, name, value)
            return
        setattr(object.__getattribute__(self, "_store"), name, value)

    # -- lifecycle -------------------------------------------------------

    def close(self) -> None:
        store = object.__getattribute__(self, "_store")
        bridge = object.__getattribute__(self, "_bridge")
        try:
            bridge.run(store.close(), timeout=15)
        finally:
            bridge.stop()

    # The watchdog adapter in cluster_core reaches into _conn/_lock only for
    # the SQLite store (guarded by hasattr); the PG store exposes set_node_status
    # which the adapter's ClusterState branch covers — advertise no _conn.
