"""One-shot SQLite -> Postgres cluster.db importer — shared/claude-plugins#829.

Copies every row of an existing ``cluster.db`` into the Postgres store so the
cutover loses nothing. Idempotent: re-running upserts by primary key
(BIGSERIAL tables preserve ids and re-sequence), so a partial import can be
safely retried and the importer can run while the old SQLite main is still
serving (do a final re-run at the cutover moment to pick up the delta).

Usage:
    python -m hermes_cluster.tools.import_sqlite --db C:/path/cluster.db

DSN resolution (NEVER pass a DSN on the command line — it embeds the password
and would land in shell history / logs):
    HERMES_CLUSTER_PG_DSN, else DATABASE_URL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# All 12 tables the SQLite store creates (enumerated from
# hermes_cluster/state/cluster_store.py::_SCHEMA_SQL + _migrate_schema, #829 —
# do not assume; this list matches the actual schema).
TABLES = [
    "nodes",
    "tasks",
    "lanes",
    "leases",
    "sync_log",
    "recovery_events",
    "scheduling_decisions",
    "federation_clusters",
    "hooks",
    "deliveries",
    "kv_store",
    "task_spawns",
]

# SQLite has no boolean type: a flag is stored as INTEGER 0/1, while the
# Postgres column is a real BOOLEAN. asyncpg is strict and refuses an int for a
# BOOLEAN parameter ("a boolean is required (got type int)"), so the import
# aborts on the first node row. Caught by CI on #907 — the local suite SKIPS
# these tests without a Postgres, so a green local run says nothing about this
# path. Any future boolean column must be added here.
_BOOL_COLUMNS = {
    "nodes": ["drained", "duplicate_executor"],
}

# ISO-datetime TEXT columns in SQLite -> TIMESTAMPTZ in Postgres.
_DT_COLUMNS = {
    "nodes": ["last_heartbeat"],
    "tasks": ["created_at", "updated_at"],
    "leases": ["created_at", "expires_at"],
    "recovery_events": ["timestamp"],
    "scheduling_decisions": ["timestamp"],
    "federation_clusters": ["registered_at", "last_ping"],
    "hooks": ["created_at", "updated_at"],
    "deliveries": ["created_at"],
}

_PK = {
    "nodes": "id",
    "tasks": "id",
    "lanes": "lane_key",
    "leases": "id",
    "sync_log": "id",
    "recovery_events": "id",
    "scheduling_decisions": "id",
    "federation_clusters": "id",
    "hooks": "id",
    "deliveries": "id",
    "kv_store": "key",
    "task_spawns": "task_id",
}


def _parse_dt(value: Any) -> Optional[datetime]:
    """SQLite stored datetime.utcnow().isoformat() (naive = UTC); empty -> None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _upsert_sql(table: str, columns: List[str]) -> str:
    quoted = ", ".join(
        f'"{c}"' if c == "key" else c for c in columns
    )
    placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != _PK[table]
    )
    pk = f'"{_PK[table]}"' if _PK[table] == "key" else _PK[table]
    if not updates:
        return f"INSERT INTO {table} ({quoted}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO NOTHING"
    return (
        f"INSERT INTO {table} ({quoted}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )


async def import_sqlite(db_path: str, dsn: str) -> Dict[str, int]:
    """Import every table from db_path into the Postgres store at dsn.

    Returns table -> row count imported.
    """
    import asyncpg

    counts: Dict[str, int] = {}
    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row

    existing = {
        r["name"]
        for r in src.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    try:
        # Ensure the target schema exists (idempotent) before writing.
        from ..state.postgres_store import _SCHEMA_SQL

        async with pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)

        for table in TABLES:
            if table not in existing:
                logger.info("import: %s absent in SQLite db — skipping", table)
                counts[table] = 0
                continue
            rows = src.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                counts[table] = 0
                continue
            columns = list(rows[0].keys())
            sql = _upsert_sql(table, columns)
            dt_cols = set(_DT_COLUMNS.get(table, []))
            bool_cols = set(_BOOL_COLUMNS.get(table, []))
            batch: List[tuple] = []
            for row in rows:
                vals: List[Any] = []
                for c in columns:
                    v = row[c]
                    if c in dt_cols:
                        vals.append(_parse_dt(v))
                    elif c in bool_cols:
                        # NULL stays NULL (column default applies); 0/1 -> bool.
                        vals.append(None if v is None else bool(v))
                    elif table == "sync_log" and c == "timestamp":
                        # BIGINT epoch in both backends — pass through.
                        vals.append(int(v or 0))
                    else:
                        vals.append(v)
                batch.append(tuple(vals))
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(sql, batch)
                    if table in ("sync_log", "scheduling_decisions"):
                        # BIGSERIAL ids preserved — keep the sequence ahead.
                        await conn.execute(
                            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                            f"(SELECT MAX(id) FROM {table}))"
                        )
            counts[table] = len(batch)
            logger.info("import: %s -> %d rows", table, len(batch))
    finally:
        await pool.close()
        src.close()
    return counts


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import a SQLite cluster.db into the Postgres ClusterStore (#829)"
    )
    parser.add_argument("--db", required=True, help="Path to the existing cluster.db")
    parser.add_argument(
        "--dsn-env",
        default="HERMES_CLUSTER_PG_DSN",
        help="Env var holding the asyncpg DSN (falls back to DATABASE_URL); "
        "a DSN is never accepted on the command line",
    )
    args = parser.parse_args(argv)

    dsn = os.environ.get(args.dsn_env, "") or os.environ.get("DATABASE_URL", "")
    if not dsn:
        # Do NOT print a DSN or any secret; only say where to put one.
        print(
            f"error: no DSN in {args.dsn_env} or DATABASE_URL",
            file=sys.stderr,
        )
        return 2

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    counts = asyncio.run(import_sqlite(args.db, dsn))
    total = sum(counts.values())
    print(f"imported {total} rows across {len(counts)} tables")
    for table, n in counts.items():
        print(f"  {table}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
