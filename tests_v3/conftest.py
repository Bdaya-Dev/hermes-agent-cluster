"""Shared fixtures for the store parity suite (#829).

Postgres availability guard: tests that need the Postgres backend call the
``pg_dsn`` fixture; if the env var is unset or no server answers, they SKIP.
CI (python-tests job) runs postgres:16 as a service and sets
HERMES_CLUSTER_PG_TEST_DSN, so the guard is satisfied there. Locally, point
it at a throwaway container (password lives in the shell env only — never in
the repo):

    docker run -d --name hermes-test-pg -e POSTGRES_PASSWORD=... \
      -e POSTGRES_DB=hermes_test -p 55432:5432 postgres:16-alpine
    export HERMES_CLUSTER_PG_TEST_DSN=postgresql://USER:***@localhost:55432/hermes_test
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _isolate_peer_auth_env():
    """Neutralize ambient PEER_TOKEN/PEER_TOKENS for every test (#872 hygiene).

    WHY: create_app() enables peer-auth whenever BOTH env vars are present
    (app.py: `_peer_auth_enabled = bool(_local_token) and bool(_peer_tokens_map)`),
    and a 401 then masks the response a test asserts on. The cluster main node
    exports exactly these variables, so any lane running the suite ON A WORKER
    (e.g. the #872 verifier) sees every API test fail 401 != 422 and reads
    that as the guard being broken. Measured: with the vars set, 40 tests in
    the API files fail; cleared, all pass. CI never set them, so this only
    bites on the fleet — the place the suite matters most.

    Test-level intent is preserved: fixtures that SET the vars (test_peer_auth)
    run after this one, and their own pops run before our restore, so the
    snapshot restore is a no-op for them.
    """
    saved = {k: os.environ.pop(k, None) for k in ("PEER_TOKEN", "PEER_TOKENS")}
    yield
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v


def _pg_available(dsn: str) -> bool:
    try:
        import asyncio

        import asyncpg

        async def _ping():
            conn = await asyncpg.connect(dsn, timeout=3)
            await conn.execute("SELECT 1")
            await conn.close()

        asyncio.run(_ping())
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    """DSN for the throwaway test database, or skip the whole test.

    CI-hardening (round-2 review): if HERMES_CLUSTER_PG_TEST_DSN IS set,
    the environment claims a server exists — an unreachable one is then a
    CI misconfiguration (dead service container, wrong port), not a
    "postgres unavailable locally" case, so FAIL loudly instead of
    skipping. A silently-skipping pg suite is worse than no suite: it
    would mark the Postgres leg green while never running it. Without the
    env var (local sqlite-only dev), skipping stays the correct behaviour.
    """
    dsn = os.environ.get("HERMES_CLUSTER_PG_TEST_DSN", "")
    if not dsn:
        pytest.skip(
            "Postgres unavailable (set HERMES_CLUSTER_PG_TEST_DSN; "
            "CI provides a postgres:16 service — see module docstring)"
        )
    if not _pg_available(dsn):
        pytest.fail(
            "HERMES_CLUSTER_PG_TEST_DSN is set but no server answers — "
            "the Postgres test leg would silently no-op. Check the CI "
            "service container (ci.yml: postgres:16, health-checked) "
            "or unset the variable for a sqlite-only local run."
        )
    return dsn


@pytest.fixture()
def pg_store(pg_dsn):
    """Fresh SyncPostgresStore with the schema truncated between tests."""
    from hermes_cluster.state.postgres_store import SyncPostgresStore

    store = SyncPostgresStore(dsn=pg_dsn)
    store.truncate_all()  # bridged coroutine (sync facade)
    yield store
    store.close()
