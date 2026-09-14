"""Real-shape links tests for the grouped-intake DAG guard (#40 crash follow-up).

The live pod on main@097d7ea7 crashed EVERY 60s grouped cycle:

    File "/app/hermes_cluster/routers/intake.py", line 1235, in poll_once
      -> line 1150, in _grouped_cycle
      -> line 1043, in _held_by_open_blockers
    AttributeError: 'list' object has no attribute 'get'

while the status endpoint reported {"last_errors":{}} — the crash AND the
silence are both pinned here.

Fixture: tests_v3/fixtures/gitlab_links_real_shape.py — VERBATIM captured
from gitlab.bdaya-dev.com for invora/invora-flutter#465 (one is_blocked_by
link to open #462). GitLab's links endpoint returns a JSON ARRAY of linked
issues, each with a SCALAR `link_type` — not {"open":[...],"closed":[...]}
with `link_types` lists (the mock shape that let the old guard test pass).

RED PROOF (TDDD-1): on origin/main the real-shape tests fail with the live
traceback ('list' object has no attribute 'get'), and the alarm tests fail
because last_errors stays {} across a raising cycle — not with ImportError
(the guard is reached through poll_once, imported like every other intake
test).
"""

import asyncio
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import httpx

from fixtures.gitlab_links_real_shape import LINKS_RESPONSES
from hermes_cluster.state import ClusterState
from hermes_cluster.routers import intake as intake_mod

from test_intake_grouping_762 import _issue_json, _make_transport, _poller, _cfg, _grouping_policy  # noqa: E402


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    yield


def _links_handler(request: httpx.Request):
    """Serve the REAL captured payloads keyed by the issue the guard queried
    ('.../issues/<iid>/links'). Unknown iid -> [] like a linkless issue."""
    parts = request.url.path.rstrip("/").split("/")
    iid = parts[-2]
    for key, payload in LINKS_RESPONSES.items():
        if key.endswith(f"#{iid}"):
            return payload
    return []


def _grouping_routes(issue_iids):
    return {
        "/groups/invora/issues": [
            _issue_json(iid, f"issue {iid}", "invora/invora-flutter", 275)
            for iid in issue_iids
        ],
        "/merge_requests": lambda req: [],
        "/links": _links_handler,
    }


def _grouping_cfg():
    return _cfg([{"type": "group", "path": "invora", "enabled": True}],
                grouping=_grouping_policy())


# ---------------------------------------------------------------------------
# 1. THE CRASH: the guard must survive the REAL array shape (RED on main as
#    AttributeError: 'list' object has no attribute 'get' — the live crash).
# ---------------------------------------------------------------------------

def test_guard_survives_real_links_array_shape(clean_env, monkeypatch):
    """465 has an open is_blocked_by #462 OUTSIDE the bundle: the cycle must
    not raise, and 465 must be HELD (fail-closed DAG authority preserved)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _grouping_routes([465, 463, 464]), _grouping_cfg())
    res = asyncio.run(poller.poll_once())          # main: AttributeError
    assert res["created"], "cycle must still bundle the unblocked issues"
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert len(bundles) == 1
    members = bundles[0].issues
    assert "invora/invora-flutter#463" in members
    assert "invora/invora-flutter#464" in members
    assert "invora/invora-flutter#465" not in members, (
        "open out-of-batch blocker (real array shape, link_type "
        "'is_blocked_by') must hold its issue")


def test_guard_reads_scalar_link_type_from_real_shape(clean_env, monkeypatch):
    """A CLOSED out-of-batch blocker must NOT hold, read from the real shape
    (scalar `link_type`, items not split into open/closed buckets)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    real = LINKS_RESPONSES["invora/invora-flutter#465"][0]
    closed = dict(real)
    closed["state"] = "closed"
    closed["closed_at"] = "2026-09-10T00:00:00.000Z"

    def links(request):
        parts = request.url.path.rstrip("/").split("/")
        iid = parts[-2]
        if iid == "465":
            return [closed]
        return []

    routes = _grouping_routes([465, 463])
    routes["/links"] = links
    st = ClusterState()
    poller = _poller(st, routes, _grouping_cfg())
    asyncio.run(poller.poll_once())
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert bundles and "invora/invora-flutter#465" in bundles[0].issues, (
        "closed blocker must release the DAG hold")


# ---------------------------------------------------------------------------
# 2. THE SILENT FAILURE: last_errors must alarm while the poller stays alive.
# ---------------------------------------------------------------------------

def test_grouped_cycle_crash_is_recorded_in_last_errors(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _grouping_routes([465, 463, 464]), _grouping_cfg())

    boom = AttributeError("'list' object has no attribute 'get'")

    async def fake_grouped(*args, **kwargs):
        raise boom

    monkeypatch.setattr(poller, "_grouped_cycle", fake_grouped)
    asyncio.run(poller.poll_once())                # must NOT propagate

    errs = poller.last_errors
    assert "grouped_cycle" in errs, (
        f"a crashing cycle must alarm in last_errors, got {errs!r}")
    entry = errs["grouped_cycle"]
    assert isinstance(entry, dict)
    assert entry["type"] == "AttributeError"
    assert "list" in entry["message"]
    assert entry["count"] >= 1
    assert entry["timestamp"]


def test_error_count_increments_per_cycle(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _grouping_routes([465]), _grouping_cfg())

    async def fake_grouped(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(poller, "_grouped_cycle", fake_grouped)
    asyncio.run(poller.poll_once())
    asyncio.run(poller.poll_once())
    entry = poller.last_errors["grouped_cycle"]
    assert entry["count"] == 2, f"count must track repeat cycles: {entry!r}"


def test_recovered_cycle_clears_the_alarm(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _grouping_routes([465, 463]), _grouping_cfg())

    async def fake_grouped(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(poller, "_grouped_cycle", fake_grouped)
    asyncio.run(poller.poll_once())
    assert "grouped_cycle" in poller.last_errors
    monkeypatch.setattr(poller, "_grouped_cycle",
                        lambda *a, **k: asyncio.sleep(0))
    asyncio.run(poller.poll_once())
    assert "grouped_cycle" not in poller.last_errors, (
        "a clean cycle must clear the alarm")


def test_status_endpoint_surfaces_grouped_crash(clean_env, monkeypatch):
    """GET /api/v1/intake/gitlab/status must show the raising cycle — the
    live pod reported last_errors:{} on every 60s crash."""
    from httpx import AsyncClient, ASGITransport
    from hermes_cluster.app import create_app

    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _grouping_routes([465, 463, 464]), _grouping_cfg())

    async def fake_grouped(*args, **kwargs):
        raise AttributeError("'list' object has no attribute 'get'")

    monkeypatch.setattr(poller, "_grouped_cycle", fake_grouped)
    monkeypatch.setattr(intake_mod, "_poller", poller)

    async def go():
        app = create_app(cluster_id="t", node_id="n", node_role="main")
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://t") as c:
            await poller.poll_once()
            r = await c.get("/api/v1/intake/gitlab/status")
            assert r.status_code == 200
            return r.json()

    body = asyncio.run(go())
    errs = body["last_errors"]
    assert "grouped_cycle" in errs, f"status stayed quiet on a crash: {errs!r}"
    assert errs["grouped_cycle"]["type"] == "AttributeError"
    assert errs["grouped_cycle"]["count"] >= 1


def test_run_loop_records_poll_crash(clean_env, monkeypatch):
    """_run's outer guard must alarm too: the thread swallows a raising
    poll_once today, so even the manual-trigger-free loop could not be seen."""
    st = ClusterState()
    poller = _poller(st, {}, _grouping_cfg())

    async def boom_poll():
        raise ValueError("cycle exploded")

    monkeypatch.setattr(poller, "poll_once", boom_poll)

    class _StopAfterFirst:
        """_run's wait() is the loop timer: let exactly one cycle pass, then
        report 'stopped' so the while-guard ends the loop."""
        def __init__(self):
            self.calls = 0

        def is_set(self):
            return self.calls > 0

        def wait(self, timeout):
            self.calls += 1
            return True

    fake = _StopAfterFirst()
    real = poller._stop
    poller._stop = fake
    try:
        poller._run()
    finally:
        poller._stop = real
    entry = poller.last_errors.get("poll_cycle")
    assert entry, f"_run swallowed the exception silently: {poller.last_errors!r}"
    assert entry["type"] == "ValueError"
    assert fake.calls == 1


# ---------------------------------------------------------------------------
# 3. PARTIAL BUNDLE: a crash mid-cycle leaves NO task behind.
# ---------------------------------------------------------------------------

def test_crash_mid_cycle_leaves_no_bundle_task(clean_env, monkeypatch):
    """Grouping ON + the real-shape crash on the SECOND repo: the first
    repo's bundle task (already created) must be rolled back — never a
    half-published batch sitting 'ready' with no lane."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()

    routes = {
        "/groups/invora/issues": [
            _issue_json(463, "a", "invora/invora-flutter", 275),
            _issue_json(501, "b", "invora/invora-backend", 276),
        ],
        "/merge_requests": lambda req: [],
        "/links": _links_handler,
    }
    poller = _poller(st, routes, _grouping_cfg())

    real_mr_census = poller._open_mr_issue_refs

    async def census_then_crash(client, endpoint, project_path, ids):
        refs = await real_mr_census(client, endpoint, project_path, ids)
        if project_path == "invora/invora-backend":
            raise AttributeError("'list' object has no attribute 'get'")
        return refs

    monkeypatch.setattr(poller, "_open_mr_issue_refs", census_then_crash)
    # The guard itself must NOT crash on the flutter repo's real-shape array:
    # we want the crash AFTER the first bundle was created.
    asyncio.run(poller.poll_once())

    tasks = st.get_all_tasks()
    bundles = [t for t in tasks if getattr(t, "issues", None)]
    assert not bundles, (
        f"mid-cycle crash left a partial bundle behind: "
        f"{[(t.id, t.status, t.issues) for t in bundles]}")
    # Dedup must roll back too, or the rolled-back issue is intake-invisible
    # forever (created, then vanished from the store).
    assert "invora/invora-flutter#463" not in intake_mod._issue_dedup_to_task_id
