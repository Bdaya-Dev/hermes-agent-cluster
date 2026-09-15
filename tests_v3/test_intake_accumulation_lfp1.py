"""LFP-1 completion: intake ACCUMULATION WINDOW (lane
cluster-lfp1-accumulation).

THE DEFECT THIS PINS. Grouped intake (#762) bundles what the planning pass
finds waiting, and it creates the bundle task the FIRST cycle that sees any
candidate. In steady state — issues arriving one or two per poll cycle across
a repo's day — the lane reaps a finished sitting, and the very next cycle
mints a NEW single-issue bundle for the one issue that arrived meanwhile.
Result: every steady-state issue becomes its own lane sitting with its own
review gate — the exact 1:1 shape #762 was filed to kill, just with the
"per issue" granularity moved from poll-time to sitting-time. The measured
2026-09-15 census: bundles of size 1 dominating the lane stream.

THE FIX THIS PINS. An ACCUMULATION WINDOW on band>0 bundles: keep a lane's
waiting candidates UNBUNDLED until the OLDEST waiting candidate has aged at
least ``grouping.accumulate_window_s`` seconds, then bundle the whole
accumulated batch in one sitting. Three exemptions, each load-bearing:

  (1) WINDOW OFF (0, the default) reproduces today's behavior byte-for-byte
      — kill-switch discipline like #762's own ``enabled`` default.
  (2) BAND 0 (business team, #886) NEVER waits for the window: Sami/emad
      stay instant.
  (3) A batch already at ``max_bundle_size`` goes NOW: the owner's per-
      sitting target size is met; holding more would only delay a full
      sitting, and capacity beyond the cap rides the NEXT window anyway.

Age anchors on GitLab's own ``created_at`` of the issue payload (UTC ISO,
the poller's existing fetch shape): stateless, restart-safe, and correct in
both directions — a pre-existing backlog issue is already old, so enabling
the window never strands the standing backlog behind it. A missing or
unparseable created_at fails OPEN (no hold): bad data must never silently
stop intake, the defect class #895/#903 trained us on.

RED PROOF (skill standard): the accumulation symbols do not exist on
origin/main, so the tests that pin them import INSIDE the test body and fail
on the missing behavior, not at module import of this file.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from hermes_cluster.state import ClusterState
from hermes_cluster.routers import intake as intake_mod

# Reuse the #762 harness verbatim — same fixture discipline, same routes.
from test_intake_grouping_762 import (  # noqa: E402
    _issue_json, _poller, _cfg, _grouping_policy, _AUTHOR_BANDS,
)


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    yield


def _iso(dt: datetime) -> str:
    """GitLab's payload spelling: UTC ISO with trailing Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _aged(iid, age_s, **kw):
    iss = _issue_json(iid, kw.pop("title", f"issue {iid}"),
                      kw.pop("project_path", "invora/invora-flutter"),
                      kw.pop("project_id", 275), **kw)
    iss["created_at"] = _iso(datetime.now(timezone.utc) - timedelta(seconds=age_s))
    return iss


# ---------------------------------------------------------------------------
# 1. Pure gate: the decision table, no I/O
# ---------------------------------------------------------------------------

def test_accumulate_gate_decision_table():
    from hermes_cluster.core.intake_grouping import (
        GroupingConfig, should_accumulate)
    w30 = GroupingConfig(enabled=True, accumulate_window_s=1800,
                         max_bundle_size=40)
    # Window off -> never hold (byte-for-byte today's behavior).
    off = GroupingConfig(enabled=True, accumulate_window_s=0)
    assert should_accumulate(batch_size=1, top_band=3, oldest_age_s=0.0,
                             config=off) is False
    assert should_accumulate(batch_size=1, top_band=3, oldest_age_s=None,
                             config=off) is False
    # Young band>0 batch under the cap -> HOLD and keep accumulating.
    assert should_accumulate(batch_size=1, top_band=3, oldest_age_s=60.0,
                             config=w30) is True
    assert should_accumulate(batch_size=12, top_band=3, oldest_age_s=1799.9,
                             config=w30) is True
    # Aged past the window -> release: bundle the whole accumulation.
    assert should_accumulate(batch_size=12, top_band=3, oldest_age_s=1800.0,
                             config=w30) is False
    # Unknown age (missing/unparseable created_at) -> FAIL OPEN, never a
    # silent stall of intake.
    assert should_accumulate(batch_size=1, top_band=3, oldest_age_s=None,
                             config=w30) is False
    # Band 0 (business team) NEVER waits for the window, any size, any age.
    assert should_accumulate(batch_size=1, top_band=0, oldest_age_s=0.0,
                             config=w30) is False
    # Cap-full batch goes NOW even young: the owner sitting target is met.
    full = GroupingConfig(enabled=True, accumulate_window_s=1800,
                          max_bundle_size=3)
    assert should_accumulate(batch_size=3, top_band=3, oldest_age_s=0.0,
                             config=full) is False
    assert should_accumulate(batch_size=2, top_band=3, oldest_age_s=0.0,
                             config=full) is True
    # Negative window is a config error, refused at the schema.
    with pytest.raises(Exception):
        GroupingConfig(enabled=True, accumulate_window_s=-1)


def test_parse_gitlab_time_shapes():
    from hermes_cluster.core.intake_grouping import parse_gitlab_time
    now = datetime.now(timezone.utc)
    assert parse_gitlab_time(_iso(now)).tzinfo is not None
    # offset spelling (no Z) still parses, naive is read as UTC
    assert parse_gitlab_time("2026-09-15T08:00:00") is not None
    for bad in (None, "", "not-a-time", 123, "2026-13-40T99:99:99Z"):
        assert parse_gitlab_time(bad) is None


# ---------------------------------------------------------------------------
# 2. Cycle behavior: young backlog holds, aged backlog bundles whole
# ---------------------------------------------------------------------------

def _routes(issues):
    return {
        "/groups/invora/issues": issues,
        "/merge_requests": lambda req: [],
        "/links": lambda req: [],
    }


def test_window_off_reproduces_legacy_bundle_immediately(clean_env, monkeypatch):
    """Control: default (no accumulate_window_s in the policy) bundles the
    young issues on the FIRST cycle exactly as before."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _routes([_aged(901, 5), _aged(902, 3)]), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    t = st.get_task(res["created"][0])
    # NEWEST FIRST (owner ruling 2026-09-15). #902 is aged 3s, #901 aged 5s,
    # so #902 leads. This test's subject is the window being OFF (bundle
    # immediately); the order is incidental to it but pinned so a future
    # ordering change cannot pass unnoticed.
    assert t.issues == ["invora/invora-flutter#902", "invora/invora-flutter#901"]


def test_window_holds_young_then_bundles_whole_accumulation(clean_env, monkeypatch):
    """The steady-state defect, end to end: three issues arriving over 90s
    inside a 1800s window produce ZERO lane sittings; once the OLDEST has
    aged past the window the whole accumulated batch rides ONE task."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = _routes([_aged(911, 30), _aged(912, 20), _aged(913, 10)])
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800)))
    res = asyncio.run(poller.poll_once())
    assert res["created"] == [], (
        "young steady-state backlog must not mint a sitting per arrival")
    # Same cycle again — still nothing (idempotent hold).
    assert asyncio.run(poller.poll_once())["created"] == []
    # The oldest issue crosses the window boundary (poller re-reads state
    # each cycle): ONE bundle, all three issues.
    #
    # NOTE the two different roles `created_at` plays here, which is easy to
    # conflate: the WINDOW is measured from the OLDEST issue (it is a proxy
    # for "how long has this window been open"), while the bundle's ORDER is
    # NEWEST FIRST (owner ruling 2026-09-15). So #911 is what releases the
    # hold, and #913 is what leads the bundle.
    routes["/groups/invora/issues"] = [_aged(911, 1801), _aged(912, 1791),
                                       _aged(913, 1781)]
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1, "aged backlog must bundle in ONE task"
    t = st.get_task(res["created"][0])
    assert t.issues == ["invora/invora-flutter#913", "invora/invora-flutter#912",
                        "invora/invora-flutter#911"]
    assert t.lane_key == "invora-flutter#env/dev"
    # The bundled trio is now blocked (one queued sitting per lane): a FRESH
    # young issue joins no bundle until the window ages it too.
    routes["/groups/invora/issues"].append(_aged(914, 5))
    assert asyncio.run(poller.poll_once())["created"] == []


def test_band0_never_waits_for_window(clean_env, monkeypatch):
    """#886 instant rule beats the accumulation window: a business-team
    issue arriving mid-window is bundled on its own IMMEDIATELY."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = _routes([_aged(921, 10),                       # band 3, young
                      _aged(922, 5, author_id=12)])         # sami, band 0
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800),
        priority=_AUTHOR_BANDS))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1, "band-0 must not be held by the window"
    t = st.get_task(res["created"][0])
    assert t.priority == 0
    assert t.issues == ["invora/invora-flutter#922"]
    # The young band-3 issue is NOT in it and stays unbundled.
    assert all("921" not in (x.issues or []) for x in st.get_all_tasks())


def test_cap_full_bundle_ignores_window(clean_env, monkeypatch):
    """Owner sitting-size target met -> no hold even when everything is
    young: 40 candidates arrive at once, one CAP-SIZED bundle fires now.

    The cap is 30 (owner ruling 2026-09-15, "a limit of 30 issues"); it was
    40 under the earlier "30-40" intent. The 10 surplus candidates ride the
    NEXT sitting — that is the documented behaviour of the exemption
    ("waiting longer only delays a full sitting while surplus arrivals ride
    the NEXT window anyway"), not a dropped-issue bug.
    """
    from hermes_cluster.core.intake_grouping import GroupingConfig

    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    cap = GroupingConfig().max_bundle_size
    # DISTINCT ages, descending: i=0 is the OLDEST (aged 100s), i=cap+9 the
    # NEWEST (aged 61s). All still young relative to the 1800s window, which
    # is the point — the cap exemption fires regardless of age.
    # (An earlier draft of this test used a uniform age for every issue,
    # which made "the newest ten" meaningless and the ordering assertion
    # unfalsifiable — it passed on id order alone.)
    issues = [_aged(1000 + i, 100 - i) for i in range(cap + 10)]
    poller = _poller(st, _routes(issues), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800)))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    t = st.get_task(res["created"][0])
    assert len(t.issues) == cap, (
        f"a full sitting must be exactly the cap ({cap}), not the candidate "
        f"count — got {len(t.issues)}")
    # Newest-first: the cap-sized batch takes the NEWEST candidates, so the
    # 10 surplus left behind are the OLDEST ten.
    bundled = {int(x.rsplit("#", 1)[1]) for x in t.issues}
    assert bundled == set(range(1000 + 10, 1000 + cap + 10)), (
        "the sitting must take the NEWEST cap-many issues")


def test_missing_created_at_fails_open(clean_env, monkeypatch):
    """Missing/junk created_at on the payload must bundle NOW (legacy shape),
    never stall intake forever behind an unknown age."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    a = _issue_json(931, "no ts", "invora/invora-flutter", 275)
    b = _issue_json(932, "junk ts", "invora/invora-flutter", 275)
    b["created_at"] = "tomorrow-ish"
    poller = _poller(st, _routes([a, b]), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800)))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    assert st.get_task(res["created"][0]).issues == [
        "invora/invora-flutter#931", "invora/invora-flutter#932"]


def test_backlog_older_than_window_bundles_immediately(clean_env, monkeypatch):
    """Enabling the window must never strand the standing backlog: issues
    created days ago are already aged -> first cycle bundles them."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    old = [_aged(941, 86400), _aged(942, 43200)]
    poller = _poller(st, _routes(old), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800)))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    assert len(st.get_task(res["created"][0]).issues) == 2


def test_two_repos_hold_independently(clean_env, monkeypatch):
    """The window is per lane: a young flutter issue holds while an aged
    backend issue bundles in the same cycle."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = _routes([
        _aged(951, 5, project_path="invora/invora-flutter", project_id=275),
        _aged(952, 3600, project_path="invora/invora-backend", project_id=276),
    ])
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(accumulate_window_s=1800)))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    t = st.get_task(res["created"][0])
    assert t.lane_key == "invora-backend#env/dev"
    assert t.issues == ["invora/invora-backend#952"]


# ---------------------------------------------------------------------------
# 3. Policy surface: the knob round-trips and validates at the write API
# ---------------------------------------------------------------------------

def test_window_roundtrips_through_policy_store(clean_env):
    from hermes_cluster.core.intake_policy import load_policy
    st = ClusterState()
    # (control) absent -> 0 (off) -> today's behavior.
    st.set_config({"intake": {"gitlab": {
        "endpoint": "https://gitlab.test", "scopes": [],
        "grouping": {"enabled": True}}}})
    assert load_policy(st).grouping.accumulate_window_s == 0
    st.set_config({"intake": {"gitlab": {
        "endpoint": "https://gitlab.test", "scopes": [],
        "grouping": {"enabled": True, "accumulate_window_s": 900}}}})
    assert load_policy(st).grouping.accumulate_window_s == 900


def test_window_write_api_rejects_negative(clean_env):
    from hermes_cluster.app import create_app
    from httpx import AsyncClient, ASGITransport
    app = create_app(cluster_id="t", node_id="n", node_role="main")

    async def go():
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://t") as c:
            r = await c.put("/api/v1/intake/gitlab/policy", json={
                "enabled": True,
                "scopes": [{"type": "group", "path": "invora"}],
                "grouping": {"enabled": True, "accumulate_window_s": -5}})
            assert r.status_code == 422
            r2 = await c.put("/api/v1/intake/gitlab/policy", json={
                "enabled": True,
                "scopes": [{"type": "group", "path": "invora"}],
                "grouping": {"enabled": True, "accumulate_window_s": 900}})
            assert r2.status_code == 200
    asyncio.run(go())
