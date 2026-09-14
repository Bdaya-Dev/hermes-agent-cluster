"""#903 TDDD-1 fence: `last_errors` must be the LAST CYCLE's errors.

Measured (2026-09-14): three group scopes were corrected at 17:42Z
(type: group), `issues_seen` jumped +54 -> +908 (groups walking fine), yet
`GET /api/v1/intake/gitlab/status` kept listing `projects/invora/issues:
HTTP 404` every cycle after. Both the lead and a landing lane read the stale
entries as a LIVE failure — one blamed the freshly-deployed image.

The leak shape precisely: the 404 was recorded under the OLD scope key
(`projects/invora/issues`); the fixed scope is walked under a NEW key
(`groups/invora/issues`), whose success pops only its OWN key. Nothing ever
revisits the old key — `last_errors` is a lifetime accumulator despite its
name and its placement in the status payload.

Fix (this MR): `poll_once` rebuilds `last_errors` per cycle; `last_poll_ok`
and a bounded `error_history` (the accumulated view, capped at 50) are
exposed in status so the instrument's scream is preserved, just no longer
mislabeled as current.

RED at base: `test_changed_scope_key_does_not_leave_stale_error` (the
incident) and the `last_poll_ok`/`error_history` contract assertions.
GREEN with the fix, all legs.
"""
from __future__ import annotations

import asyncio

import httpx

from hermes_cluster.routers import intake as intake_mod
from hermes_cluster.routers.intake import _GitLabPoller
from hermes_cluster.state import ClusterState


def _defaults():
    return {
        "endpoint": "https://gitlab.test",
        "token": "t",
        "project": "invora/invora-backend",
        "label": "",
        "interval": 30,
        "requires": ["tooling"],
    }


def _poller_with(handler, state=None):
    return _GitLabPoller(state=state or ClusterState(), defaults=_defaults(),
                         transport=httpx.MockTransport(handler))


def _cycle(poller):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(poller.poll_once())
    finally:
        loop.close()


def _set_scopes(state, scopes):
    cfg = dict(state.get_config() or {})
    cfg["intake"] = {"gitlab": {"enabled": True, "scopes": scopes}}
    state.set_config(cfg)


# ------------------------------------------------------------- incident leg


def test_changed_scope_key_does_not_leave_stale_error():
    """THE measured failure, mechanically: cycle 1 walks a broken PROJECT
    scope (404 recorded under `projects/invora/issues`); the operator fixes
    the scope to a GROUP; cycle 2 walks `groups/invora/issues` healthy.
    Status must show NO live error — RED at base: the old key rides forever."""
    state = ClusterState()
    _set_scopes(state, [{"type": "project", "path": "invora", "enabled": True}])

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/projects/invora/issues" in url:
            return httpx.Response(404, json={"error": "Not Found"})
        # group walk: healthy, empty backlog
        return httpx.Response(200, json=[])

    poller = _poller_with(handler, state=state)
    _cycle(poller)
    assert poller.last_errors == {"projects/invora/issues": "HTTP 404"}
    assert poller.last_poll_ok is False

    # operator corrects the scope (the 17:42Z action from the issue).
    _set_scopes(state, [{"type": "group", "path": "invora", "enabled": True}])
    _cycle(poller)
    # RED at base: {'projects/invora/issues': 'HTTP 404'} survives although
    # every live scope is healthy. GREEN: rebuilt per cycle.
    assert poller.last_errors == {}
    assert poller.last_poll_ok is True
    # the scream is not LOST — accumulated trail holds it.
    assert [e["stage"] for e in poller.error_history] == ["projects/invora/issues"]


def test_same_scope_healthy_cycle_clears_like_base():
    """Regression pin (passes both sides): a scope fixed IN PLACE also clears."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(404, json={"error": "gone"})
        return httpx.Response(200, json=[])

    poller = _poller_with(handler)
    _cycle(poller)
    assert poller.last_errors == {"projects/invora/invora-backend/issues": "HTTP 404"}
    _cycle(poller)
    assert poller.last_errors == {}
    assert poller.last_poll_ok is True


def test_recurrence_still_alarms_next_cycle():
    """Per-cycle rebuild must not HIDE a scope that keeps failing."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "gone"})

    poller = _poller_with(handler)
    _cycle(poller)
    _cycle(poller)
    assert poller.last_errors == {"projects/invora/invora-backend/issues": "HTTP 404"}
    assert poller.last_poll_ok is False
    assert len(poller.error_history) == 2  # history accumulates what status no longer shows


# ------------------------------------------------------- status payload leg


def test_status_endpoint_exposes_the_new_contract():
    poller = _poller_with(lambda req: httpx.Response(200, json=[]))
    _cycle(poller)
    old = intake_mod._poller
    intake_mod._poller = poller
    try:
        loop = asyncio.new_event_loop()
        try:
            body = loop.run_until_complete(intake_mod.status())
        finally:
            loop.close()
    finally:
        intake_mod._poller = old
    assert body["configured"] is True
    assert body["last_poll_ok"] is True
    assert body["last_errors"] == {}
    assert isinstance(body["error_history"], list)


def test_history_is_bounded():
    poller = _poller_with(lambda req: httpx.Response(200, json=[]))
    for i in range(60):
        poller._alarm_history("stage", {"message": f"m{i}", "timestamp": "t"})
    assert len(poller.error_history) <= 50
    assert poller.error_history[-1]["message"] == "m59"  # newest retained
