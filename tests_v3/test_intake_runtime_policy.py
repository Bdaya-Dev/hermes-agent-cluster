"""Runtime intake policy tests — hermes-factory-intake lane (P0, #886 program).

Pins the owner requirements as executable rules:
  (1) intake covers arbitrary PROJECT *and* GROUP scopes ("auto-work
      everything open"), resolved from the runtime store, not env vars;
  (2) priorities change at RUNTIME via PUT /intake/gitlab/policy — no
      restart, no redeploy;
  (3) an issue authored by a business-team user id (Sami=12, emad=8 on the
      live instance) is created at band 0 (top) and the scheduler is kicked
      immediately;
  (4) the legacy env-var hermes-factory path keeps working byte-for-byte when
      NO policy is configured (regression guard, brief design constraint c).

Separate by-design controls vs new behavior are noted at each test.
"""

import os
import json
import pytest
import httpx
from httpx import AsyncClient, ASGITransport

from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState
from hermes_cluster.routers import intake as intake_mod
from hermes_cluster.core.intake_policy import (
    GitLabIntakePolicy,
    IntakePriority,
    load_policy,
    policy_from_raw,
)


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    # Fresh dedup maps (module-level; tests must not leak ids into each other).
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    yield


# ---------------------------------------------------------------------------
# 1. Policy model + store round-trip (by-design: pure logic, also passes pre-change
#    ONLY for the model — the load/persist path is new)
# ---------------------------------------------------------------------------

def test_policy_band_resolution_author_wins():
    p = IntakePriority(default=3,
                       author_ids={12: 0, 8: 0},
                       project_paths={"invora": 1},
                       labels={"priority::p0": 1},
                       label_prefixes=["business-now"])
    # Author top priority outranks everything (owner requirement 3).
    assert p.band_for(author_id=12, project_path="anything", labels=[]) == 0
    assert p.band_for(author_id=8) == 0
    # Then project, then label, then prefix, then default.
    assert p.band_for(author_id=99, project_path="invora") == 1
    assert p.band_for(author_id=99, project_path="other", labels=["priority::p0"]) == 1
    assert p.band_for(author_id=99, labels=["business-now::q3"]) == 0
    assert p.band_for(author_id=99, labels=[]) == 3


def test_policy_rejects_out_of_band_values():
    with pytest.raises(ValueError):
        IntakePriority(default=9)
    with pytest.raises(ValueError):
        IntakePriority(author_ids={12: 7})


# ---------------------------------------------------------------------------
# 2. Runtime policy API — no restart, survives a config save
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_put_get_roundtrip(clean_env):
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # Unconfigured -> legacy defaults surface, configured False.
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert r.status_code == 200
        assert r.json()["configured"] is False

        body = {
            "enabled": True,
            "interval_seconds": 45,
            "scopes": [
                {"type": "project", "path": "shared/claude-plugins",
                 "label": "hermes-factory", "enabled": True},
                {"type": "group", "path": "invora", "enabled": True},
            ],
            "priority": {"default": 3, "author_ids": {"12": 0, "8": 0}},
        }
        r = await c.put("/api/v1/intake/gitlab/policy", json=body)
        assert r.status_code == 200
        assert r.json()["status"] == "saved"
        assert r.json()["scopes"] == 2

        r = await c.get("/api/v1/intake/gitlab/policy")
        got = r.json()
        assert got["configured"] is True
        assert got["policy"]["interval_seconds"] == 45
        assert {s["path"] for s in got["policy"]["scopes"]} == {
            "shared/claude-plugins", "invora"}

        # BAD policy -> 422, previous policy intact (validate-before-persist).
        r = await c.put("/api/v1/intake/gitlab/policy",
                        json={"priority": {"default": 99}})
        assert r.status_code == 422
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert r.json()["policy"]["interval_seconds"] == 45


@pytest.mark.asyncio
async def test_policy_survives_full_config_put(clean_env):
    """Dashboard 'save config' (PUT /api/v1/config, ConfigJSON-shaped) must not
    silently wipe the runtime intake section (extra-key drop regression)."""
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy",
                        json={"scopes": [{"type": "group", "path": "invora"}]})
        assert r.status_code == 200
        r = await c.get("/api/v1/config")
        cfg = r.json()
        cfg.setdefault("intake", {})  # echoes the GET'd config back verbatim
        r = await c.put("/api/v1/config", json=cfg)
        assert r.status_code == 200
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert r.json()["configured"] is True, (
            "PUT /api/v1/config dropped the unknown intake section — "
            "ConfigJSON must use extra='allow'")


# ---------------------------------------------------------------------------
# 3. Webhook: business-team author -> band 0 instantly; scopes gate ingest
# ---------------------------------------------------------------------------

def _issue_event(iid=501, title="Pay in full", action="open",
                 project="invora/invora-backend", author_id=12,
                 author="sami-hegazi", labels=("bug",)):
    return {
        "object_kind": "issue",
        "project": {"id": 300, "path_with_namespace": project},
        "user": {"id": author_id, "username": author},
        "object_attributes": {"iid": iid, "title": title, "action": action,
                              "url": f"https://gitlab.bdaya-dev.com/{project}/-/issues/{iid}"},
        "labels": [{"title": l} for l in labels],
    }


@pytest.mark.asyncio
async def test_webhook_business_author_band_zero(clean_env):
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy", json={
            "scopes": [{"type": "group", "path": "invora", "enabled": True}],
            "priority": {"default": 3, "author_ids": {"12": 0, "8": 0}},
        })
        assert r.status_code == 200
        # Sami (id 12) opens in invora group -> band 0
        r = await c.post("/api/v1/intake/gitlab/webhook", json=_issue_event())
        assert r.status_code == 200
        assert r.json()["status"] == "created"
        assert r.json()["task"]["priority"] == 0

        # Emad (id 8) -> also band 0
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(iid=502, author_id=8, author="emadsoliman"))
        assert r.json()["task"]["priority"] == 0

        # Other author in scope -> default band 3
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(iid=503, author_id=99, author="ahmed"))
        assert r.json()["task"]["priority"] == 3

        # Out of scope -> ignored, no task
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(iid=504, project="metaphor/morshdy/morshdy-backend"))
        assert r.json()["status"] == "ignored"
        assert "not in any enabled scope" in r.json()["reason"]


@pytest.mark.asyncio
async def test_webhook_legacy_mode_unchanged(clean_env, monkeypatch):
    """Design constraint (c): with NO policy, the env-label gate still rules."""
    monkeypatch.setenv("GITLAB_INTAKE_LABEL", "hermes-factory")
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(iid=505, labels=("bug",)))
        assert r.json()["status"] == "ignored"          # label gate holds
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(iid=506, labels=("hermes-factory",)))
        assert r.json()["status"] == "created"
        assert r.json()["task"]["priority"] == 3        # legacy band
        assert r.json()["task"]["requires"] == ["tooling"]  # #873 holds


@pytest.mark.asyncio
async def test_webhook_policy_enabled_false_kill_switch(clean_env):
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.put("/api/v1/intake/gitlab/policy", json={
            "enabled": False,
            "scopes": [{"type": "group", "path": "invora"}],
        })
        r = await c.post("/api/v1/intake/gitlab/webhook", json=_issue_event(iid=507))
        assert r.json()["status"] == "ignored"
        assert "disabled" in r.json()["reason"]


# ---------------------------------------------------------------------------
# 4. Poller: fan-out across project+group scopes, full dedup, per-issue band
# ---------------------------------------------------------------------------

def _issue_json(iid, title, project_path, project_id, author_id=99, labels=None):
    return {
        "iid": iid, "id": project_id * 100000 + iid, "title": title,
        "project_id": project_id,
        "references": {"full": f"{project_path}#{iid}"},
        "author": {"id": author_id, "username": "x"},
        "labels": list(labels or []),
        "state": "opened",
    }


def _make_transport(routes):
    """routes: {path_suffix: [issue dicts]} -> MockTransport serving /api/v4/...

    Matches on the RAW (still percent-encoded) path: httpx decodes %2F in
    request.url.path, which would make a project-path key like
    '/projects/shared%2Fclaude-plugins/issues' miss.
    """
    def handler(request: httpx.Request) -> httpx.Response:
        raw = request.url.raw_path.decode("ascii").split("?", 1)[0]
        for key, issues in routes.items():
            if raw.endswith(key):
                return httpx.Response(200, json=issues)
        return httpx.Response(404, json={"message": "404 Not Found"})
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_poller_group_fanout_and_full_dedup(clean_env):
    """The core new capability: one cycle walks project AND group scopes
    (group paths 404 on /projects/:id — measured live, 2026-09-13), assigns
    per-issue bands (business author -> 0), and dedups by project+iid so two
    projects sharing iid 420 produce two tasks."""
    routes = {
        "/projects/shared%2Fclaude-plugins/issues": [
            _issue_json(900, "legacy still works", "shared/claude-plugins", 1,
                        author_id=12, labels=["hermes-factory"]),
        ],
        "/groups/invora/issues": [
            _issue_json(420, "Sami urgent", "invora/invora-backend", 461, author_id=12),
            _issue_json(421, "someone else", "invora/invora-flutter", 462, author_id=99),
        ],
        "/groups/metaphor%2Fbayader/issues": [
            _issue_json(420, "collides-with invora iid", "metaphor/bayader/bayader-backend",
                        501, author_id=8),
        ],
    }
    state = ClusterState()
    poller = intake_mod._GitLabPoller(
        state=state,
        defaults={"token": "t", "endpoint": "https://gitlab.test",
                  "project": "shared%2Fclaude-plugins", "label": "hermes-factory",
                  "interval": 30, "requires": ["tooling"]},
        transport=_make_transport(routes),
    )
    # Policy via the RUNTIME store (same set_config the PUT endpoint uses).
    state.set_config({"intake": {"gitlab": {
        "enabled": True,
        "endpoint": "https://gitlab.test",
        "scopes": [
            {"type": "project", "path": "shared/claude-plugins",
             "label": "hermes-factory", "enabled": True},
            {"type": "group", "path": "invora", "enabled": True},
            {"type": "group", "path": "metaphor/bayader", "enabled": True},
        ],
        "priority": {"default": 3, "author_ids": {"12": 0, "8": 0},
                     "project_paths": {"invora/invora-flutter": 2}},
        "dedup_scope": "full",
    }}})

    res = await poller.poll_once()
    assert res["created"], "cycle created no tasks"
    tasks = {t.id: t for t in state.get_all_tasks()}

    by_title = {t.title: t for t in tasks.values()}
    assert by_title["[#420] Sami urgent"].priority == 0        # author band
    assert by_title["[#421] someone else"].priority == 2       # project band
    assert by_title["[#900] legacy still works"].priority == 0  # author band
    # iid 420 collision: invora + bayader both ingested, two distinct tasks.
    cands = [t for t in tasks.values() if t.title.startswith("[#420]")]
    assert len(cands) == 2, "iid 420 must dedup per-project, not globally"

    # Second cycle: nothing new (seen).
    res2 = await poller.poll_once()
    assert res2["created"] == []


@pytest.mark.asyncio
async def test_poller_group_path_404_on_project_endpoint_is_fatal_for_scope_only(clean_env):
    """A broken scope logs per-scope error and the others still ingest."""
    routes = {
        "/groups/invora/issues": [
            _issue_json(7, "ok from group", "invora/invora-backend", 461),
        ],
        # any /projects/... route 404s via the fallthrough.
    }
    state = ClusterState()
    poller = intake_mod._GitLabPoller(
        state=state,
        defaults={"token": "t", "endpoint": "https://gitlab.test",
                  "project": "does%2Fnot-exist", "label": "x", "interval": 30,
                  "requires": ["tooling"]},
        transport=_make_transport(routes),
    )
    state.set_config({"intake": {"gitlab": {
        "enabled": True, "endpoint": "https://gitlab.test",
        "scopes": [
            {"type": "project", "path": "does/not-exist", "enabled": True},
            {"type": "group", "path": "invora", "enabled": True},
        ],
    }}})
    res = await poller.poll_once()
    assert len(res["created"]) == 1
    assert poller.last_errors  # 404 recorded, but did not stop the group scope


@pytest.mark.asyncio
async def test_poller_legacy_cycle_unchanged(clean_env):
    """NO policy configured -> legacy single-project cycle, band 3, iid dedup
    (design constraint c regression guard)."""
    routes = {
        "/projects/shared%2Fclaude-plugins/issues": [
            _issue_json(11, "old path", "shared/claude-plugins", 1,
                        labels=["hermes-factory"]),
        ],
    }
    state = ClusterState()
    poller = intake_mod._GitLabPoller(
        state=state,
        defaults={"token": "t", "endpoint": "https://gitlab.test",
                  "project": "shared%2Fclaude-plugins", "label": "hermes-factory",
                  "interval": 30, "requires": ["tooling"]},
        transport=_make_transport(routes),
    )
    res = await poller.poll_once()
    assert len(res["created"]) == 1
    t = state.get_all_tasks()[0]
    assert t.priority == 3
    assert t.requires == ["tooling"]
    assert t.title == "[#11] old path"


# ---------------------------------------------------------------------------
# 5. Bootstrap: token env enables poller; policy `enabled` cannot resurrect
#    with no token at boot (documented bootstrap boundary, not runtime config)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_put_requires_token_when_secret_configured(clean_env, monkeypatch):
    """The policy surface sits on the peer-auth public list (operators cannot
    HMAC-sign), so it carries its own gate: X-Gitlab-Token must match
    GITLAB_INTAKE_WEBHOOK_SECRET once that secret is configured. This pins the
    auth contract its middleware comment promises."""
    monkeypatch.setenv("GITLAB_INTAKE_WEBHOOK_SECRET", "op-secret")
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY_BODY)
        assert r.status_code == 401
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert r.status_code == 401
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY_BODY,
                        headers={"X-Gitlab-Token": "op-secret"})
        assert r.status_code == 200


POLICY_BODY = {
    "scopes": [{"type": "group", "path": "invora", "enabled": True}],
    "priority": {"default": 3, "author_ids": {"12": 0}},
}


# ---------------------------------------------------------------------------
# 6. GitOps bootstrap seed: config FILE seeds an EMPTY store once, never
#    clobbers a runtime-updated policy (the no-restart ordering).
# ---------------------------------------------------------------------------

def test_config_file_seeds_only_empty_store(clean_env, tmp_path, monkeypatch):
    seed_yaml = tmp_path / "cluster.yaml"
    seed_yaml.write_text(
        "cluster:\n  id: x\n  role: main\n"
        "intake:\n  gitlab:\n    interval_seconds: 45\n"
        "    scopes:\n      - {type: group, path: invora, enabled: true}\n")
    state = ClusterState()
    state.set_config_path(str(seed_yaml))
    intake_mod._seed_policy_from_config_file(state)
    got = policy_from_raw(state.get_config())
    assert got and got["interval_seconds"] == 45, "file must seed an empty store"

    # Runtime PUT wins forever after: simulate a live update, then re-seed.
    cfg = state.get_config() or {}
    cfg["intake"]["gitlab"]["interval_seconds"] = 90
    state.set_config(cfg)
    intake_mod._seed_policy_from_config_file(state)
    assert policy_from_raw(state.get_config())["interval_seconds"] == 90, (
        "config file must NEVER clobber a runtime-updated policy")


def test_boot_poller_requires_token_env(clean_env, monkeypatch):
    monkeypatch.delenv("GITLAB_INTAKE_TOKEN", raising=False)
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    assert intake_mod._poller is None
    # Unroutable endpoint so the immediate first cycle fails locally instead
    # of hitting real GitLab from the test suite.
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "http://127.0.0.1:9")
    monkeypatch.setenv("GITLAB_INTAKE_TOKEN", "secret-token-present")
    try:
        app2 = create_app(cluster_id="t", node_id="n", node_role="main")
        assert intake_mod._poller is not None
    finally:
        if intake_mod._poller is not None:
            intake_mod._poller.stop()
        intake_mod._poller = None
