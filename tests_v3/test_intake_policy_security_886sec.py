"""Intake policy endpoint hardening — PR#36 security review findings 1 & 2.

Finding 1 (policy surface gated ONLY by a shared webhook secret, with no rate
limit, no audit trail, and — critically — the SAME secret that GitLab posts to
the public webhook): the fix separates the operator secret from the webhook
secret and adds a fail-closed, audited, rate-limited gate on POLICY WRITES.
Policy READS stay open: the response carries zero secrets (only a
token_present bool) and the operator-friendly control surface depends on
being able to fetch the current policy, edit it, and PUT it back.

Finding 2 (runtime-writable `endpoint` is the base URL for every GitLab API
call, so a policy-token leak becomes a GITLAB_INTAKE_TOKEN (real PAT)
exfiltration): endpoint is validated at WRITE time against a host allowlist
whose default contains ONLY the boot/env endpoint. The attacker-visible
surface cannot learn other hostnames (422 is generic), and the operator path
to an extra host is the authenticated config file seed — never the policy PUT.
"""

import os
import time

import pytest
from httpx import AsyncClient, ASGITransport

from hermes_cluster.app import create_app
from hermes_cluster.core import intake_policy
from hermes_cluster.routers import intake as intake_mod

POLICY = {
    "enabled": True,
    "interval_seconds": 45,
    "scopes": [{"type": "group", "path": "invora", "enabled": True}],
    "priority": {"default": 3, "author_ids": {"12": 0}},
}


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    """Full env isolation for every test here (create_app reads GITLAB_* at boot)."""
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    monkeypatch.setattr(intake_mod, "_poller", None)
    intake_mod._reset_policy_rate_limiter()
    yield
    # A test that exercises the limiter must not leak its window into the
    # next file's tests.
    intake_mod._reset_policy_rate_limiter()


# ---------------------------------------------------------------------------
# FINDING 1 — credential separation + fail-closed write gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_write_uses_separate_secret_not_webhook_secret(isolate):
    """The webhook secret must NOT open the privileged policy-write surface.

    A leak of GITLAB_INTAKE_WEBHOOK_SECRET (the secret GitLab itself posts
    to a public webhook) used to hand over intake configuration wholesale.
    After the fix the two credentials are DIFFERENT secrets, so:
      - webhook secret on a policy PUT  -> 401 (no more credential reuse)
      - operator policy secret          -> 200 (Sami's path stays plain curl)
      - and the two secrets are asserted (by length, never printed) to be
        distinct values.
    """
    assert len("webhook-s3cret") != len("operator-p0licy-token")  # no-accident guard
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "webhook-s3cret"
    os.environ["GITLAB_INTAKE_POLICY_SECRET"] = "operator-p0licy-token"
    # The two secrets MUST be distinct values — never print them, lengths only.
    assert os.environ["GITLAB_INTAKE_POLICY_SECRET"] != os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"]
    assert len(os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"]) == 14
    assert len(os.environ["GITLAB_INTAKE_POLICY_SECRET"]) == 21

    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # (a) webhook secret alone no longer writes the policy
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                        headers={"X-Gitlab-Token": "webhook-s3cret"})
        assert r.status_code == 401, (
            "webhook secret still opens the privileged policy write — "
            "credential reuse not fixed")
        # (b) the dedicated operator secret works — plain header, no HMAC,
        #     no engineer, no redeploy.
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                        headers={"X-Gitlab-Token": "operator-p0licy-token"})
        assert r.status_code == 200, "operator secret must open policy writes"
        assert r.json()["status"] == "saved"
        # (c) the WEBHOOK keeps its own secret — separation is not a swap.
        r = await c.post("/api/v1/intake/gitlab/webhook", json={
            "object_kind": "issue",
            "project": {"path_with_namespace": "invora/invora-backend"},
            "user": {"id": 12},
            "object_attributes": {"iid": 771, "title": "t", "action": "open"},
            "labels": [],
        }, headers={"X-Gitlab-Token": "webhook-s3cret"})
        assert r.status_code == 200, "webhook must still accept the webhook secret"


@pytest.mark.asyncio
async def test_policy_write_fails_closed_without_operator_secret(isolate):
    """With the webhook secret configured but NO dedicated policy secret,
    writes are DENIED — the old behavior (webhook secret doubles as operator
    token) is exactly the reuse finding. Reads may answer; writes must not."""
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "webhook-s3cret"
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                        headers={"X-Gitlab-Token": "webhook-s3cret"})
        assert r.status_code == 401, (
            "fail-open: policy writes were authenticated by the webhook secret")
        # And an anonymous write is denied too.
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY)
        assert r.status_code == 401


@pytest.mark.asyncio
async def test_policy_writes_are_rate_limited(isolate):
    """Burst of wrong-token writes must hit 429 — a bare public token gate
    is otherwise brute-forceable at unbounded speed."""
    os.environ["GITLAB_INTAKE_POLICY_SECRET"] = "operator-p0licy-token"
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        codes = []
        for i in range(60):
            r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                            headers={"X-Gitlab-Token": f"wrong-{i}"})
            codes.append(r.status_code)
        assert codes[0] == 401, "first bad attempt must be a clean 401"
        assert 429 in codes, (
            "no rate limiting — 60 token attempts all reached the compare")
        assert codes.count(429) > 30, "rate limiter never engaged"
        # A correct token after the limiter engaged is ALSO held off:
        # the limiter gates attempts, not just failures.
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                        headers={"X-Gitlab-Token": "operator-p0licy-token"})
        assert r.status_code == 429, "limiter must throttle the whole write path"


@pytest.mark.asyncio
async def test_policy_write_is_audited(isolate, caplog):
    """Every policy WRITE is audited with who-via-what / when / from-which IP,
    and the audit line NEVER contains a secret value (asserted by length and
    by explicit absence of the secret bytes)."""
    os.environ["GITLAB_INTAKE_POLICY_SECRET"] = "operator-p0licy-token"
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    with caplog.at_level(
            "INFO", logger="hermes_cluster.intake_policy_audit"):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                            headers={"X-Gitlab-Token": "operator-p0licy-token"})
            assert r.status_code == 200
    audit = [rec.getMessage() for rec in caplog.records
             if rec.name == "hermes_cluster.intake_policy_audit"]
    assert audit, "policy write produced no audit record"
    line = audit[-1]
    assert "allow" in line.lower(), "audit must record the decision"
    assert "127.0.0.1" in line, "audit must record the source IP (who/when/where)"
    assert time.strftime("%Y") in line, "audit must carry a timestamp"
    assert "operator-p0licy-token" not in line
    assert len(os.environ["GITLAB_INTAKE_POLICY_SECRET"]) == 21  # sanity, never printed
    # A denied attempt is audited too (that is the interesting one).
    caplog.clear()
    with caplog.at_level(
            "INFO", logger="hermes_cluster.intake_policy_audit"):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY,
                            headers={"X-Gitlab-Token": "nope"})
            assert r.status_code == 401
    audit = [rec.getMessage() for rec in caplog.records
             if rec.name == "hermes_cluster.intake_policy_audit"]
    assert audit and "deny" in audit[-1].lower(), "denied writes must be audited too"


# ---------------------------------------------------------------------------
# FINDING 2 — endpoint exfiltration surface
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_put_rejects_foreign_endpoint(isolate):
    """A policy PUT pointing intake at any host other than the boot/env
    endpoint must be REJECTED — otherwise the poller ships GITLAB_INTAKE_TOKEN
    (a real GitLab PAT) to the attacker's host. Generic 422: the error text
    must not enumerate allowed hosts (the surface itself cannot leak them)."""
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy",
                        json={**POLICY, "endpoint": "https://attacker.example"})
        assert r.status_code == 422, (
            "runtime-writable endpoint accepted an arbitrary host — PAT "
            "exfiltration vector")
        # And it must not have persisted.
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert "attacker.example" not in r.text
        # The boot endpoint itself is always writable (it is a no-op rewrite).
        r = await c.put("/api/v1/intake/gitlab/policy",
                        json={**POLICY, "endpoint": "https://gitlab.bdaya-dev.com"})
        assert r.status_code == 200


def test_endpoint_allowlist_defaults_to_env_only(isolate, monkeypatch):
    """The DEFAULT allowlist contains exactly the boot endpoint — no other
    hostname is reachable through the policy PUT, and the default is NOT
    wildcarded to *.bdaya-dev.com (a public subgroup would be enumerable).
    Extra hosts exist ONLY via the authenticated config-file seed."""
    monkeypatch.delenv("GITLAB_INTAKE_ENDPOINT", raising=False)
    monkeypatch.delenv("GITLAB_INTAKE_ALLOWED_ENDPOINTS", raising=False)
    hosts = intake_policy._allowed_endpoint_hosts(
        boot_default="https://gitlab.bdaya-dev.com")
    assert hosts == {"gitlab.bdaya-dev.com"}, (
        "default allowlist must be env-endpoint-only, got " + repr(sorted(hosts)))
    # File seed may add hosts (operator path via infra review, not the token).
    monkeypatch.setenv("GITLAB_INTAKE_ALLOWED_ENDPOINTS", "https://gitlab.other.test")
    hosts = intake_policy._allowed_endpoint_hosts(
        boot_default="https://gitlab.bdaya-dev.com")
    assert hosts == {"gitlab.bdaya-dev.com", "gitlab.other.test"}


def test_policy_endpoint_is_env_only_when_unbootstrapped(isolate):
    """Defense in depth: even a state dict hand-crafted around the validator
    cannot set an endpoint outside the allowlist — the field falls back to
    the boot default instead of persisting (env-only behavior at read time)."""
    p = intake_policy.GitLabIntakePolicy.model_validate(
        {"endpoint": "https://evil.test", "scopes": [{"type": "group", "path": "x"}]})
    assert p.endpoint != "https://evil.test"
    p2 = intake_policy.GitLabIntakePolicy.model_validate(
        {"endpoint": "https://gitlab.bdaya-dev.com"})
    assert p2.endpoint == "https://gitlab.bdaya-dev.com"


@pytest.mark.asyncio
async def test_poller_never_send_token_to_foreign_endpoint(isolate):
    """End-to-end oracle for the leak: drive the real poller against a mock
    that records the PRIVATE-TOKEN header per host. A policy that (somehow)
    carries a foreign endpoint must NOT produce a single request to it."""
    import httpx
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers.get("PRIVATE-TOKEN")))
        return httpx.Response(200, json=[])

    from hermes_cluster.state import ClusterState
    state = ClusterState()
    poller = intake_mod._GitLabPoller(
        state=state,
        defaults={"token": "PAT-SECRET-VALUE",
                  "endpoint": "https://gitlab.test",
                  "project": "p", "label": "", "interval": 30,
                  "requires": ["tooling"]},
        transport=httpx.MockTransport(handler),
    )
    # Store a foreign endpoint DIRECTLY (bypassing the write gate) — the
    # load-path sanitizer must neutralize it before the token ever moves.
    state.set_config({"intake": {"gitlab": {
        "enabled": True,
        "endpoint": "https://attacker.example",
        "scopes": [{"type": "group", "path": "invora", "enabled": True}],
    }}})
    await poller.poll_once()
    foreign = [(h, t) for h, t in seen if h == "attacker.example"]
    assert not foreign, (
        "poller sent the GitLab PAT to a non-allowlisted host: " + repr(foreign))
    assert seen and all(h == "gitlab.test" for h, _ in seen)
