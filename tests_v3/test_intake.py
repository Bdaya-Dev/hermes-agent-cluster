"""Tests for GitLab intake router."""

import os
import pytest
from httpx import AsyncClient, ASGITransport
from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState


@pytest.fixture
def app():
    os.environ.pop("GITLAB_INTAKE_TOKEN", None)
    os.environ.pop("GITLAB_INTAKE_WEBHOOK_SECRET", None)
    return create_app(cluster_id="test", node_id="test-node", node_role="main")


@pytest.mark.asyncio
async def test_webhook_creates_task(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "iid": 999,
                "title": "Test intake issue",
                "url": "https://gitlab.bdaya-dev.com/shared/claude-plugins/-/issues/999",
                "action": "open",
            },
            "labels": [{"title": "tooling"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "created"
        assert body["task_id"].startswith("task_")
        assert body["task"]["title"] == "[#999] Test intake issue"
        assert "tooling" in body["task"]["requires"]


@pytest.mark.asyncio
async def test_webhook_idempotent(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "iid": 888,
                "title": "Same issue",
                "url": "https://gitlab.bdaya-dev.com/shared/claude-plugins/-/issues/888",
                "action": "open",
            },
            "labels": [{"title": "tooling"}],
        }
        r1 = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert r1.json()["status"] == "created"
        r2 = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert r2.json()["status"] == "deduped"
        assert r1.json()["task_id"] == r2.json()["task_id"]


@pytest.mark.asyncio
async def test_webhook_ignores_wrong_label(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {"iid": 777, "title": "Nope", "action": "open"},
            "labels": [{"title": "bug"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.json()["status"] == "ignored"


@pytest.mark.asyncio
async def test_webhook_ignores_non_issue_event(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/v1/intake/gitlab/webhook", json={"object_kind": "merge_request"})
        assert resp.json()["status"] == "ignored"


@pytest.mark.asyncio
async def test_status_unconfigured(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/intake/gitlab/status")
        assert resp.status_code == 200
        assert resp.json()["configured"] is False


@pytest.mark.asyncio
async def test_webhook_secret_validation_correct():
    """Test that correct webhook secret is accepted."""
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "test-secret-123"
    try:
        app = create_app(cluster_id="test", node_id="test-node", node_role="main")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "object_kind": "issue",
                "object_attributes": {
                    "iid": 111,
                    "title": "Test with secret",
                    "action": "open",
                },
                "labels": [{"title": "tooling"}],
            }
            resp = await client.post(
                "/api/v1/intake/gitlab/webhook",
                json=payload,
                headers={"X-Gitlab-Token": "test-secret-123"}
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "created"
    finally:
        os.environ.pop("GITLAB_INTAKE_WEBHOOK_SECRET", None)


@pytest.mark.asyncio
async def test_webhook_secret_validation_wrong():
    """Test that wrong webhook secret is rejected with 401."""
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "test-secret-123"
    try:
        app = create_app(cluster_id="test", node_id="test-node", node_role="main")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "object_kind": "issue",
                "object_attributes": {
                    "iid": 222,
                    "title": "Test with wrong secret",
                    "action": "open",
                },
                "labels": [{"title": "tooling"}],
            }
            resp = await client.post(
                "/api/v1/intake/gitlab/webhook",
                json=payload,
                headers={"X-Gitlab-Token": "wrong-secret"}
            )
            assert resp.status_code == 401
            assert "Invalid webhook token" in resp.json()["detail"]
    finally:
        os.environ.pop("GITLAB_INTAKE_WEBHOOK_SECRET", None)


@pytest.mark.asyncio
async def test_webhook_secret_validation_missing():
    """Test that missing webhook secret is rejected with 401 when secret is configured."""
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "test-secret-123"
    try:
        app = create_app(cluster_id="test", node_id="test-node", node_role="main")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "object_kind": "issue",
                "object_attributes": {
                    "iid": 333,
                    "title": "Test without secret header",
                    "action": "open",
                },
                "labels": [{"title": "tooling"}],
            }
            resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
            assert resp.status_code == 401
            assert "Invalid webhook token" in resp.json()["detail"]
    finally:
        os.environ.pop("GITLAB_INTAKE_WEBHOOK_SECRET", None)


@pytest.mark.asyncio
async def test_webhook_iid_validation_missing(app):
    """Test that missing iid is rejected with 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "title": "Test without iid",
                "action": "open",
            },
            "labels": [{"title": "tooling"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.status_code == 400
        assert "Invalid or missing iid" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_webhook_iid_validation_non_integer(app):
    """Test that non-integer iid is rejected with 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "iid": "not-an-integer",
                "title": "Test with string iid",
                "action": "open",
            },
            "labels": [{"title": "tooling"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.status_code == 400
        assert "Invalid or missing iid" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Round-3 regression tests (R3-2, iid bool)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r3_2_non_ascii_token_returns_401_not_500():
    """R3-2: non-ASCII X-Gitlab-Token must return 401, not 500 (TypeError).

    hmac.compare_digest(str, str) raises TypeError on non-ASCII chars.
    Fix: compare as bytes so any token is safely handled.
    """
    os.environ["GITLAB_INTAKE_WEBHOOK_SECRET"] = "correct-secret"
    try:
        app = create_app(cluster_id="test", node_id="test-node", node_role="main")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "object_kind": "issue",
                "object_attributes": {
                    "iid": 444,
                    "title": "Test non-ASCII token",
                    "action": "open",
                },
                "labels": [{"title": "tooling"}],
            }
            # Send a token with latin-1 byte 0xE9 (é) — non-ASCII.
            # httpx can't send non-ASCII header values directly, so we pass
            # raw bytes via the header tuple form.
            non_ascii_token_bytes = b"forg\xe9d-token"
            resp = await client.post(
                "/api/v1/intake/gitlab/webhook",
                json=payload,
                headers=[(b"x-gitlab-token", non_ascii_token_bytes)],
            )
            # Must be 401 (auth rejected), NOT 500 (TypeError unhandled)
            assert resp.status_code == 401, \
                f"non-ASCII token must return 401, got {resp.status_code}"
            assert "Invalid webhook token" in resp.json()["detail"]
    finally:
        os.environ.pop("GITLAB_INTAKE_WEBHOOK_SECRET", None)


@pytest.mark.asyncio
async def test_iid_bool_rejected():
    """iid: bool must be rejected (True==1 would collide with real issue #1).

    type(iid) is int rejects bool; isinstance(True, int) is True (bool subclasses int).
    """
    app = create_app(cluster_id="test", node_id="test-node", node_role="main")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "iid": True,  # bool, not int
                "title": "Test with bool iid",
                "action": "open",
            },
            "labels": [{"title": "tooling"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.status_code == 400, \
            f"bool iid must be rejected with 400, got {resp.status_code}"
        assert "Invalid or missing iid" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_requires_is_independent_of_filter_label(monkeypatch):
    """#873: `requires` must name a CAPABILITY, never the ingest filter label.

    The deployed main node runs with GITLAB_INTAKE_LABEL=hermes-factory. No
    worker advertises that, so before the fix every ingested task was created
    `ready` and could never be claimed -- silently, because an unclaimable task
    is indistinguishable from a queued one. The pre-existing tests never caught
    it: their fixture leaves GITLAB_INTAKE_LABEL unset, so it defaulted to
    "tooling" and the assertion passed vacuously.
    """
    monkeypatch.setenv("GITLAB_INTAKE_LABEL", "hermes-factory")
    monkeypatch.delenv("GITLAB_INTAKE_REQUIRES", raising=False)
    monkeypatch.delenv("GITLAB_INTAKE_TOKEN", raising=False)
    monkeypatch.delenv("GITLAB_INTAKE_WEBHOOK_SECRET", raising=False)
    app = create_app(cluster_id="test", node_id="test-node", node_role="main")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {
                "iid": 8731,
                "title": "Filter label must not become a capability",
                "action": "open",
            },
            "labels": [{"title": "hermes-factory"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.status_code == 200
        requires = resp.json()["task"]["requires"]
        assert requires == ["tooling"], (
            f"requires={requires!r} -- the filter label leaked into the capability "
            "requirement; no worker advertises it, so the task is unclaimable"
        )


@pytest.mark.asyncio
async def test_requires_override_is_honoured(monkeypatch):
    """GITLAB_INTAKE_REQUIRES sets the capability, comma-separated."""
    monkeypatch.setenv("GITLAB_INTAKE_LABEL", "hermes-factory")
    monkeypatch.setenv("GITLAB_INTAKE_REQUIRES", "review, native-win")
    monkeypatch.delenv("GITLAB_INTAKE_TOKEN", raising=False)
    monkeypatch.delenv("GITLAB_INTAKE_WEBHOOK_SECRET", raising=False)
    app = create_app(cluster_id="test", node_id="test-node", node_role="main")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "object_kind": "issue",
            "object_attributes": {"iid": 8732, "title": "Override", "action": "open"},
            "labels": [{"title": "hermes-factory"}],
        }
        resp = await client.post("/api/v1/intake/gitlab/webhook", json=payload)
        assert resp.json()["task"]["requires"] == ["review", "native-win"]
