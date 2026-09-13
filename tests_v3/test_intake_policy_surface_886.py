"""Intake runtime-policy ENDPOINT surface — kept import-clean against main so
each red on bdaya/main is the defect's own assertion, not an ImportError
(lane standard from #868/#871 deliveries).

What these pin (all observable purely through the HTTP API, no new modules):

  main today: intake config is env-var-only. There is no policy endpoint, and
  an `intake:` section stored via the config API is DROPPED by a full-config
  PUT (ConfigJSON's drop-unknown-keys default). The fixes live in
  routers/intake.py + models (extra='allow'); this file proves they exist and
  behave.
"""

import os
import json
import pytest
from httpx import AsyncClient, ASGITransport

from hermes_cluster.app import create_app


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    yield


POLICY = {
    "enabled": True,
    "interval_seconds": 45,
    "scopes": [
        {"type": "group", "path": "invora", "enabled": True},
        {"type": "group", "path": "metaphor/bayader", "enabled": True},
        {"type": "group", "path": "metaphor/morshdy", "enabled": True},
    ],
    "priority": {"default": 3, "author_ids": {"12": 0, "8": 0}},
}


def _issue_event(iid, project="invora/invora-backend", author_id=12, labels=("bug",)):
    return {
        "object_kind": "issue",
        "project": {"id": 300, "path_with_namespace": project},
        "user": {"id": author_id, "username": "sami-hegazi"},
        "object_attributes": {"iid": iid, "title": f"issue {iid}", "action": "open"},
        "labels": [{"title": l} for l in labels],
    }


@pytest.mark.asyncio
async def test_policy_endpoints_exist(clean_env):
    """GET+PUT /api/v1/intake/gitlab/policy must exist (runtime control surface).

    RED ON MAIN: 405/404 — no such route today; intake config is env-only.
    """
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/api/v1/intake/gitlab/policy")
        assert r.status_code == 200, (
            "no runtime intake policy endpoint — configuration is env-var-only")
        assert "policy" in r.json()
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY)
        assert r.status_code == 200, "policy PUT must be accepted"
        assert r.json()["status"] == "saved"


@pytest.mark.asyncio
async def test_policy_survives_config_roundtrip(clean_env):
    """PUT /api/v1/config must not drop the stored intake section.

    RED ON MAIN: ConfigJSON (extra=ignore) model_dumps away unknown sections,
    so the round-trip wipes `intake` from the store. This test needs no new
    endpoints at all — it pins the models fix.
    """
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/api/v1/config")
        assert r.status_code == 200
        cfg = r.json()
        cfg["intake"] = {"gitlab": POLICY}
        r = await c.put("/api/v1/config", json=cfg)
        assert r.status_code == 200
        # Whatever read path exists must still see it: prefer the policy
        # endpoint if present, else the config GET.
        r2 = await c.get("/api/v1/config")
        assert r2.json().get("intake") == {"gitlab": POLICY}, (
            "full-config PUT silently dropped the unknown 'intake' section "
            "— a runtime policy stored through the same API cannot survive "
            "the dashboard's save button")


@pytest.mark.asyncio
async def test_webhook_business_author_band_zero_api(clean_env):
    """With a policy stored, Sami's (user id 12) issue enters at band 0.

    RED ON MAIN: policy cannot be stored at all (PUT /policy 405 -> setup
    assertion fails), and even if seeded via set_config the webhook ignores
    scopes and author rules — everything is band 3 behind the env-label gate.
    """
    app = create_app(cluster_id="t", node_id="n", node_role="main")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.put("/api/v1/intake/gitlab/policy", json=POLICY)
        assert r.status_code == 200, "cannot seed a policy without the runtime API"
        r = await c.post("/api/v1/intake/gitlab/webhook", json=_issue_event(601))
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "created", (
            f"sami's issue in an enabled group scope was not ingested: {data}")
        assert data["task"]["priority"] == 0, "business-team author must be band 0"
        # Out-of-scope project -> ignored (scope gating exists at all)
        r = await c.post("/api/v1/intake/gitlab/webhook",
                         json=_issue_event(602, project="nobody/nobody"))
        assert r.json()["status"] == "ignored"
