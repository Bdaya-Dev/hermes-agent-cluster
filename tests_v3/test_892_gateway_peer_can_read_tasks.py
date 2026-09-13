import json
import time
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from hermes_cluster.app import create_app


@pytest.fixture
def main_app(tmp_path, monkeypatch):
    """A cluster main with peer auth ON and the gateway registered as a peer."""
    monkeypatch.setenv("PEER_TOKEN", "t" * 64)
    monkeypatch.setenv("PEER_TOKENS", "k8s_main:" + "t" * 64 + ",k8s_gateway:" + "t" * 64)
    app = create_app(cluster_id="c", node_id="k8s_main", node_role="main",
                     config_path="", fed_token="", static_dir=None)
    return app


@pytest.mark.asyncio
async def test_unsigned_list_denied(main_app):
    transport = ASGITransport(app=main_app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/api/v1/tasks")
    assert r.status_code == 401  # the blindness the gateway had


@pytest.mark.asyncio
async def test_gateway_peer_signed_list_accepted(main_app):
    """k8s_gateway (registered in PEER_TOKENS) may GET the task list — this is
    the exact call the chat plugin makes; proves in-cluster visibility is legal."""
    import hermes_cluster.core.peer_auth as pa
    pa.configure(local_node_id="k8s_gateway", local_token="t" * 64,
                 peer_tokens={"k8s_gateway": "t" * 64})
    hdrs = pa.sign_request("GET", "/api/v1/tasks", b"")
    assert hdrs["X-Peer-Node"] == "k8s_gateway"
    transport = ASGITransport(app=main_app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/api/v1/tasks", headers=hdrs)
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) or "tasks" in body  # the plugin handles both shapes
