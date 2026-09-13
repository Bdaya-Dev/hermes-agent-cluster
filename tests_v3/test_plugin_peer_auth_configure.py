"""plugin-only mode must configure peer-auth signing from env (else _api_call is unsigned → 401)."""
import os
from hermes_cluster import plugin
from hermes_cluster.core import peer_auth


def test_parse_peer_tokens_shape():
    assert plugin._parse_peer_tokens("a:1,b:2, c : 3 ,bad,:x") == {"a": "1", "b": "2", "c": "3"}


def test_configure_peer_auth_from_env(monkeypatch):
    monkeypatch.setenv("HERMES_CLUSTER_TOKEN", "tok-xyz")
    monkeypatch.setenv("HERMES_CLUSTER_NODE_ID", "node_a")
    monkeypatch.setenv("PEER_TOKENS", "node_a:tok-xyz,node_b:tok-b")
    monkeypatch.setenv("HERMES_CLUSTER_AUTO_START", "false")
    cfg = plugin._get_plugin_config()
    assert plugin._configure_peer_auth(cfg) is True
    assert peer_auth.is_configured()
    st = peer_auth.get_default_state()
    assert st.local_node_id == "node_a"
    hdrs = peer_auth.sign_request("GET", "/api/v1/tasks", b"")
    assert hdrs.get("X-Peer-Node") == "node_a" and hdrs.get("X-Peer-Signature")


def test_configure_peer_auth_noop_without_token(monkeypatch):
    monkeypatch.delenv("HERMES_CLUSTER_TOKEN", raising=False)
    # #893: _configure_peer_auth falls back to the fleet convention file
    # (~/.config/bdaya/hermes-peer-token, same as worker_connector). This test
    # asserts "no token ANYWHERE -> noop", so neutralize the file leg — on a
    # fleet member the file exists and would flip the answer for reasons the
    # test does not model (#872 ambient-state class).
    monkeypatch.setattr(plugin, "_peer_token_from_file", lambda: "")
    cfg = plugin._get_plugin_config()
    cfg["token"] = ""
    assert plugin._configure_peer_auth(cfg) is False


def test_configure_peer_auth_falls_back_to_fleet_token_file(monkeypatch):
    """#893: a worker with no explicit token signs with the fleet-convention
    file — zero env vars required (owner ruling 2026-09-13)."""
    monkeypatch.delenv("HERMES_CLUSTER_TOKEN", raising=False)
    monkeypatch.setattr(plugin, "_peer_token_from_file", lambda: "tok-from-file")
    cfg = plugin._get_plugin_config()
    cfg["token"] = ""
    cfg["node_id"] = "node_file"
    assert plugin._configure_peer_auth(cfg) is True
    from hermes_cluster.core import peer_auth
    st = peer_auth.get_default_state()
    assert st.local_node_id == "node_file"
    assert peer_auth.sign_request("GET", "/api/v1/tasks", b"").get("X-Peer-Node") == "node_file"


def test_register_configures_signing(monkeypatch):
    monkeypatch.setenv("HERMES_CLUSTER_TOKEN", "tok-reg")
    monkeypatch.setenv("HERMES_CLUSTER_NODE_ID", "node_reg")
    monkeypatch.setenv("HERMES_CLUSTER_AUTO_START", "false")

    class Ctx:
        def __init__(self): self.tools = []
        def register_tool(self, **kw): self.tools.append(kw["name"])
        def register_hook(self, *a, **kw): pass

    ctx = Ctx(); plugin.register(ctx)
    assert any(n.startswith("kanban_cluster_") for n in ctx.tools)
    assert peer_auth.get_default_state().local_node_id == "node_reg"


def test_register_sets_base_url_in_attach_mode(monkeypatch):
    monkeypatch.setenv("HERMES_CLUSTER_AUTO_START", "false")
    monkeypatch.setenv("HERMES_CLUSTER_PORT", "8787")
    monkeypatch.setenv("HERMES_CLUSTER_TOKEN", "tok-url")
    monkeypatch.setattr(plugin, "_base_url", "")

    class Ctx:
        def register_tool(self, **kw): pass
        def register_hook(self, *a, **kw): pass

    plugin.register(Ctx())
    assert plugin._base_url == "http://127.0.0.1:8787"
