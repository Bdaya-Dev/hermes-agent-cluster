"""#893: the cluster plugin's base URL resolves from cluster CONFIG, not loopback.

Blockers this locks:
  (1) _base_url was hard-bound to f"http://127.0.0.1:{port}" — from a worker
      every kanban_cluster_* addressed the worker's own process, never the
      hosted main. Resolution now comes from the cluster config file
      (cluster: endpoint: — the same key serve.py feeds the worker connector),
      defaulting to loopback ONLY when no config names an endpoint.
  (2) Nothing about this may come from an environment variable (owner ruling
      2026-09-13: "NOTHING should ever be configured from env vars").

These tests import plugin internals directly; they make no network calls.
"""
from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _no_ambient_fleet_home(tmp_path, monkeypatch):
    """Keep the resolver's fleet-convention home file out of tests that do
    not model it: on a fleet member ~/.config/bdaya/hermes-cluster.yaml is
    REAL (install-worker-profile.sh renders it) and would win the search
    order, turning 'loopback default' tests into false reds — the #872
    ambient-state class, same lesson."""
    from hermes_cluster.core import cluster_endpoint as ce
    empty_home = tmp_path / "no-home"
    empty_home.mkdir()
    monkeypatch.setattr(ce.Path, "home", classmethod(lambda cls: empty_home))


# ---------------------------------------------------------------------------
# core/cluster_endpoint.py — the config resolver
# ---------------------------------------------------------------------------

def _fresh_resolver(tmp_path: Path, monkeypatch):
    """Reload the resolver module with _REPO_ROOT pointed at tmp_path."""
    import importlib

    from hermes_cluster.core import cluster_endpoint as ce
    monkeypatch.setattr(ce, "_REPO_ROOT", tmp_path)
    return ce


def test_unset_config_yields_loopback_default(tmp_path, monkeypatch):
    ce = _fresh_resolver(tmp_path, monkeypatch)
    r = ce.resolve_cluster_endpoint(node_id="windows_desktop_worker")
    assert r["endpoint"] == ""
    assert not r["incomplete"]  # nothing configured -> single-node mode, not an error


def test_worker_file_endpoint_found_by_node_id(tmp_path, monkeypatch):
    ce = _fresh_resolver(tmp_path, monkeypatch)
    (tmp_path / "cluster-worker-desktop.yaml").write_text(
        "cluster:\n  id: bdaya_hermes_cluster\n  role: worker\n"
        '  endpoint: "https://hermes.bdaya-dev.com"\n'
        "node:\n  id: windows_desktop_worker\n", encoding="utf-8")
    r = ce.resolve_cluster_endpoint(node_id="windows_desktop_worker")
    assert r["endpoint"] == "https://hermes.bdaya-dev.com"
    assert r["node_id"] == "windows_desktop_worker"
    assert "cluster-worker-desktop.yaml" in r["source"]


def test_worker_file_matched_by_node_id_not_file_name(tmp_path, monkeypatch):
    """A file named *-pc.yaml whose node.id is NOT the requested one must not match."""
    ce = _fresh_resolver(tmp_path, monkeypatch)
    (tmp_path / "cluster-worker-pc.yaml").write_text(
        "cluster:\n  endpoint: http://100.107.212.67:8787\n"
        "node:\n  id: windows_pc_worker\n", encoding="utf-8")
    r = ce.resolve_cluster_endpoint(node_id="windows_desktop_worker")
    assert r["endpoint"] == ""  # the pc file must not answer for the desktop node


def test_explicit_path_wins(tmp_path, monkeypatch):
    ce = _fresh_resolver(tmp_path, monkeypatch)
    conf = tmp_path / "anywhere.yaml"
    conf.write_text('cluster:\n  endpoint: "http://main.local:8787/"\n', encoding="utf-8")
    r = ce.resolve_cluster_endpoint(explicit_path=str(conf))
    assert r["endpoint"] == "http://main.local:8787"  # trailing slash stripped


def test_fleet_home_file_is_in_the_search_order(tmp_path, monkeypatch):
    """~/.config/bdaya/hermes-cluster.yaml — rendered per machine by
    install-worker-profile.sh — answers even when no node_id is known."""
    ce = _fresh_resolver(tmp_path, monkeypatch)
    home = tmp_path / "home"
    (home / ".config" / "bdaya").mkdir(parents=True)
    (home / ".config" / "bdaya" / "hermes-cluster.yaml").write_text(
        "cluster:\n  id: bdaya_hermes_cluster\n  role: worker\n"
        '  endpoint: "https://hermes.bdaya-dev.com"\n'
        "node:\n  id: macbook_worker\n", encoding="utf-8")
    monkeypatch.setattr(ce.Path, "home", classmethod(lambda cls: home))
    r = ce.resolve_cluster_endpoint()
    assert r["endpoint"] == "https://hermes.bdaya-dev.com"
    assert r["node_id"] == "macbook_worker"


def test_non_http_endpoint_is_incomplete_not_guessed(tmp_path, monkeypatch):
    ce = _fresh_resolver(tmp_path, monkeypatch)
    conf = tmp_path / "cluster.yaml"
    conf.write_text('cluster:\n  endpoint: "hermes.bdaya-dev.com"\n', encoding="utf-8")
    r = ce.resolve_cluster_endpoint()
    assert r["endpoint"] == ""
    assert r["incomplete"] is True
    assert "not an http(s) URL" in r["why"]


def test_no_env_vars_read_by_resolver(tmp_path, monkeypatch):
    """Owner ruling: the endpoint MUST NOT come from an env var. The resolver's
    source must contain no os.environ reads at all."""
    src = (REPO_ROOT / "hermes_cluster" / "core" / "cluster_endpoint.py").read_text(encoding="utf-8")
    assert "environ" not in src
    assert "os.getenv" not in src
    assert "getenv" not in src


# ---------------------------------------------------------------------------
# plugin._ensure_base_url — integration of the resolver
# ---------------------------------------------------------------------------

@pytest.fixture()
def plugin_fresh(monkeypatch):
    """Import plugin with a clean module state per test."""
    import importlib

    monkeypatch.delenv("HERMES_CLUSTER_CONFIG", raising=False)
    for k in list(__import__("os").environ):
        if k.startswith("HERMES_CLUSTER_"):
            monkeypatch.delenv(k, raising=False)
    import hermes_cluster.plugin as plugin
    importlib.reload(plugin)
    return plugin


def test_ensure_base_url_defaults_loopback_with_no_config(plugin_fresh, monkeypatch, tmp_path):
    from hermes_cluster.core import cluster_endpoint as ce
    monkeypatch.setattr(ce, "_REPO_ROOT", tmp_path)
    plugin_fresh._base_url = ""
    base = plugin_fresh._ensure_base_url({"port": 8787})
    assert base == "http://127.0.0.1:8787"  # single-node behaviour unchanged


def test_ensure_base_url_resolves_remote_from_config(plugin_fresh, monkeypatch, tmp_path):
    from hermes_cluster.core import cluster_endpoint as ce
    (tmp_path / "cluster-worker-desktop.yaml").write_text(
        'cluster:\n  endpoint: "https://hermes.bdaya-dev.com"\n'
        "node:\n  id: windows_desktop_worker\n", encoding="utf-8")
    monkeypatch.setattr(ce, "_REPO_ROOT", tmp_path)
    plugin_fresh._base_url = ""
    cfg = {"node_id": "windows_desktop_worker", "port": 8787}
    base = plugin_fresh._ensure_base_url(cfg)
    assert base == "https://hermes.bdaya-dev.com"
    assert cfg["node_id"] == "windows_desktop_worker"


def test_ensure_base_url_explicit_endpoint_key(plugin_fresh, tmp_path):
    plugin_fresh._base_url = ""
    base = plugin_fresh._ensure_base_url({"endpoint": "http://other-main:8787"})
    assert base == "http://other-main:8787"


def test_ensure_base_url_bad_endpoint_sets_error_not_loopback(plugin_fresh, monkeypatch, tmp_path):
    from hermes_cluster.core import cluster_endpoint as ce
    monkeypatch.setattr(ce, "_REPO_ROOT", tmp_path)
    plugin_fresh._base_url = ""
    plugin_fresh._endpoint_error = ""
    base = plugin_fresh._ensure_base_url({"endpoint": "not-a-url"})
    assert base == ""
    assert plugin_fresh._endpoint_error  # loud, never a silent loopback fallback


def test_api_call_surfaces_endpoint_error(plugin_fresh):
    plugin_fresh._base_url = ""
    plugin_fresh._endpoint_error = "cluster endpoint config invalid"
    out = plugin_fresh._api_call("GET", "/api/v1/tasks")
    assert out == {"error": "cluster endpoint config invalid"}


def test_submit_triggers_schedule_after_create(plugin_fresh, monkeypatch):
    """A lane that submits and exits must not leave its task inert in ready."""
    calls = []

    def fake_api(method, path, data=None):
        calls.append((method, path, data))
        if path == "/api/v1/tasks":
            return {"id": "task_test123", "status": "pending"}
        return {"promoted": 1, "scheduled": 1}

    monkeypatch.setattr(plugin_fresh, "_api_call", fake_api)
    out = json.loads(plugin_fresh.handle_cluster_submit({
        "title": "review MR !42", "requires": ["review"],
        "role": "reviewer", "lane_key": "shared/claude-plugins!42",
    }))
    assert out["id"] == "task_test123"
    assert ("POST", "/api/v1/tasks", {
        "title": "review MR !42", "requires": ["review"], "priority": 3,
        "role": "reviewer", "lane_key": "shared/claude-plugins!42"}) in calls
    assert ("POST", "/api/v1/schedule/trigger", {}) in calls
    # trigger fires AFTER create
    paths = [c[1] for c in calls]
    assert paths.index("/api/v1/schedule/trigger") > paths.index("/api/v1/tasks")


def test_submit_no_trigger_on_error(plugin_fresh, monkeypatch):
    calls = []
    monkeypatch.setattr(plugin_fresh, "_api_call",
                        lambda m, p, d=None: (calls.append(p), {"error": "boom"})[1])
    out = json.loads(plugin_fresh.handle_cluster_submit({"title": "x"}))
    assert out == {"error": "boom"}
    assert calls == ["/api/v1/tasks"]  # no trigger after a failed create


def test_session_start_skips_local_server_on_remote_main(plugin_fresh, monkeypatch):
    """#890's failure mode: with a remote main configured, auto-start must NOT
    spawn a second, empty local cluster."""
    started = []
    monkeypatch.setattr(plugin_fresh, "_start_server", lambda cfg: started.append(cfg) or True)
    monkeypatch.setattr(plugin_fresh, "_ensure_base_url", lambda cfg: "https://hermes.bdaya-dev.com")
    plugin_fresh._on_session_start()
    assert started == []


def test_session_start_still_autostarts_on_loopback(plugin_fresh, monkeypatch):
    """Single-node behaviour unchanged: loopback base still auto-starts."""
    started = []
    monkeypatch.setattr(plugin_fresh, "_start_server", lambda cfg: started.append(cfg) or True)
    monkeypatch.setattr(plugin_fresh, "_ensure_base_url", lambda cfg: "http://127.0.0.1:8787")

    class FakeThread:
        def __init__(self, *a, **k): pass
        def start(self): started.append("thread")

    monkeypatch.setattr(plugin_fresh.threading, "Thread", FakeThread)
    plugin_fresh._on_session_start()
    assert started == ["thread"]


def test_register_reads_hermes_plugin_settings(plugin_fresh, monkeypatch):
    """register() must take endpoint/node_id from ctx.get_config (Hermes
    config file) — the config home per the owner ruling."""
    seen = {}

    def fake_ensure(cfg):
        seen.update(cfg)
        return "https://configured.main"

    monkeypatch.setattr(plugin_fresh, "_ensure_base_url", fake_ensure)
    monkeypatch.setattr(plugin_fresh, "_configure_peer_auth", lambda cfg: False)

    class Ctx:
        def get_config(self, key, default=None):
            return {"endpoint": "https://configured.main", "node_id": "n1"}.get(key, default)
        def register_tool(self, **kw): pass
        def register_hook(self, *a, **kw): pass

    plugin_fresh.register(Ctx())
    assert seen["endpoint"] == "https://configured.main"
    assert seen["node_id"] == "n1"


def test_register_survives_ctx_without_get_config(plugin_fresh, monkeypatch):
    """Fork unit-test Ctx stubs predate get_config; register must not crash."""
    monkeypatch.setattr(plugin_fresh, "_ensure_base_url", lambda cfg: "http://127.0.0.1:8787")
    monkeypatch.setattr(plugin_fresh, "_configure_peer_auth", lambda cfg: False)

    class LegacyCtx:
        tools = []
        def register_tool(self, **kw): self.tools.append(kw["name"])
        def register_hook(self, *a, **kw): pass

    plugin_fresh.register(LegacyCtx())
    assert any(t.startswith("kanban_cluster_") for t in LegacyCtx.tools)


def test_package_init_does_not_eagerly_import_fastapi():
    """#893: `import hermes_cluster.core.peer_auth` (stdlib-only, used by the
    plugin signer) must not drag fastapi/pydantic in through the package
    __init__ — that made every lane boot pay >10 s (or ImportError where the
    server deps are absent). create_app stays reachable (lazy)."""
    import subprocess
    import sys
    code = (
        "import sys; import hermes_cluster.core.peer_auth;"
        "assert 'fastapi' not in sys.modules, 'fastapi leaked via package __init__';"
        "import hermes_cluster; assert callable(hermes_cluster.create_app)"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       timeout=120)
    assert r.returncode == 0, r.stderr[-400:]
