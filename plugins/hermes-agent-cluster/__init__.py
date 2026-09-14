"""hermes-cluster Hermes plugin — THIN SHIM over hermes_cluster.plugin (#893).

Hermes loads THIS file (plugins/hermes-agent-cluster/__init__.py is what
`hermes plugins install` drops into a profile). For the whole history of the
fork up to this fix, it was a full pre-#893 copy of hermes_cluster/plugin.py:
its own _api_call/_start_server/handle_* with the base URL hard-bound to
loopback, so PR#42's remote-endpoint/peer-signing fix to the runtime module
NEVER reached a worker session — every lane still talked to a local,
auto-started, empty cluster (measured on windows_desktop 2026-09-14:
kanban_cluster_status answered the local {cluster_id: hermes-cluster,
nodes.total: 0} while the hosted main answered
{cluster_id: bdaya_hermes_cluster, nodes.total: 3}).

This file now owns ZERO implementation: it re-exports the single canonical
plugin — register(), the nine tool handlers, the session hooks, schemas and
internals — from hermes_cluster.plugin, so a fix to the module is a fix to
the loadable plugin. Drift is pinned by
tests_v3/test_plugin_dir_shim_893.py and the manifest correlation by
tests_v3/test_plugin_manifest_correlation_893.py.

Deployment contract: hermes_cluster (the `hermes-cluster` distribution, see
pyproject.toml) must be importable in the Hermes runtime environment on any
node that enables this plugin. Inside a full repo checkout the path is added
automatically; after a standalone `plugins install` of just this directory,
the rollout must make the package available (pip install the distribution or
pin the checkout on sys.path — the same way fastapi/uvicorn from
plugin.yaml's pip_dependencies must be available).
"""
from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path

# Dev/checkout convenience: this file lives at <repo>/plugins/hermes-agent-cluster/,
# so when hermes_cluster sits next to it (full clone, worktree, red-proof run)
# make the repo root importable without any install step.
_REPO_ROOT = _Path(__file__).resolve().parents[2]
if (_REPO_ROOT / "hermes_cluster").is_dir() and str(_REPO_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPO_ROOT))

try:
    import hermes_cluster.plugin as _plugin
except ImportError as _e:  # pragma: no cover - deployment misconfiguration
    raise ImportError(
        "plugins/hermes-agent-cluster is a thin shim over the hermes-cluster "
        "python package (#893) — install the distribution (pyproject.toml in "
        "the repo root) into the Hermes runtime environment, or run from a "
        f"full repo checkout. Underlying error: {_e}"
    ) from _e

# The canonical surface, re-exported (not re-implemented):
from hermes_cluster.plugin import *  # noqa: F401,F403

# Star-import skips underscore names; Hermes and the drift guards bind these
# by name, so alias them explicitly against the same function objects.
_api_call = _plugin._api_call
_start_server = _plugin._start_server
_stop_server = _plugin._stop_server
_health_check = _plugin._health_check
_ensure_base_url = _plugin._ensure_base_url
_get_plugin_config = _plugin._get_plugin_config
_config_from_hermes_settings = _plugin._config_from_hermes_settings
_configure_peer_auth = _plugin._configure_peer_auth
_parse_peer_tokens = _plugin._parse_peer_tokens
_peer_token_from_file = _plugin._peer_token_from_file
_on_session_start = _plugin._on_session_start
_on_session_end = _plugin._on_session_end
handle_cluster_init = _plugin.handle_cluster_init
handle_cluster_join = _plugin.handle_cluster_join
handle_cluster_submit = _plugin.handle_cluster_submit
handle_cluster_list = _plugin.handle_cluster_list
handle_cluster_nodes = _plugin.handle_cluster_nodes
handle_cluster_heartbeat = _plugin.handle_cluster_heartbeat
handle_cluster_complete = _plugin.handle_cluster_complete
handle_cluster_status = _plugin.handle_cluster_status
handle_cluster_config = _plugin.handle_cluster_config
register = _plugin.register
SCHEMAS = _plugin.SCHEMAS
HANDLERS = _plugin.HANDLERS
DEFAULT_PORT = _plugin.DEFAULT_PORT
