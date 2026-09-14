"""hermes-cluster Python plugin — replaces Go binary with native Python backend.

Registers tools that let the agent interact with the cluster:
- kanban_cluster_init: Start the Python cluster backend
- kanban_cluster_join: Join a cluster as worker
- kanban_cluster_submit: Submit a task to the cluster
- kanban_cluster_list: List tasks on the cluster
- kanban_cluster_nodes: List cluster nodes
- kanban_cluster_heartbeat: Send heartbeat
- kanban_cluster_complete: Mark task as completed
- kanban_cluster_status: Get cluster status
- kanban_cluster_config: Get/update cluster configuration

Auto-start: When the plugin loads, it starts the Python FastAPI server
in a background thread. No Go binary required.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_cluster_config: Dict[str, Any] = {}
_server_thread: Optional[threading.Thread] = None
_server_stop = threading.Event()
_base_url: str = ""
# #893: set when a cluster config NAMES an endpoint but it is unusable
# (e.g. no http(s) scheme). The tools then answer LOUDLY with this reason
# instead of silently addressing the wrong (loopback) cluster.
_endpoint_error: str = ""

DEFAULT_PORT = 8787
DEFAULT_CLUSTER_ID = "hermes-cluster"
DEFAULT_NODE_ID = "node_main"
DEFAULT_NODE_NAME = "main-node"
DEFAULT_CAPABILITIES = ["planning", "reviewing", "scheduling"]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_plugin_config() -> Dict[str, Any]:
    """Load plugin configuration from environment or config file."""
    config = {
        "auto_start": True,
        "port": DEFAULT_PORT,
        "cluster_id": DEFAULT_CLUSTER_ID,
        "node_id": DEFAULT_NODE_ID,
        "node_name": DEFAULT_NODE_NAME,
        "capabilities": DEFAULT_CAPABILITIES,
        "token": "",
        "config_path": "",
        "static_dir": "",
    }

    env_map = {
        "HERMES_CLUSTER_AUTO_START": ("auto_start", lambda x: x.lower() in ("true", "1", "yes")),
        "HERMES_CLUSTER_PORT": ("port", int),
        "HERMES_CLUSTER_ID": ("cluster_id", str),
        "HERMES_CLUSTER_NODE_ID": ("node_id", str),
        "HERMES_CLUSTER_NODE_NAME": ("node_name", str),
        "HERMES_CLUSTER_TOKEN": ("token", str),
        "HERMES_CLUSTER_CONFIG": ("config_path", str),
        "HERMES_CLUSTER_STATIC_DIR": ("static_dir", str),
    }

    for env_var, (key, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[key] = converter(value)
            except (ValueError, TypeError):
                logger.warning("Invalid value for %s: %s", env_var, value)

    return config


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def _parse_peer_tokens(raw: str) -> Dict[str, str]:
    """Parse "node_id:token,node_id:token" (same shape app.py reads from PEER_TOKENS)."""
    out: Dict[str, str] = {}
    for item in (raw or "").split(","):
        item = item.strip()
        if ":" in item:
            node_id, token = item.split(":", 1)
            if node_id.strip() and token.strip():
                out[node_id.strip()] = token.strip()
    return out


def _peer_token_from_file() -> str:
    """Read the fleet-convention peer token file, or "" (never raises).

    Same path worker_connector._resolve_peer_token falls back to
    (~/.config/bdaya/hermes-peer-token) — one fleet convention, no env var.
    """
    try:
        p = Path.home() / ".config" / "bdaya" / "hermes-peer-token"
        if p.is_file():
            return p.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    return ""


def _configure_peer_auth(config: Dict[str, Any]) -> bool:
    """Configure module-level peer-auth signing for THIS process (plugin-only mode).

    In auto_start=false mode no server is created in-process, so nothing else
    ever calls peer_auth.configure(); without this every _api_call is unsigned
    and an auth-ON main answers 401. Returns True when signing was configured.

    Token resolution mirrors worker_connector._resolve_peer_token — the fleet
    convention file ``~/.config/bdaya/hermes-peer-token`` is the fallback when
    no token is set in cluster config — so a worker needs ZERO env vars.
    """
    token = config.get("token") or ""
    if not token:
        token = _peer_token_from_file()
    if not token:
        return False
    try:
        from hermes_cluster.core import peer_auth
        peers = _parse_peer_tokens(os.environ.get("PEER_TOKENS", ""))
        peer_auth.configure(config.get("node_id", DEFAULT_NODE_ID), token, peers or None)
        return True
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("Peer auth configure failed: %s", e)
        return False


def _ensure_base_url(config: Dict[str, Any]) -> str:
    """Resolve the module base URL from cluster CONFIG (#893), loopback default.

    History: _start_server() used to be the only place that set _base_url, so
    in auto_start=false (attach-to-existing-main) mode every _api_call was
    built from an empty base and urlopen raised "unknown url type". Then the
    base was hardcoded to 127.0.0.1, which made every tool on a WORKER node
    address that worker's own local process — never the hosted main — so a
    lane's kanban_cluster_submit silently submitted into a local void (#893).

    Resolution order, all from cluster config files (owner ruling 2026-09-13:
    NOTHING is configured from env vars):
      1. an explicit ``endpoint`` key in the plugin config,
      2. the node's cluster config file via core.cluster_endpoint
         (``cluster: endpoint:`` — the SAME key the worker connector reads),
      3. loopback on the configured port — single-node behaviour, unchanged.

    When a config file NAMES an endpoint but it is unusable, _endpoint_error
    is set and returned as "" so callers surface the reason instead of
    guessing a cluster the operator did not configure.
    """
    global _base_url, _endpoint_error
    if _base_url:
        return _base_url

    explicit = str(config.get("endpoint") or "").strip().rstrip("/")
    if explicit:
        if explicit.startswith("http://") or explicit.startswith("https://"):
            _base_url = explicit
            logger.info("cluster plugin base URL from config endpoint: %s", _base_url)
            return _base_url
        _endpoint_error = (f"cluster endpoint config {explicit!r} is not an "
                           "http(s) URL — refusing to guess a base URL")
        return ""

    try:
        from .core.cluster_endpoint import resolve_cluster_endpoint
        resolved = resolve_cluster_endpoint(
            node_id=str(config.get("node_id") or ""),
            explicit_path=str(config.get("config_path") or ""),
        )
    except Exception as e:  # pragma: no cover - defensive
        resolved = {"endpoint": "", "incomplete": False, "why": str(e),
                    "source": "", "node_id": "", "token": ""}

    if resolved.get("endpoint"):
        _base_url = resolved["endpoint"]
        # The config file that NAMED the endpoint is also the authority for
        # WHO this node is and what it signs with — take node_id/token from
        # it (the same fields serve.py feeds the worker connector). This only
        # runs when the file actually resolved an endpoint, so a repo with no
        # cluster config cannot rewrite an env-configured plugin identity.
        if resolved.get("node_id"):
            config["node_id"] = resolved["node_id"]
        if not config.get("token") and resolved.get("token"):
            config["token"] = resolved["token"]
        logger.info("cluster plugin base URL from %s: %s",
                    resolved.get("source"), _base_url)
        return _base_url

    if resolved.get("incomplete"):
        _endpoint_error = resolved.get("why") or "cluster endpoint config invalid"
        return ""

    # Nothing names an endpoint: single-node default, loopback (pre-#893).
    _base_url = f"http://127.0.0.1:{config.get('port', DEFAULT_PORT)}"
    return _base_url


def _start_server(config: Dict[str, Any]) -> bool:
    """Start the Python FastAPI server in a background thread."""
    global _server_thread, _base_url

    port = config["port"]
    _base_url = f"http://127.0.0.1:{port}"

    # Check if already running
    if _health_check():
        logger.info("Cluster already running on port %d", port)
        return True

    def _run_server():
        try:
            import uvicorn
            from hermes_cluster.app import create_app

            # Determine static directory
            static_dir = config.get("static_dir", "")
            if not static_dir:
                # Look for dashboard static files in the Go project
                go_static = Path(__file__).parent.parent.parent / "internal" / "dashboard" / "static"
                if go_static.exists():
                    static_dir = str(go_static)

            app = create_app(
                cluster_id=config["cluster_id"],
                node_id=config["node_id"],
                node_role="main",
                config_path=config.get("config_path", ""),
                fed_token=config.get("token", ""),
                static_dir=static_dir if static_dir else None,
            )

            uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
        except Exception as e:
            logger.error("Failed to start cluster server: %s", e)

    _server_thread = threading.Thread(target=_run_server, daemon=True, name="cluster-server")
    _server_thread.start()

    # Wait for server to be ready
    deadline = time.time() + 5
    while time.time() < deadline:
        if _health_check():
            logger.info("Cluster server started on port %d", port)
            return True
        time.sleep(0.2)

    logger.error("Cluster server failed to start within timeout")
    return False


def _stop_server():
    """Stop the server (sets stop event, daemon thread exits)."""
    _server_stop.set()
    logger.info("Cluster server stop requested")


def _health_check() -> bool:
    """Check if the server is running."""
    try:
        req = Request(f"{_base_url}/health", method="GET")
        with urlopen(req, timeout=2) as resp:
            data = json.loads(resp.read().decode())
            return data.get("status") == "ok"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _api_call(method: str, path: str, data: dict = None) -> dict:
    """Make HTTP request to the Python cluster API.

    D2b fix: signs outgoing requests via peer_auth when configured,
    so the product works with auth ON.
    """
    url = f"{_base_url}{path}"
    if not _base_url:
        # #893 loud-by-default: config named an unusable endpoint
        # (_endpoint_error) or nothing resolved. Never address the wrong
        # cluster silently — an author lane must see WHY its submit failed.
        return {"error": _endpoint_error or "cluster endpoint not configured"}
    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    # D2b: sign the request when peer auth is configured
    try:
        from hermes_cluster.core import peer_auth
        if peer_auth.is_configured():
            # M1: include query string in signed path
            from hermes_cluster.core.peer_auth import build_signed_path
            signed_path = build_signed_path(url)
            auth_headers = peer_auth.sign_request(method, signed_path, body or b"")
            for key, value in auth_headers.items():
                req.add_header(key, value)
    except Exception as e:
        logger.debug("Peer auth signing skipped: %s", e)
    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def handle_cluster_init(args: dict, **kwargs) -> str:
    """Start the Python cluster backend."""
    config = _get_plugin_config()
    config.update(args)

    success = _start_server(config)
    if success:
        return json.dumps({
            "status": "started",
            "backend": "python",
            "port": config["port"],
            "cluster_id": config["cluster_id"],
            "node_id": config["node_id"],
            "dashboard": f"http://127.0.0.1:{config['port']}/dashboard/",
            "api_docs": f"http://127.0.0.1:{config['port']}/docs",
        })
    return json.dumps({"error": "Failed to start cluster server"})


def handle_cluster_join(args: dict, **kwargs) -> str:
    """Join an existing cluster as worker."""
    endpoint = args.get("endpoint", "http://127.0.0.1:8787")
    result = _api_call("POST", "/api/v1/nodes/join", {
        "node_name": args.get("node_id", "worker"),
        "capabilities": args.get("capabilities", ["coding"]),
        "endpoint": endpoint,
    })
    return json.dumps(result)


def handle_cluster_submit(args: dict, **kwargs) -> str:
    """Submit a task to the cluster.

    #893: POST /api/v1/tasks creates the row but does NOT schedule it — on the
    deployed main assignment happens at node joins, capability changes and
    completions, or on an explicit POST /api/v1/schedule/trigger. A lane that
    submits a reviewer task and stops must not leave it inert in `ready`
    waiting for the next event (measured 2026-09-12 in #878's notes: an
    unscheduled ready task sat 3+ minutes with idle workers; one trigger call
    dispatched it in <15s). Trigger after a successful create so self-dispatch
    actually dispatches. The call is best-effort: a main that already schedules
    on create is unaffected, and a trigger failure never masks the submit.
    """
    payload = {
        "title": args.get("title", "Untitled task"),
        "requires": args.get("requires", []),
        "priority": args.get("priority", 3),
    }
    # Fork PR#16 lane contract: role + lane_key ride the same submit so an
    # author lane can hand off a reviewer task bound to the reviewer lane key.
    for opt in ("role", "lane_key"):
        if args.get(opt):
            payload[opt] = args[opt]
    result = _api_call("POST", "/api/v1/tasks", payload)
    if isinstance(result, dict) and not result.get("error"):
        trigger = _api_call("POST", "/api/v1/schedule/trigger", {})
        if isinstance(trigger, dict) and trigger.get("error"):
            logger.warning("schedule trigger after submit failed: %s", trigger["error"])
    return json.dumps(result)


def handle_cluster_list(args: dict, **kwargs) -> str:
    """List tasks on the cluster."""
    result = _api_call("GET", "/api/v1/tasks")
    return json.dumps(result, indent=2)


def handle_cluster_nodes(args: dict, **kwargs) -> str:
    """List cluster nodes."""
    result = _api_call("GET", "/api/v1/nodes")
    return json.dumps(result, indent=2)


def handle_cluster_heartbeat(args: dict, **kwargs) -> str:
    """Send heartbeat to cluster."""
    node_id = args.get("node_id", _cluster_config.get("node_id", "node_main"))
    result = _api_call("POST", "/api/v1/nodes/heartbeat", {"node_id": node_id})
    return json.dumps(result)


def handle_cluster_complete(args: dict, **kwargs) -> str:
    """Mark a task as completed."""
    task_id = args.get("task_id")
    if not task_id:
        return json.dumps({"error": "task_id is required"})
    result = _api_call("POST", f"/api/v1/tasks/{task_id}/complete")
    return json.dumps(result)


def handle_cluster_status(args: dict, **kwargs) -> str:
    """Get cluster status."""
    result = _api_call("GET", "/api/v1/summary")
    return json.dumps(result, indent=2)


def handle_cluster_block(args: dict, **kwargs) -> str:
    """#894: park THIS task blocked on an owner decision ballot.

    A lane that hits a decision it cannot make headless may either write the
    escalation side-file (the executor's reap files the ballot) or call this
    directly — same endpoint, same record.
    """
    task_id = args.get("task_id")
    if not task_id:
        return json.dumps({"error": "task_id is required"})
    body = {
        "question": args.get("question") or "",
        "options": args.get("options") or [],
    }
    if args.get("class"):
        body["class"] = args["class"]
    result = _api_call("POST", f"/api/v1/tasks/{task_id}/block", body)
    return json.dumps(result)


def handle_cluster_answer(args: dict, **kwargs) -> str:
    """#894: record the owner's answer to a task's ballot and unblock it.

    Normal flow is the Telegram relay; this tool exists for an operator (or a
    lead session) answering from the cluster side.
    """
    task_id = args.get("task_id")
    if not task_id:
        return json.dumps({"error": "task_id is required"})
    body = {"answer": args.get("answer") or "",
            "answered_by": str(args.get("answered_by") or "")}
    result = _api_call("POST", f"/api/v1/tasks/{task_id}/answer", body)
    return json.dumps(result)


def handle_cluster_config(args: dict, **kwargs) -> str:
    """Get or update cluster configuration."""
    if args:
        result = _api_call("PUT", "/api/v1/config", args)
    else:
        result = _api_call("GET", "/api/v1/config")
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

SCHEMAS = {
    "kanban_cluster_init": {
        "name": "kanban_cluster_init",
        "description": "Start the Python cluster backend server. No Go binary required.",
        "parameters": {
            "type": "object",
            "properties": {
                "port": {"type": "integer", "description": "Port to listen on", "default": 8787},
                "cluster_id": {"type": "string", "description": "Cluster identifier"},
                "node_id": {"type": "string", "description": "This node's unique ID"},
                "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            },
        },
    },
    "kanban_cluster_join": {
        "name": "kanban_cluster_join",
        "description": "Join an existing cluster as a worker node.",
        "parameters": {
            "type": "object",
            "properties": {
                "endpoint": {"type": "string", "description": "Main node URL"},
                "node_id": {"type": "string", "description": "This node's unique ID"},
                "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            },
            "required": ["endpoint"],
        },
    },
    "kanban_cluster_submit": {
        "name": "kanban_cluster_submit",
        "description": (
            "Submit a task to the cluster for distributed execution. Used by an "
            "author lane for its hand-off: pass role='reviewer', requires=['review'] "
            "and lane_key='<repo>!<mr_iid>' to dispatch the independent reviewer "
            "yourself (RV-1: a fresh context on a different lane — never approve or "
            "merge your own work)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Task title/description (the lane's brief)"},
                "requires": {"type": "array", "items": {"type": "string"}, "description": "Required capabilities"},
                "priority": {"type": "integer", "description": "Priority band, ascending sort: 0=most urgent, 1..5 documented bands (default 3)", "default": 3},
                "role": {"type": "string", "enum": ["author", "reviewer"], "description": "Lane role: reviewer lanes spawn fresh-context on the reviewer tier"},
                "lane_key": {"type": "string", "description": "Stateful lane identity, e.g. '<repo>!<mr_iid>' for a reviewer lane"},
            },
            "required": ["title"],
        },
    },
    "kanban_cluster_list": {
        "name": "kanban_cluster_list",
        "description": "List all tasks in the cluster.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_nodes": {
        "name": "kanban_cluster_nodes",
        "description": "List all nodes in the cluster.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_heartbeat": {
        "name": "kanban_cluster_heartbeat",
        "description": "Send heartbeat to indicate this node is alive.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {"type": "string", "description": "Node ID"},
            },
        },
    },
    "kanban_cluster_complete": {
        "name": "kanban_cluster_complete",
        "description": "Mark a task as completed.",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Task ID to complete"},
            },
            "required": ["task_id"],
        },
    },
    "kanban_cluster_status": {
        "name": "kanban_cluster_status",
        "description": "Get cluster status summary.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_block": {
        "name": "kanban_cluster_block",
        "description": ("#894: park a task blocked on an owner decision ballot "
                        "(question + options + class 'technical'|'product'). "
                        "The hosted gateway relays the ballot to the owner's "
                        "phone; the answer unblocks the task and resumes the "
                        "same lane session. Preferred path for a cluster lane "
                        "is the escalation side-file named in its brief — use "
                        "this tool directly only from a lead/operator context."),
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "The blocked task."},
                "question": {"type": "string", "description": "The decision, phrased for a phone."},
                "options": {"type": "array", "items": {"type": "string"},
                            "description": "The choices (rendered as native buttons + 'Other')."},
                "class": {"type": "string", "description": "'technical' (default, owner DM) or 'product' (business group)."},
            },
            "required": ["task_id", "question"],
        },
    },
    "kanban_cluster_answer": {
        "name": "kanban_cluster_answer",
        "description": ("#894: record the owner's answer to a task's ballot and "
                        "unblock the task (re-dispatches to resume the lane)."),
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "The blocked task."},
                "answer": {"type": "string", "description": "The chosen option text or free text."},
                "answered_by": {"type": "string", "description": "Who answered (identity for the audit trail)."},
            },
            "required": ["task_id", "answer"],
        },
    },
    "kanban_cluster_config": {
        "name": "kanban_cluster_config",
        "description": "Get or update cluster configuration.",
        "parameters": {"type": "object", "properties": {}},
    },
}

HANDLERS = {
    "kanban_cluster_init": handle_cluster_init,
    "kanban_cluster_join": handle_cluster_join,
    "kanban_cluster_submit": handle_cluster_submit,
    "kanban_cluster_list": handle_cluster_list,
    "kanban_cluster_nodes": handle_cluster_nodes,
    "kanban_cluster_heartbeat": handle_cluster_heartbeat,
    "kanban_cluster_complete": handle_cluster_complete,
    "kanban_cluster_status": handle_cluster_status,
    "kanban_cluster_block": handle_cluster_block,
    "kanban_cluster_answer": handle_cluster_answer,
    "kanban_cluster_config": handle_cluster_config,
}


# ---------------------------------------------------------------------------
# Hook handlers
# ---------------------------------------------------------------------------

def _on_session_start(**kwargs) -> None:
    """Auto-start cluster server when session begins."""
    # #893: reuse the config resolved at register() (Hermes settings + file
    # defaults) — _get_plugin_config() alone would drop config_path/endpoint
    # and could wrongly auto-start a local server on a worker attached to a
    # remote main.
    config = dict(_cluster_config) if _cluster_config else _get_plugin_config()
    if not config.get("auto_start", True):
        return
    # #893: when cluster config points at a REMOTE main, attaching is the
    # whole point — auto-starting a local FastAPI here would spawn a second,
    # empty cluster (the #890 gateway-pod failure mode). Resolve first, then
    # decide.
    base = _ensure_base_url(config)
    if base and "127.0.0.1" not in base and "localhost" not in base:
        logger.info("cluster plugin attached to remote main %s — local auto-start skipped", base)
        return

    def _start_in_background():
        try:
            success = _start_server(config)
            if success:
                logger.info("Python cluster auto-started successfully")
            else:
                logger.debug("Cluster auto-start skipped")
        except Exception as e:
            logger.warning("Cluster auto-start failed: %s", e)

    thread = threading.Thread(target=_start_in_background, daemon=True, name="cluster-auto-start")
    thread.start()


def _on_session_end(**kwargs) -> None:
    """Stop cluster server when session ends."""
    _stop_server()
    logger.info("Cluster plugin session ended")


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def _config_from_hermes_settings(ctx) -> Dict[str, Any]:
    """Layer Hermes plugin settings (plugins.entries.<id>.settings.*) on the
    file defaults. This is the config home per the owner ruling — the Hermes
    config FILE, read through ctx.get_config — not env vars. get_config is
    probed with getattr: the fork's unit-test Ctx stubs predate it.

    Settings honoured (all optional):
      endpoint    — direct cluster-main base URL (beats cluster config files)
      node_id     — which worker cluster config file names this node
      config_path — explicit cluster config YAML (search anchor)
      auto_start  — local server auto-start (default true, single-node mode)
    """
    cfg = _get_plugin_config()
    get = getattr(ctx, "get_config", None)
    if callable(get):
        for key in ("endpoint", "node_id", "config_path", "auto_start"):
            try:
                value = get(key)
            except Exception:
                value = None
            if value not in (None, ""):
                cfg[key] = value
    return cfg


def register(ctx) -> None:
    """Register cluster tools with Hermes Agent."""
    _cfg = _config_from_hermes_settings(ctx)
    global _cluster_config
    _cluster_config = _cfg
    _ensure_base_url(_cfg)
    _configure_peer_auth(_cfg)
    for name, schema in SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset="kanban_cluster",
            schema=schema,
            handler=HANDLERS[name],
            description=schema["description"],
            emoji="🏗️",
        )

    # Register lifecycle hooks
    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("on_session_end", _on_session_end)

    logger.info("hermes-cluster Python plugin registered %d tools + auto-start hooks", len(SCHEMAS))
