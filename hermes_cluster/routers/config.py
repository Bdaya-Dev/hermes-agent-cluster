"""Config API — 5 endpoints matching Go internal/config/config.go.

Endpoints:
  GET    /api/v1/config          — current config (or defaults)
  PUT    /api/v1/config          — update config
  POST   /api/v1/config/validate — validate config fields
  GET    /api/v1/config/yaml     — config as YAML string
  POST   /api/v1/config/restart  — trigger graceful restart
"""

from __future__ import annotations

import copy
import logging
import os
import signal
import sys
from typing import Any, Dict, List, Optional

import yaml
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from ..models import ConfigJSON

router = APIRouter(prefix="/api/v1/config", tags=["config"])
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state — initialized by app.py via init()
# ---------------------------------------------------------------------------
_store = None  # ClusterStore or ClusterState
_config_path: str = ""
_restart_callback = None  # Optional callable invoked on restart request
# Optional callable invoked AFTER a successful config save (same lifecycle
# hook shape as restart): the metering poller uses it to arm/disarm without
# a redeploy (PUT config -> ensure_started reads metering.enabled).
_post_save_callback = None


def set_post_save_callback(callback) -> None:
    global _post_save_callback
    _post_save_callback = callback


def init(store, config_path: str = "", restart_callback=None):
    """Bind the config router to a store instance.

    Args:
        store: ClusterStore or ClusterState with get_config/set_config/get_config_path
        config_path: optional path to cluster.yaml file
        restart_callback: optional callable() invoked when restart is requested
    """
    global _store, _config_path, _restart_callback
    _store = store
    _config_path = config_path or getattr(store, "_config_path", "")
    _restart_callback = restart_callback


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _current_config() -> Dict[str, Any]:
    """Return current config dict, falling back to defaults."""
    cfg = _store.get_config() if _store else None
    if cfg is None:
        return ConfigJSON().model_dump()
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> List[str]:
    """Validate config dict against Go's Validate() rules.

    Returns list of error strings. Empty list = valid.
    """
    errors: List[str] = []

    # cluster.id required
    cluster = cfg.get("cluster", {})
    if not cluster.get("id"):
        errors.append("cluster.id is required")

    # node.id required
    node = cfg.get("node", {})
    if not node.get("id"):
        errors.append("node.id is required")

    # server.port 1-65535
    server = cfg.get("server", {})
    port = server.get("port", 8787)
    if not isinstance(port, int) or port < 1 or port > 65535:
        errors.append(f"server.port must be 1-65535, got {port}")

    # cluster.role must be "main" or "worker"
    role = cluster.get("role", "main")
    if role not in ("main", "worker"):
        errors.append(f"cluster.role must be 'main' or 'worker', got '{role}'")

    # worker nodes require endpoint
    if role == "worker" and not cluster.get("endpoint"):
        errors.append("cluster.endpoint is required for worker nodes")

    return errors


def _config_to_yaml(cfg: Dict[str, Any]) -> str:
    """Convert config dict to YAML string."""
    return yaml.dump(cfg, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
async def get_config(defaults: bool = Query(False, description="Return default config")):
    """GET /api/v1/config — return current config or defaults.

    H1 fix: redact token fields from the response to prevent secret leakage.
    """
    if defaults:
        cfg = ConfigJSON().model_dump()
    else:
        cfg = _current_config()
    # N3a: deep-copy before redaction — _redact_tokens mutates in place,
    # and _current_config() returns the LIVE store dict. Without copy, a
    # GET would permanently overwrite tokens with "***REDACTED***" (N3b).
    cfg = copy.deepcopy(cfg)
    _redact_tokens(cfg)
    return cfg


def _redact_tokens(cfg: dict) -> dict:
    """Recursively redact token fields from config dict (H1 fix)."""
    _REDACT_KEYS = {"token", "secret", "password"}
    if isinstance(cfg, dict):
        for key in list(cfg.keys()):
            if key in _REDACT_KEYS:
                cfg[key] = "***REDACTED***"
            else:
                _redact_tokens(cfg[key])
    elif isinstance(cfg, list):
        for item in cfg:
            _redact_tokens(item)
    return cfg


@router.put("")
async def update_config(cfg: ConfigJSON):
    """PUT /api/v1/config — update config and persist to file."""
    config_dict = cfg.model_dump()

    # Validate before saving
    errors = _validate_config(config_dict)
    if errors:
        raise HTTPException(
            status_code=422,
            detail={"validation_errors": errors},
        )

    # Update in-memory store
    if _store:
        _store.set_config(config_dict)

    # Persist to file if path configured. BEST-EFFORT (#906): on the hosted
    # main the config path is the GitOps-seeded ConfigMap mount
    # (/etc/hermes/cluster.yaml) — read-only BY DESIGN, since the runtime
    # store is authoritative after first boot (see the seed-once rule in
    # core/metering.py and routers/intake). A failed file mirror must never
    # 500 a save that already landed in the store: operators read the 500 as
    # "the change did not apply" and retried or rolled back something that
    # HAD applied. Log and skip; report the file state in the response.
    path = _config_path or (getattr(_store, "get_config_path", lambda: "")() if _store else "")
    file_status = "skipped"
    if path:
        try:
            with open(path, "w") as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
            file_status = "written"
        except OSError as e:
            logger.warning(
                "config YAML mirror not written (%s: %s) — runtime store "
                "holds the change; this is expected on the read-only GitOps "
                "mount (#906)", path, e)

    # Lifecycle hook after a successful save (metering arm/disarm without a
    # redeploy). Best-effort: a poller failure must never mask a saved config.
    if _post_save_callback:
        try:
            _post_save_callback()
        except Exception:
            logging.getLogger(__name__).exception("post-save config callback failed")

    return {"status": "saved", "config": config_dict, "file": file_status}


@router.post("/validate")
async def validate_config(cfg: Optional[ConfigJSON] = None):
    """POST /api/v1/config/validate — validate config (current or provided).

    If no body is provided, validates the current config.
    If a body is provided, validates that config.
    """
    if cfg is not None:
        config_dict = cfg.model_dump()
    else:
        config_dict = _current_config()

    errors = _validate_config(config_dict)
    valid = len(errors) == 0

    # N3c: redact tokens from the echoed config — validate previously
    # returned the live config raw, leaking tokens to any authenticated peer.
    config_echo = copy.deepcopy(config_dict)
    _redact_tokens(config_echo)

    return {
        "valid": valid,
        "errors": errors,
        "config": config_echo,
    }


@router.get("/yaml")
async def get_config_yaml(defaults: bool = Query(False, description="Return default config as YAML")):
    """GET /api/v1/config/yaml — return config as YAML string.

    H1 fix: redact token fields.
    """
    if defaults:
        cfg = ConfigJSON().model_dump()
    else:
        cfg = _current_config()
    # N3a: deep-copy before redaction (same fix as GET /config)
    cfg = copy.deepcopy(cfg)
    _redact_tokens(cfg)

    yaml_str = _config_to_yaml(cfg)
    return PlainTextResponse(content=yaml_str, media_type="text/yaml")


@router.post("/restart")
async def restart_config():
    """POST /api/v1/config/restart — trigger graceful service restart.

    Invokes the registered restart_callback if available.
    Otherwise returns restart_unavailable.
    """
    pid = os.getpid()

    # Invoke callback if registered (testable path)
    if _restart_callback is not None:
        try:
            _restart_callback()
        except Exception as e:
            return {"status": "restart_failed", "pid": pid, "error": str(e)}
        return {"status": "restart_requested", "pid": pid, "method": "callback"}

    return {"status": "restart_unavailable", "pid": pid, "message": "no restart handler registered"}
