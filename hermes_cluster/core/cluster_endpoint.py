"""Resolve the cluster-main endpoint purely from cluster CONFIG files — no env vars.

#893: the plugin previously hard-bound _base_url to f"http://127.0.0.1:{port}",
so from a worker node every kanban_cluster_* tool addressed that worker's own
local process and could never reach the hosted main. The fix reads the SAME
``cluster: endpoint:`` key the worker connector already uses
(serve.py:58 -> create_app(cluster_endpoint=...) -> worker_connector) from
whichever cluster config file this node boots with, searched in a fixed order:

  1. the file HERMES points at via the plugin's own settings entry
     (plugins.entries.hermes-agent-cluster.settings.config_path) — a HERMES
     config FILE, not an env var;
  2. ``<repo>/cluster-worker-<node>.yaml`` (the fleet convention:
     cluster-worker-desktop.yaml / cluster-worker-pc.yaml);
  3. ``<repo>/cluster.yaml`` — the single-node main default.

The repo root is derived from this module's own path
(hermes_cluster/plugin.py -> parents[1]); it is NOT configurable.

Return contract (loud-by-default, the #890 lesson: a silent empty answer is
the failure this class of bug hides behind):

  {"endpoint": str, "source": str, "incomplete": bool, "why": str}

  endpoint != ""             -> use it as the cluster main base URL.
  incomplete=True            -> a config file named cluster.endpoint but it
                                (and its token) are unusable: a host-less or
                                non-http value. The caller MUST surface
                                `why`, never fall back to loopback —
                                misconfiguration must not silently address
                                the wrong cluster.
  endpoint == "" and not
  incomplete                 -> no config names an endpoint at all:
                                single-node mode, loopback default, exactly
                                the pre-#893 behaviour.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

# Derived from this file's location: hermes_cluster/plugin.py -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]

_UNSET = {"endpoint": "", "node_id": "", "token": "", "source": "",
          "incomplete": False, "why": ""}


def _load_yaml(path: Path) -> Optional[Dict[str, Any]]:
    try:
        import yaml
    except Exception:
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else None
    except OSError:
        return None


def _apply_cluster_section(cfg: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Interpret one parsed config dict's cluster: section (+ node.id/token)."""
    out = {"endpoint": "", "node_id": "", "token": "", "source": source,
           "incomplete": False, "why": ""}
    node = cfg.get("node") or {}
    if isinstance(node, dict):
        out["node_id"] = str(node.get("id") or "").strip()
    section = cfg.get("cluster") or {}
    if not isinstance(section, dict):
        return out
    out["token"] = str(section.get("token") or "").strip()
    raw = section.get("endpoint")
    if raw is None or str(raw).strip() == "":
        return out
    endpoint = str(raw).strip().rstrip("/")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        out["endpoint"] = endpoint
        return out
    out["incomplete"] = True
    out["why"] = (f"cluster.endpoint={endpoint!r} in {source} is not an http(s) "
                  "URL — fix the config; refusing to guess a base URL.")
    return out


def candidate_config_paths(node_id: str = "",
                           explicit_path: str = "") -> list[Path]:
    """The fixed, documented search order for cluster config files.

    Worker files are matched by their ``node.id`` (fleet convention:
    cluster-worker-desktop.yaml declares windows_desktop_worker,
    cluster-worker-pc.yaml declares windows_pc_worker, ...): the file NAME is
    not the node id, so names alone cannot be trusted — every worker file is
    parsed and only the one whose node.id equals *node_id* is considered.
    """
    out: list[Path] = []
    if explicit_path:
        out.append(Path(explicit_path).expanduser())
    if node_id:
        for worker_file in sorted(_REPO_ROOT.glob("cluster-worker-*.yaml")):
            cfg = _load_yaml(worker_file)
            node = (cfg or {}).get("node") or {}
            if isinstance(node, dict) and str(node.get("id") or "").strip() == node_id:
                out.append(worker_file)
                break
    out.append(_REPO_ROOT / "cluster.yaml")
    return out


def resolve_cluster_endpoint(node_id: str = "",
                             explicit_path: str = "") -> Dict[str, Any]:
    """Return the cluster-main endpoint resolution (see module docstring)."""
    for path in candidate_config_paths(node_id=node_id,
                                       explicit_path=explicit_path):
        cfg = _load_yaml(path)
        if not cfg:
            continue
        resolved = _apply_cluster_section(cfg, str(path))
        if resolved["endpoint"] or resolved["incomplete"]:
            return resolved
    return dict(_UNSET)
