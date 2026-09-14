#!/usr/bin/env python3
"""Entry point for the hermes-cluster Python server.

Usage:
    python -m hermes_cluster.serve [--port 8787] [--config cluster.yaml]
"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Hermes Agent Cluster — Python backend")
    parser.add_argument("--port", type=int, default=8787, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--config", default="", help="Path to cluster.yaml config file")
    parser.add_argument("--static-dir", default="", help="Path to dashboard static files")
    parser.add_argument("--cluster-id", default="cluster_default", help="Cluster identifier")
    parser.add_argument("--node-id", default="node_main", help="Node identifier")
    parser.add_argument("--node-role", default="main", choices=["main", "worker"], help="Node role")
    parser.add_argument("--fed-token", default="", help="Federation auth token")
    parser.add_argument("--cluster-endpoint", default="", help="Main node endpoint (worker only)")
    parser.add_argument("--db-path", default="", help="SQLite path for a persistent cluster store (default: in-memory)")
    args = parser.parse_args()

    # Load config from YAML if provided
    config_path = args.config
    if config_path and Path(config_path).exists():
        try:
            import yaml
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            # Override CLI args from config
            if "cluster" in cfg:
                args.cluster_id = cfg["cluster"].get("id", args.cluster_id)
                args.node_role = cfg["cluster"].get("role", args.node_role)
                args.fed_token = cfg["cluster"].get("token", args.fed_token)
                args.cluster_endpoint = cfg["cluster"].get("endpoint", getattr(args, "cluster_endpoint", ""))
            if "node" in cfg:
                args.node_id = cfg["node"].get("id", args.node_id)
                args.node_capabilities = cfg["node"].get("capabilities", [])
                args.node_capability_probes = cfg["node"].get("capability_probes", {})
                # #892: disk preflight floor — YAML only (owner ruling: never
                # an env var). Absent key -> create_app default 5.0; explicit
                # 0 disables the rule.
                args.node_min_free_disk_gb = cfg["node"].get("min_free_disk_gb", None)
            if "server" in cfg:
                args.port = cfg["server"].get("port", args.port)
                args.host = cfg["server"].get("bind", args.host)
            if "agent_executor" in cfg:
                args.agent_executor_config = cfg["agent_executor"]
            if "store" in cfg and not args.db_path:
                args.db_path = cfg["store"].get("db_path", "") or ""
            # #829: store.backend selects the ClusterStore implementation.
            # store.dsn_env names the env var holding the (secret) DSN —
            # a literal DSN in the config file is rejected unconditionally.
            args.store_backend = ""
            args.store_dsn_env = "HERMES_CLUSTER_PG_DSN"
            if "store" in cfg:
                _store_cfg = cfg["store"] or {}
                args.store_backend = str(_store_cfg.get("backend", "") or "")
                if _store_cfg.get("dsn"):
                    raise SystemExit(
                        "config error: store.dsn is not supported — a literal "
                        "DSN embeds the DB password in the repo/config. "
                        "Put it in an env var and point store.dsn_env at it."
                    )
                if _store_cfg.get("dsn_env"):
                    args.store_dsn_env = str(_store_cfg["dsn_env"])
        except ImportError:
            print("Warning: PyYAML not installed, ignoring config file", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to load config: {e}", file=sys.stderr)

    # Auto-detect static directory
    static_dir = args.static_dir
    if not static_dir:
        # Look for static files in the Go project's dashboard directory
        go_dashboard = Path(__file__).parent.parent.parent / "internal" / "dashboard" / "static"
        if go_dashboard.exists():
            static_dir = str(go_dashboard)

    # #899: worker role = single-instance lock per node id, taken BEFORE
    # anything binds or starts polling. Measured 2026-09-14: a :loop wrapper
    # relaunch + a scheduled-task start produced two workers under one node
    # id and every lane ran twice. The second instance exits non-zero (code
    # 3) with a message naming the live twin; the wrapper backs off like any
    # crash. Main role is exempt (hosted main is single-replica by k8s).
    if args.node_role == "worker":
        from .instance_lock import guard_singleton_or_exit
        _lock_dir = ""
        if config_path and Path(config_path).exists():
            try:
                import yaml
                with open(config_path) as f:
                    _cfg = yaml.safe_load(f) or {}
                _lock_dir = str((_cfg.get("agent_executor") or {}).get(
                    "lock_dir", "") or "")
            except Exception:
                _lock_dir = ""
        guard_singleton_or_exit(
            args.node_id, data_dir=_lock_dir or None)
        # A graceful exit releases the lock; a hard kill leaves a stale file
        # whose dead-pid holder acquire() steals on the next start.
        import atexit
        from . import instance_lock as _il
        atexit.register(lambda: _il._HELD and _il._HELD.release())

    # Create and run app
    from .app import create_app

    app = create_app(
        cluster_id=args.cluster_id,
        node_id=args.node_id,
        node_role=args.node_role,
        config_path=config_path,
        fed_token=args.fed_token,
        cluster_endpoint=args.cluster_endpoint,
        node_capabilities=getattr(args, "node_capabilities", []),
        node_capability_probes=getattr(args, "node_capability_probes", {}) or None,
        node_min_free_disk_gb=getattr(args, "node_min_free_disk_gb", None),
        agent_executor_config=getattr(args, "agent_executor_config", None),
        static_dir=static_dir if static_dir else None,
        db_path=getattr(args, "db_path", "") or "",
        store_backend=getattr(args, "store_backend", "") or "",
        store_dsn_env=getattr(args, "store_dsn_env", "") or "HERMES_CLUSTER_PG_DSN",
    )

    print(f"Starting hermes-cluster (Python) on {args.host}:{args.port}")
    print(f"Cluster: {args.cluster_id} | Node: {args.node_id} | Role: {args.node_role}")
    if static_dir:
        print(f"Dashboard: http://{args.host}:{args.port}/dashboard/")
    print(f"API docs:  http://{args.host}:{args.port}/docs")
    print(f"Health:    http://{args.host}:{args.port}/health")

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
