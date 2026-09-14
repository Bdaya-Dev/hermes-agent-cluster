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
    # #899: per-process identity carried to /join so the main can tell two
    # executors for the same node id apart. Hidden: set by the wrapper or a
    # restart script, never by hand.
    parser.add_argument("--instance-token", default="", help=argparse.SUPPRESS)
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

    # --- #899: per-node-id single-instance lock (worker role) -------------
    # Two live executors for one node id spawn every lane twice (measured
    # 2026-09-14 on windows_desktop: :loop wrapper relaunch + Start-
    # ScheduledTask = 4 executors, 81 orphan lane processes). The lock is
    # taken BEFORE anything starts; refusal exits non-zero with a fixed,
    # machine-detectable token (ALREADY RUNNING) the .cmd wrappers back off
    # on. The OS releases the lock (a bound loopback socket) on exit, so a
    # crash never wedges the restart. The main role keeps the pre-#899
    # contract (uvicorn's --port bind is its own single-instance arbiter);
    # the worker binds a local API port too, but that port is per-host-
    # config, while the node id is what the fleet dedups on.
    instance_token = args.instance_token or ""
    _worker_lock = None
    if args.node_role == "worker":
        from .core import single_instance as _si
        _lock_dir = None
        if args.db_path and args.db_path != ":memory:":
            _lock_dir = Path(args.db_path).expanduser().parent
        elif config_path:
            _lock_dir = Path(config_path).expanduser().parent
        try:
            _worker_lock = _si.acquire(args.node_id, data_dir=_lock_dir,
                                       token=instance_token or None)
        except _si.LockHeldByLiveInstance as exc:
            print(f"single-instance lock held for node '{exc.node_id}': "
                  f"ALREADY RUNNING (pid {exc.holder_pid})", file=sys.stderr)
            print(str(exc), file=sys.stderr)
            sys.exit(3)
        except RuntimeError as exc:
            print(f"single-instance lock error: {exc}", file=sys.stderr)
            sys.exit(4)
        instance_token = _worker_lock.token

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
        # #899: the worker lock's instance token rides to /join; main role
        # passes whatever --instance-token carried (normally '').
        instance_token=instance_token,
    )

    print(f"Starting hermes-cluster (Python) on {args.host}:{args.port}")
    print(f"Cluster: {args.cluster_id} | Node: {args.node_id} | Role: {args.node_role}")
    if static_dir:
        print(f"Dashboard: http://{args.host}:{args.port}/dashboard/")
    print(f"API docs:  http://{args.host}:{args.port}/docs")
    print(f"Health:    http://{args.host}:{args.port}/health")

    import uvicorn
    try:
        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        # #899: release the node-id lock on graceful shutdown (the OS frees
        # it on any exit anyway; this keeps the lock file tidy for the
        # documented restart path).
        if _worker_lock is not None:
            _worker_lock.release()


if __name__ == "__main__":
    main()
