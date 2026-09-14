"""FastAPI application factory — replaces Go's Server + Chi router.

Serves:
  - REST API at /api/v1/*
  - Health check at /health
  - Web Dashboard at /dashboard/*
  - Metrics placeholder at /metrics
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .state import ClusterState
from .routers import (
    nodes_router,
    tasks_router,
    ballots_router,
    leases_router,
    sync_router,
    recovery_router,
    schedule_router,
    federation_router,
    hooks_router,
    workflow_router,
    status_router,
    config_router,
    visualization_router,
    setup_router,
    cluster_router,
    intake_router,
)
from .routers import nodes as nodes_mod
from .routers import tasks as tasks_mod
from .routers import leases as leases_mod
from .routers import sync as sync_mod
from .routers import recovery as recovery_mod
from .routers import schedule as schedule_mod
from .routers import federation as federation_mod
from .routers import hooks as hooks_mod
from .routers import workflow as workflow_mod
from .routers import status as status_mod
from .routers import config as config_mod
from .routers import visualization as visualization_mod
from .routers import setup as setup_mod
from .routers import cluster as cluster_mod
from .routers import intake as intake_mod
import logging
logger = logging.getLogger(__name__)


def create_app(
    cluster_id: str = "cluster_default",
    node_id: str = "node_main",
    node_role: str = "main",
    config_path: str = "",
    fed_token: str = "",
    cluster_endpoint: str = "",
    node_capabilities: Optional[list] = None,
    node_capability_probes: Optional[dict] = None,
    # #892: node.min_free_disk_gb from cluster YAML (None -> default 5.0 GB,
    # 0 -> rule disabled). Owner ruling: YAML only, NEVER an env var.
    node_min_free_disk_gb: Optional[float] = None,
    agent_executor_config: Optional[dict] = None,
    static_dir: Optional[str] = None,
    db_path: str = "",
    store_backend: str = "",
    store_dsn_env: str = "HERMES_CLUSTER_PG_DSN",
    # #899: per-process identity of this executor, carried on every /join so
    # the main can refuse a SECOND executor for the same node id while the
    # first is still heartbeating. serve() acquires the node-id single-
    # instance lock and hands its token down; "" = no identity declared
    # (older/manual launch — pre-#899 idempotent join semantics).
    instance_token: str = "",
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        cluster_id: Cluster identifier
        node_id: This node's identifier
        node_role: "main" or "worker"
        config_path: Path to cluster.yaml for config save/load
        fed_token: Shared secret for federation auth
        static_dir: Path to dashboard static files (HTML/CSS/JS)
    """
    app = FastAPI(
        title="hermes-agent-cluster",
        description="Python backend for Hermes Agent Cluster — replaces Go implementation",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Peer-token auth (P1.2) — H2: per-app state, L2: INSIDE CORS
    # L2 fix: register PeerAuth BEFORE CORS so it sits INSIDE CORS.
    # Starlette middleware is a stack: last added = outermost. So:
    #   add_middleware(PeerAuth) then add_middleware(CORS) →
    #   request: CORS → PeerAuth → app; response: app → PeerAuth → CORS
    # This ensures 401s from PeerAuth carry CORS headers (ACAO).
    import os as _os
    from .core.peer_auth import PeerAuthState
    from .auth_middleware import PeerAuthMiddleware
    _local_token = _os.environ.get("PEER_TOKEN", fed_token)
    _peer_tokens_env = _os.environ.get("PEER_TOKENS", "")  # "nodeA:tokenA,nodeB:tokenB"
    _peer_tokens_map = {}
    if _peer_tokens_env:
        for entry in _peer_tokens_env.split(","):
            if ":" in entry:
                nid, tok = entry.split(":", 1)
                _peer_tokens_map[nid.strip()] = tok.strip()
    _peer_auth_enabled = bool(_local_token) and bool(_peer_tokens_map)
    # H2: create a per-app state, not module globals
    _peer_auth_state = PeerAuthState(
        local_node_id=node_id,
        local_token=_local_token if _peer_auth_enabled else "",
        peer_tokens=_peer_tokens_map if _peer_auth_enabled else {},
    )
    # Also configure the module-level default (for plugin.py outgoing calls
    # in single-process mode, where the plugin shares the server's process)
    if _peer_auth_enabled:
        from .core import peer_auth as _peer_auth_mod
        _peer_auth_mod.configure(
            local_node_id=node_id,
            local_token=_local_token,
            peer_tokens=_peer_tokens_map,
        )
    app.add_middleware(PeerAuthMiddleware, state=_peer_auth_state, enabled=_peer_auth_enabled)

    # CORS middleware — added AFTER PeerAuth so CORS is outermost
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Initialize state via the backend factory (#829):
    #   store.backend=postgres -> asyncpg-backed PostgresClusterStore (sync facade)
    #   store.db_path          -> SQLite ClusterStore (default, local/dev)
    #   neither                -> in-memory ClusterState.
    # All three expose the same public API.
    if store_backend and store_backend.lower() not in ("sqlite", ""):
        from .state.factory import create_store
        state = create_store(backend=store_backend, dsn_env=store_dsn_env)
    elif db_path:
        from .state.cluster_store import ClusterStore
        state = ClusterStore(db_path=db_path)
        logger.info("cluster state: SQLite store at %s", db_path)
    else:
        state = ClusterState()
        logger.info("cluster state: in-memory (set store.db_path in cluster.yaml to persist)")

    @app.on_event("shutdown")
    def _close_store() -> None:
        close = getattr(state, "close", None)
        if callable(close):
            close()
    state.cluster_id = cluster_id
    state.node_id = node_id
    state.node_role = node_role
    if config_path:
        state.set_config_path(config_path)

    # --- Initialize managers ---
    from .core.node_manager import NodeManager
    from .lease.lease_manager import LeaseManager
    from .recovery.manager import RecoveryManager

    # Create managers (ClusterState implements the same API as ClusterStore)
    _node_manager = NodeManager(state, min_free_disk_gb=node_min_free_disk_gb)
    _lease_manager = LeaseManager(state)
    _recovery_manager = RecoveryManager(state)

    # Wire: watchdog detects offline → trigger recovery
    def _on_node_event(event):
        if event.event_type == "status_changed" and "offline" in event.detail:
            _recovery_manager.trigger_recovery_async(event.node_id)

    _node_manager.add_listener(_on_node_event)

    # Wire: lease expiry → recovery (task needs rescheduling when lease expires)
    def _on_lease_expired(task_id, node_id):
        _recovery_manager.trigger_recovery_async(node_id)

    _lease_manager.on_expire(_on_lease_expired)

    # Start background services
    _node_manager.start_watchdog()
    _lease_manager.start()
    _recovery_manager.start_auto_recovery()

    # Start heartbeat for this node
    _node_manager.start_heartbeat_sender(state.node_id)

    # Worker federation: when role=worker and cluster_endpoint is set,
    # start an outbound connector that signs and POSTs join+heartbeat
    # to the main node. Without this, the worker only updates its local
    # store and the main never sees it.
    if node_role == "worker" and cluster_endpoint:
        from .core.worker_connector import start_worker_connector
        from .core.disk_preflight import effective_min_free_gb
        # Declare the worker's concurrency ceiling at /join so the main's
        # scheduler never assigns more tasks than this node can run (#833).
        _worker_max_concurrent = int(
            (agent_executor_config or {}).get("max_concurrent", 1)
        )
        # #892: report free disk of the volume holding the lanes dir (the
        # executor's working_dir when set, else HERMES_HOME) in every
        # join/heartbeat so the main can degrade this node below the floor.
        _disk_probe_path = str(
            (agent_executor_config or {}).get("working_dir", "") or ""
        )
        start_worker_connector(
            node_id=state.node_id,
            cluster_endpoint=cluster_endpoint,
            capabilities=node_capabilities or [],
            peer_token=fed_token,
            max_concurrent=_worker_max_concurrent,
            capability_probes=node_capability_probes or None,
            disk_probe_path=_disk_probe_path,
            # #899: declare this process's instance identity at /join.
            instance_token=instance_token,
        )

    # Agent executor: when role=worker and agent_executor is configured+enabled,
    # start the poll loop that claims leased tasks and spawns bdaya workers.
    _agent_executor = None
    if node_role == "worker" and cluster_endpoint and agent_executor_config:
        from .core.agent_executor import AgentExecutor, AgentExecutorConfig
        ae_cfg_dict = agent_executor_config or {}
        if ae_cfg_dict.get("enabled", False):
            ae_cfg = AgentExecutorConfig(
                enabled=True,
                profile=ae_cfg_dict.get("profile", "alibaba1"),
                model=ae_cfg_dict.get("model", "qwen3.7-plus"),
                worker=ae_cfg_dict.get("worker", "bdaya-dispatch"),
                poll_interval=float(ae_cfg_dict.get("poll_interval", 15)),
                max_concurrent=int(ae_cfg_dict.get("max_concurrent", 1)),
                spawn_timeout=float(ae_cfg_dict.get("spawn_timeout", 1800)),
                lane_idle_timeout=float(ae_cfg_dict.get("lane_idle_timeout", 21600)),
                working_dir=ae_cfg_dict.get("working_dir", ""),
                hermes_profile=ae_cfg_dict.get("hermes_profile", "default"),
                hermes_bin=ae_cfg_dict.get("hermes_bin", ""),
                hermes_reviewer_model=ae_cfg_dict.get("hermes_reviewer_model", "qwen3.7-plus"),
                retry_limit=int(ae_cfg_dict.get("retry_limit", 3)),
                # #892: worker-side claim guard uses the SAME YAML floor the
                # main degrades on (None -> default, 0 -> disabled).
                min_free_disk_gb=effective_min_free_gb(node_min_free_disk_gb),
            )
            _agent_executor = AgentExecutor(
                config=ae_cfg,
                node_id=state.node_id,
                cluster_endpoint=cluster_endpoint,
                peer_token=fed_token,
                # Persisted task->lane map: a mid-task restart reconciles from
                # the same store instead of re-spawning (#804 note 132791).
                store=state,
            )
            _agent_executor.start()

    # Store on state for router access
    state._node_manager = _node_manager
    state._lease_manager = _lease_manager
    state._recovery_manager = _recovery_manager
    state._agent_executor = _agent_executor

    # Wire up all routers with shared state
    nodes_mod.init(state, node_manager=_node_manager)
    tasks_mod.init(state, lease_manager=_lease_manager)
    leases_mod.init(state)
    sync_mod.init(state)
    recovery_mod.init(state, recovery_manager=_recovery_manager)
    schedule_mod.init(state)
    federation_mod.init(state, fed_token)
    # Initialize HookManager for the hooks router
    from .hooks.manager import HookManager
    hook_manager = HookManager()
    hooks_mod.set_hook_manager(hook_manager)
    workflow_mod.init(state)
    status_mod.init(state)
    config_mod.init(state)
    visualization_mod.init(state)
    cluster_mod.init(state)
    intake_mod.init(state)

    # Register routers
    app.include_router(nodes_router)
    app.include_router(tasks_router)
    app.include_router(ballots_router)
    app.include_router(leases_router)
    app.include_router(sync_router)
    app.include_router(recovery_router)
    app.include_router(schedule_router)
    app.include_router(federation_router)
    app.include_router(hooks_router)
    app.include_router(workflow_router)
    app.include_router(status_router)
    app.include_router(config_router)
    app.include_router(visualization_router)
    app.include_router(setup_router)
    app.include_router(cluster_router)
    app.include_router(intake_router)

    # Health endpoint (outside /api/v1)
    @app.get("/health")
    async def health():
        uptime = int((time.time() - state.started_at.timestamp()))
        return {
            "status": "ok",
            "cluster_id": state.cluster_id,
            "node_id": state.node_id,
            "role": state.node_role,
            "uptime_seconds": uptime,
            "version": "python-1.0.0",
        }

    # Metrics endpoint (placeholder)
    @app.get("/metrics")
    async def metrics():
        # Prometheus text format placeholder
        return JSONResponse(
            content="# No metrics collected yet\n",
            media_type="text/plain",
        )

    # Startup handler — re-schedule anything left `ready` by the previous process
    @app.on_event("startup")
    async def reschedule_orphaned_ready_tasks():
        """#878: a restart used to silently strand the entire queue.

        Scheduling is driven purely by task-lifecycle events — task created,
        capabilities updated, task completed. Each of those calls
        ``trigger_pending_tasks()`` exactly once. A task that is still `ready`
        when the process dies has therefore already spent its only trigger:
        the new process loads it from the store, and nothing ever asks the
        scheduler to look at it again. Node re-registration does NOT help —
        the trigger in routers/nodes.py sits in ``update_capabilities``, not
        in join or heartbeat.

        Measured on the hosted main 2026-09-12: a task with
        ``requires: ["tooling"]`` sat `ready` for over three minutes after a
        pod restart with three workers online and idle at ``load 0.0``, with
        zero rows written to ``scheduling_decisions``. One
        ``POST /api/v1/schedule/trigger`` dispatched it in under 15 seconds.

        This is worse in Kubernetes than it ever was on a desktop: a pod
        restarts on any node drain, image bump or eviction, unattended, and
        the failure is silent — no error, no `fail_reason`, `failed_schedules`
        stays 0. The queue simply goes inert.

        Workers do not schedule, so this runs on the main only.
        """
        if node_role != "main":
            return
        # Sync store calls inside an async handler: acceptable HERE because
        # startup runs before the server accepts any request, so there is no
        # concurrency to block. Moving this to a different lifecycle point
        # (a periodic sweep, a request hook) would need a thread or an async
        # store path instead.
        try:
            promoted = state.trigger_pending_tasks()
            assignments = state.schedule_pending_detailed()
            if promoted or assignments:
                logger.info(
                    "startup reschedule: promoted=%s assigned=%s",
                    promoted, len(assignments),
                )
        except Exception:
            # A scheduling hiccup must never prevent the server from starting —
            # an unscheduled queue is recoverable, a main that refuses to boot
            # is not.
            logger.exception("startup reschedule failed (continuing)")

    # Shutdown handler — stop all background threads
    @app.on_event("shutdown")
    async def shutdown():
        _node_manager.stop_heartbeat_sender()
        _node_manager.stop_watchdog()
        _lease_manager.stop()
        _recovery_manager.stop_auto_recovery()
        if _agent_executor:
            _agent_executor.stop()

    # Dashboard static file serving
    if static_dir:
        static_path = Path(static_dir)
        if static_path.exists():
            # Mount static files
            app.mount("/dashboard/static", StaticFiles(directory=str(static_path)), name="static")

            # Serve index.html at /dashboard/
            @app.get("/dashboard/")
            async def dashboard_index():
                index_file = static_path / "index.html"
                if index_file.exists():
                    return FileResponse(str(index_file))
                return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)

            # Serve guide.html at /dashboard/guide.html
            @app.get("/dashboard/guide.html")
            async def dashboard_guide():
                guide_file = static_path / "guide.html"
                if guide_file.exists():
                    return FileResponse(str(guide_file))
                return HTMLResponse("<h1>Guide not found</h1>", status_code=404)

            # Serve config.html at /dashboard/config.html
            @app.get("/dashboard/config.html")
            async def dashboard_config():
                config_file = static_path / "config.html"
                if config_file.exists():
                    return FileResponse(str(config_file))
                return HTMLResponse("<h1>Config page not found</h1>", status_code=404)

    # Redirect /dashboard to /dashboard/ (only when static dir is NOT configured)
    if not static_dir:
        @app.get("/dashboard/{path:path}")
        async def dashboard_fallback(path: str = ""):
            from fastapi.responses import HTMLResponse
            return HTMLResponse("<h1>Dashboard not configured</h1>", status_code=404)

    return app
