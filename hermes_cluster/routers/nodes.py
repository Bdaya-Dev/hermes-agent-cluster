"""Node management endpoints — /api/v1/nodes"""

from fastapi import APIRouter, HTTPException

from ..models import (
    JoinRequest,
    JoinResponse,
    HeartbeatRequest,
    UpdateCapabilitiesRequest,
    Node,
    TaskStatus,
)
from ..state import ClusterState
from ..core.disk_preflight import (
    DEFAULT_MIN_FREE_DISK_GB,
    disk_reason,
)

router = APIRouter(prefix="/api/v1/nodes", tags=["nodes"])

# Will be set by the app factory
_state: ClusterState = None
_node_manager = None


def init(state: ClusterState, node_manager=None):
    global _state, _node_manager
    _state = state
    _node_manager = node_manager


@router.post("/join", response_model=JoinResponse)
async def join(req: JoinRequest):
    if _node_manager:
        node = _node_manager.join(
            node_id="node_" + req.node_name,
            name=req.node_name,
            capabilities=req.capabilities,
            max_concurrent=req.max_concurrent,
            disk_free_gb=req.disk_free_gb,
            instance_id=req.instance_id,
        )
    else:
        # Fallback to direct state
        node_id = "node_" + req.node_name
        existing = _state.get_node(node_id)
        if existing is not None:
            if req.instance_id \
                    and getattr(existing, "instance_id", "") != req.instance_id:
                # #899 re-join replaces the owning instance (same rule as the
                # NodeManager branch).
                _state.update_instance(node_id, req.instance_id)
            node = existing
        else:
            node = Node(
                id=node_id,
                name=req.node_name,
                capabilities=req.capabilities,
                max_concurrent=req.max_concurrent,
                disk_free_gb=req.disk_free_gb,
                instance_id=req.instance_id,
            )
            _state.register_node(node)
    return JoinResponse(node_id=node.id, status="registered")


@router.post("/heartbeat")
async def heartbeat(req: HeartbeatRequest):
    # #897: DISTINGUISH unknown-node beats. After any main restart the store
    # can lose (or not yet have) a worker's node row; NodeManager and every
    # store silently drop the beat while this endpoint answered
    # {"status":"ok"}, so the worker connector stayed orphaned until the
    # WORKER restarted (its own module NOTE documented this). 200 is kept —
    # a 4xx would make old connectors log an auth failure instead.
    # #899: a beat whose instance token disagrees with the node's current
    # owner is refused as "replaced" (checked FIRST — a replaced twin's beat
    # must not even reach the clock refresh; unknown_node outranking it
    # would send the zombie into the #897 re-join loop, which is the
    # join-storm shape #899 forbids).
    if _node_manager:
        accepted = _node_manager.send_heartbeat(
            req.node_id, disk_free_gb=req.disk_free_gb,
            instance_id=req.instance_id or None,
        )
        if not accepted:
            return {"status": "replaced", "node_id": req.node_id}
        node_exists = _node_manager.get_node(req.node_id) is not None
    else:
        node = _state.get_node(req.node_id)
        stored = getattr(node, "instance_id", "") if node else ""
        if stored and req.instance_id and req.instance_id != stored:
            return {"status": "replaced", "node_id": req.node_id}
        reason = disk_reason(req.disk_free_gb, _min_free_disk_gb())
        node_exists = node is not None
        _state.update_heartbeat(req.node_id, disk_free_gb=req.disk_free_gb,
                                status_reason=reason)
    if not node_exists:
        return {"status": "unknown_node", "node_id": req.node_id}
    return {"status": "ok"}


def _min_free_disk_gb() -> float:
    """The floor the fallback (no-node-manager) heartbeat path consults:
    the NodeManager's configured value when present, otherwise the #892
    default (5.0 GB) — same resolution rule as everywhere else."""
    if _node_manager is not None:
        return float(getattr(_node_manager, "_min_free_disk_gb",
                             DEFAULT_MIN_FREE_DISK_GB))
    return DEFAULT_MIN_FREE_DISK_GB


@router.get("")
async def list_nodes():
    if _node_manager:
        return _node_manager.get_all_nodes()
    return _state.get_all_nodes()


@router.patch("/{node_id}/capabilities")
async def update_capabilities(node_id: str, req: UpdateCapabilitiesRequest):
    node = _state.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="node not found")
    _state.update_capabilities(node_id, req.capabilities)
    # Re-trigger scheduling
    _state.trigger_pending_tasks()
    _state.schedule_pending()
    return {
        "node_id": node_id,
        "capabilities": req.capabilities,
        "status": "updated",
    }
