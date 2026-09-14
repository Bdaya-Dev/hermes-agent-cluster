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
    if _node_manager:
        accepted = _node_manager.send_heartbeat(
            req.node_id, disk_free_gb=req.disk_free_gb,
            instance_id=req.instance_id or None,
        )
        # #899: answer "replaced" (still 200 — the answer is in the status
        # field, matching the older-worker contract that only reads .status)
        # so the stale twin's connector can stop itself.
        return {"status": "ok" if accepted else "replaced"}
    node = _state.get_node(req.node_id)
    stored = getattr(node, "instance_id", "") if node else ""
    if stored and req.instance_id and req.instance_id != stored:
        return {"status": "replaced"}
    reason = disk_reason(req.disk_free_gb, _min_free_disk_gb())
    _state.update_heartbeat(req.node_id, disk_free_gb=req.disk_free_gb,
                            status_reason=reason)
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
