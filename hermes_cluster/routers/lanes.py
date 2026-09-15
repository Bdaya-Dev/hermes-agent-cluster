"""Stateful lane placements — /api/v1/lanes (#909).

The #858 affinity scheduler reads `lane_nodes` from MAIN's store, but
`record_lane()` only ever wrote the WORKER-local lanes table: main's
`lanes` stayed empty, `pinned_node_id` was always '' at choose time, and
30 of 375 lane_keys had deliveries bounce across nodes (measured
2026-09-15) — every bounce silently cold-starting a fresh Hermes session
off the clone's home machine, which is exactly the LFP-1 saving leaking.

This router is the missing data surface:

  * POST /api/v1/lanes/report — workers upsert their own lane placements
    (spawn-time persist + reap-time session capture). The authenticated
    X-Peer-Node header is authoritative for the pin: a body `node_id`
    that doesn't resolve to the reporting node is REJECTED (a worker
    cannot pin a lane to someone else's machine); an empty body node_id
    defaults to the authenticated node. Both node-id spellings
    (`macbook_worker` / `node_macbook_worker`) resolve to the REGISTERED
    node id so one lane never grows two rows.
  * GET /api/v1/lanes, GET /api/v1/lanes/{lane_key} — operator/dashboard
    read surface (main had no lane surface at all: /api/v1/lanes 404'd).

Like leases/tasks, peer-auth is enforced by the app middleware (deny-by-
default); nothing here is public.
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/lanes", tags=["lanes"])

_state = None


def init(state):
    global _state
    _state = state


class LaneReportRequest(BaseModel):
    lane_key: str
    node_id: str = ""  # must resolve to the authenticated peer node
    session_id: str = ""
    profile: str = ""
    role: str = "author"
    last_task_id: str = ""
    last_active_at: float | None = None


def _registered_node_id(peer_node: str) -> str:
    """Map either spelling of a node to its registered id; '' if unknown.

    The executor signs with its bare name (`macbook_worker`) while the
    registry may store `node_macbook_worker` (routers/nodes.py joins the
    prefix on /join). Normalizing to the REGISTERED row keeps one lane row
    per node regardless of which spelling was used, exactly like the
    executor's own `mine = {node_id, f'node_{node_id}'}` tolerance.
    """
    if not peer_node:
        return ""
    try:
        nodes = {str(n.id).lower(): str(n.id) for n in _state.get_all_nodes()}
    except Exception:
        return peer_node  # store trouble: keep the header value verbatim
    for cand in (peer_node, f"node_{peer_node}"):
        rid = nodes.get(cand.lower())
        if rid:
            return rid
    # Unregistered reporter: pin under the authenticated name anyway — the
    # signature proves identity; refusing placement would just re-open the
    # bounce this endpoint exists to close.
    return peer_node


@router.post("/report")
async def report_lane(
    req: LaneReportRequest,
    x_peer_node: str = Header(default="", alias="X-Peer-Node"),
):
    if not req.lane_key or not req.lane_key.strip():
        raise HTTPException(status_code=400, detail="lane_key required")
    peer = (x_peer_node or "").strip()
    if not peer:
        # Middleware-verified requests always carry the header; direct
        # unauthenticated calls never reach here. Belt-and-braces.
        raise HTTPException(status_code=401, detail="missing X-Peer-Node")
    registered = _registered_node_id(peer)
    if req.node_id and req.node_id.strip():
        want = _registered_node_id(req.node_id.strip())
        if want.lower() != registered.lower():
            raise HTTPException(
                status_code=403,
                detail=f"node_id {req.node_id!r} does not match authenticated peer "
                       f"node {peer!r} — a worker cannot pin a lane to another node",
            )
    node = registered
    lane = req.lane_key.strip()
    _state.record_lane(
        lane_key=lane,
        session_id=req.session_id or "",
        profile=req.profile or "",
        role=req.role or "author",
        node=node,
        last_task_id=req.last_task_id or "",
        last_active_at=req.last_active_at,
    )
    row = _state.get_lane(lane)
    return {"status": "reported", "lane": row}


@router.get("")
async def list_lanes():
    return _state.get_all_lanes()


@router.get("/{lane_key:path}")
async def get_lane(lane_key: str):
    row = _state.get_lane(lane_key)
    if not row:
        raise HTTPException(status_code=404, detail="lane not found")
    return row
