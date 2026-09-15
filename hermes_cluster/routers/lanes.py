"""Lane-placement endpoints — /api/v1/lanes

Why this file exists (shared/claude-plugins#909): the #858 lane-to-node
affinity made the SCHEDULER honest — a lane whose ``lanes`` row names a node
is pinned to that node — but only MAIN's scheduler decides, and until now
main's ``lanes`` table was permanently empty: every ``record_lane`` call
happened in the WORKER's local store (the executor runs with the worker's
own state), and the sync layer replicates only nodes/tasks/leases. So
``pinned_node_id`` was always ``''`` on main and every lane delivery fell
through to fair least-loaded scheduling — measured: 30 of 375 lanes had
deliveries assigned to different nodes (a "bounce"), each one building a
second Hermes session + second clone on a machine that does not own the
lane. LFP-1's whole saving leaks exactly there.

The fix is a data path, not a new policy: workers REPORT their lane
placement (node id at spawn, session id at session-capture) and main stores
it in the same ``lanes`` table its scheduler already reads.

Endpoints (all behind the standard peer-auth middleware; nothing here is
public):
  POST /api/v1/lanes/report   — upsert ONE lane placement from the caller
  GET  /api/v1/lanes          — list placements (operators/dashboard)
  GET  /api/v1/lanes/{key}    — read one placement

Honesty rules baked into the report path:
  - A worker may only pin lanes to ITSELF: when the authenticated caller is
    known (X-Peer-Node, present on every peer-authed request), a body
    ``node_id`` naming another node is refused 403. Placement authority
    stays with the node that actually owns the session.
  - A report with an empty ``session_id`` never clobbers a known session —
    the spawn-time report legitimately has none yet; the reap-time report
    carries it.
  - Recording a placement re-triggers scheduling IN THE SAME call: the
    window between "pin became known" and "next external trigger" is exactly
    when a parked lane task could be fair-scheduled away again.
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..core.lane_affinity import same_node
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/lanes", tags=["lanes"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


class LaneReportRequest(BaseModel):
    """One lane placement as recorded by the node that owns the session."""
    lane_key: str
    node_id: str
    session_id: str = ""
    profile: str = ""
    role: str = "author"
    last_task_id: str = ""


def _same_node(a: str, b: str) -> bool:
    """Node-id spelling tolerance for the placement-authority gate.

    Delegates to :func:`core.lane_affinity.same_node` — ONE implementation,
    so the 403 gate here and the scheduler's pin match can never disagree
    about whether two spellings name the same node.

    This used to carry its own asymmetric copy of the rule, which the RV-1
    reviewer of PR#67 broke with a working exploit: ``_same_node("node_x",
    "node_node_x")`` returned True, so a caller holding ``node_x``'s token
    could pin lanes onto the unrelated node ``node_node_x``.
    """
    return same_node(a, b)


@router.post("/report")
async def report_lane(req: LaneReportRequest, request: Request):
    """Upsert a lane placement reported by the node running its delivery."""
    lane_key = (req.lane_key or "").strip()
    node_id = (req.node_id or "").strip()
    if not lane_key or not node_id:
        raise HTTPException(
            status_code=422,
            detail="lane placement report requires both lane_key and node_id")

    # Placement self-authority: a caller identified by peer auth may only
    # pin its lanes to itself. Under auth-off (single-node/local/dev) the
    # header is absent and the pre-#909 trust boundary is unchanged.
    caller = request.headers.get("X-Peer-Node", "") or ""
    if caller and not _same_node(caller, node_id):
        raise HTTPException(
            status_code=403,
            detail=(
                f"a lane may only be pinned to the reporting node: caller "
                f"{caller!r} cannot place {lane_key!r} on {node_id!r} "
                "(#909 — placement authority belongs to the session owner)"))

    existing = _state.get_lane(lane_key) or {}
    session_id = (req.session_id or "").strip() or (
        existing.get("session_id") or "")
    _state.record_lane(
        lane_key=lane_key,
        session_id=session_id,
        profile=req.profile or existing.get("profile") or "",
        role=req.role or existing.get("role") or "author",
        node=node_id,
        last_task_id=req.last_task_id or existing.get("last_task_id") or "",
    )

    # A new pin can un-park lane tasks this call — re-trigger scheduling so
    # the placement takes effect without waiting for an external beat.
    promoted = 0
    scheduled = 0
    try:
        promoted = _state.trigger_pending_tasks()
        scheduled = len(_state.schedule_pending_detailed())
    except Exception:
        # Recording the placement is the contract; a scheduling hiccup on
        # the next trigger must not fail the report (#897 posture: the
        # queue is recoverable, refusing the write is not).
        pass

    return {
        "status": "recorded",
        "lane_key": lane_key,
        "node": node_id,
        "promoted": promoted,
        "scheduled": scheduled,
    }


@router.get("")
async def list_lanes():
    lanes = _state.get_all_lanes()
    return {"lanes": lanes, "count": len(lanes)}


@router.get("/{lane_key}")
async def get_lane(lane_key: str):
    lane = _state.get_lane(lane_key)
    if not lane:
        raise HTTPException(status_code=404, detail="lane not found")
    return lane
