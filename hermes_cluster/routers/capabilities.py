"""The capability vocabulary — GET /api/v1/capabilities (#907).

Lanes were inventing capability names (`merge`, `land`, `flutter`, `invora`,
`author`) because the fleet published no vocabulary anywhere: a task's
`requires` was matched against node capabilities by
``scheduler.node_can_run`` and a miss produced NOTHING — no error, no log
line, no degraded node. The task simply sat READY forever, indistinguishable
from one that is merely waiting its turn. bayader-flutter!221's landing task
sat that way while the cluster ran at 4 of 22 slots.

Two things are needed to stop that, and this endpoint is both:

* **Say what exists.** A caller composing a task can read the real vocabulary
  instead of guessing a plausible-sounding word.
* **Say what is being asked for and cannot be served.** ``stalled`` lists
  every capability a LIVE task requires that no schedulable node advertises.
  That list is empty on a healthy fleet, so a non-empty one is an actionable
  alarm rather than something an operator has to go mining 1394 tasks for.

Read-only; no auth beyond whatever guards the rest of the read surface.
"""

from typing import Dict, List

from fastapi import APIRouter

from ..core.scheduler import ACTIVE_TASK_STATUSES, node_at_capacity
from ..models import NodeStatus, TaskStatus
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/capabilities", tags=["capabilities"])

_state: ClusterState = None

# Statuses where a task still WANTS to run. A stalled capability is only
# interesting while something is waiting on it; a completed task that once
# required a now-absent capability is history, not an alarm.
_LIVE_TASK_STATUSES = frozenset(
    {TaskStatus.pending, TaskStatus.ready, TaskStatus.blocked}
    | set(ACTIVE_TASK_STATUSES)
)


def init(state: ClusterState):
    global _state
    _state = state


def _schedulable(node) -> bool:
    """True when the scheduler would consider handing this node work.

    Mirrors the scheduler's own gates rather than re-deciding them: ONLINE,
    and not drained. A node at ``max_concurrent`` is deliberately still
    counted as schedulable — it is busy, not incapable, and reporting its
    capability as absent would turn a queue into a phantom outage.
    """
    if node.status is not NodeStatus.online:
        return False
    return not getattr(node, "drained", False)


@router.get("")
async def list_capabilities():
    """The fleet's capability vocabulary, plus anything stalled on a gap."""
    nodes = _state.get_all_nodes() if _state else []

    active_counts: Dict[str, int] = {}
    tasks = _state.get_all_tasks() if _state else []
    for task in tasks:
        if task.status in ACTIVE_TASK_STATUSES and task.assigned_to:
            active_counts[task.assigned_to] = active_counts.get(task.assigned_to, 0) + 1

    vocab: Dict[str, dict] = {}
    for node in nodes:
        schedulable = _schedulable(node)
        free = 0
        if schedulable and not node_at_capacity(node, active_counts.get(node.id, 0)):
            free = (
                max(0, node.max_concurrent - active_counts.get(node.id, 0))
                if node.max_concurrent > 0
                else -1  # -1 = unlimited, distinct from 0 = full
            )
        for cap in node.capabilities or []:
            entry = vocab.setdefault(
                cap,
                {"capability": cap, "nodes": [], "unavailable_nodes": [],
                 "servable": False, "free_slots": 0},
            )
            if schedulable:
                entry["nodes"].append(node.id)
                entry["servable"] = True
                if entry["free_slots"] >= 0:
                    entry["free_slots"] = -1 if free < 0 else entry["free_slots"] + free
            else:
                # Advertised, but by a node the scheduler will not use. Kept
                # visible rather than dropped: "offline/drained node has it"
                # is a different remedy from "nobody has it".
                entry["unavailable_nodes"].append(node.id)

    known = {cap for cap, e in vocab.items() if e["servable"]}

    # What is actually being asked for that the fleet cannot serve. This is
    # the alarm — on a healthy fleet it is empty.
    stalled: Dict[str, dict] = {}
    for task in tasks:
        if task.status not in _LIVE_TASK_STATUSES:
            continue
        for cap in task.requires or []:
            if cap in known:
                continue
            e = stalled.setdefault(cap, {"capability": cap, "task_count": 0,
                                         "task_ids": [], "oldest_created_at": None})
            e["task_count"] += 1
            if len(e["task_ids"]) < 20:  # bounded: an alarm, not a task dump
                e["task_ids"].append(task.id)
            created = task.created_at.isoformat() if task.created_at else None
            if created and (e["oldest_created_at"] is None
                            or created < e["oldest_created_at"]):
                e["oldest_created_at"] = created

    return {
        "capabilities": sorted(vocab.values(), key=lambda e: e["capability"]),
        "known": sorted(known),
        "stalled": sorted(stalled.values(), key=lambda e: -e["task_count"]),
        "node_count": len(nodes),
        "schedulable_node_count": sum(1 for n in nodes if _schedulable(n)),
    }
