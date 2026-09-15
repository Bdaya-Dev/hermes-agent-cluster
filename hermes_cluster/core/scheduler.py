"""Fair, capacity-aware scheduling core (shared by in-memory + SQLite stores).

Single source of truth for *which* task goes to *which* node. Both
``ClusterState.schedule_pending`` and ``ClusterStore.schedule_pending``
used to run their own "first capability-matching node wins" loop, which
funelled every ready task to the first online node and left alternate
workers idle (#804, #833 — node_macbook_worker 32/32 while the Windows
nodes sat at 0/0).

Scheduling rule:

1. Only ONLINE nodes are candidates.
2. A READY task (priority ASC, then created_at ASC) goes to the candidate
   node with the fewest ACTIVE tasks; load ties break by least-recently
   picked (round-robin), so an identical-capability cluster spreads
   2/2/2 instead of N/0/0.
3. A node whose active count has reached its ``max_concurrent`` receives
   nothing. ``0`` means unlimited (matches ``NodeInfo.max_capacity``).
4. A task that still holds an ACTIVE lease is never (re-)assigned: a lease
   is exclusive ownership, and only recovery/expiry may move a task again.

A node's active count = tasks in ``running``/``assigned`` status whose
``assigned_to`` is that node.

LFP-1 lane serialization (#762 cardinality guard, #833 note 133037 rule #1):
a task carrying a ``lane_key`` is never assigned while ANOTHER task with the
same ``lane_key`` is ACTIVE (running/assigned) — exactly one live author
session per lane key, matching the bdaya-enforcement ``lane_key_guard`` DENY
at spawn time so intake can never schedule a lane onto a second node behind
the plugin's back. Queued sibling bundles on one lane therefore execute in
band order, one sitting at a time (the scheduler sort is priority, then
created_at — a band-0 bundle overtakes a queued band-3 sibling but still
waits for the ACTIVE sitting).

The planner is pure: it returns assignments and the stores apply them
under their own lock/transaction. The only mutable state is the
round-robin tiebreak cursor, which each store owns per-instance.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from ..models import Node, TaskStatus

# Task states that occupy a node slot. ``running`` is what the scheduler
# sets on assignment; ``assigned`` exists in the Go enum and would count
# the same way if a store ever uses it.
ACTIVE_TASK_STATUSES = frozenset({TaskStatus.running, TaskStatus.assigned})


def lane_blocked_ready_ids(tasks: Iterable[Any]) -> set:
    """READY task ids whose lane key already has an ACTIVE task (or an
    earlier-created ready sibling on the same lane).

    One pass: for each lane key, every ready task except the
    (priority, created_at)-first one is blocked, plus ALL ready tasks when an
    active sitting exists. Returning ready siblings as well keeps two bundle
    tasks of one lane from being handed to two nodes in the SAME schedule
    tick — the first assign flips it to running and the next tick continues,
    but within this tick we serialize up-front.
    """
    materialized = []
    for t in tasks:
        if isinstance(t, dict):
            lane = t.get("lane_key") or ""
            status = t.get("status")
            status = getattr(status, "value", status)
            materialized.append((t.get("id"), lane, str(status),
                                 t.get("priority", 3), t.get("created_at")))
        else:
            materialized.append((t.id, getattr(t, "lane_key", "") or "",
                                 t.status.value, t.priority, t.created_at))
    active_lanes = {lane for _, lane, status, _, _ in materialized
                    if lane and status in {s.value for s in ACTIVE_TASK_STATUSES}}
    by_lane: Dict[str, List] = {}
    for tid, lane, status, prio, created in materialized:
        if lane and status == TaskStatus.ready.value:
            by_lane.setdefault(lane, []).append((prio, created, tid))
    blocked = set()
    for lane, entries in by_lane.items():
        if lane in active_lanes:
            blocked.update(tid for _, _, tid in entries)
        else:
            entries.sort(key=lambda e: (e[0], e[1] or ""))
            blocked.update(tid for _, _, tid in entries[1:])
    return blocked

# Terminal states — a task in one of these must never be unassigned/revived.
TERMINAL_TASK_STATUSES = frozenset({
    TaskStatus.completed,
    TaskStatus.failed,
    TaskStatus.cancelled,
    TaskStatus.cancel_requested,
})

# 0 == unlimited (matches NodeInfo.max_capacity in models/__init__.py).
MAX_CONCURRENT_UNLIMITED = 0


def node_at_capacity(node: Node, active_count: int) -> bool:
    """True when *node* cannot accept another task under ``max_concurrent``.

    ``max_concurrent <= 0`` means unlimited (a node registered without a
    capacity is never considered full).
    """
    if node.max_concurrent <= 0:
        return False
    return active_count >= node.max_concurrent


def node_can_run(task_requires: List[str], node: Node) -> bool:
    """True when *node* declares every capability *task_requires*.

    A DRAINED node can run nothing at all (#907), including a task with an
    EMPTY ``requires``. Draining used to be attempted by stripping a node's
    capabilities, which fails twice over: an empty ``requires`` matches every
    node regardless of what it advertises, and the worker's next re-join
    overwrites the stripped list from its own local config
    (``node_manager.register_node`` -> ``update_capabilities``). Measured
    2026-09-15: a node that could not write results at all kept being handed
    unconstrained work, and the operator's strip silently reverted, orphaning
    the 9 tasks that required the maintenance-only capability it had used.
    Drain is therefore a node FLAG the scheduler honours before any capability
    matching, not a capability trick.
    """
    if getattr(node, "drained", False):
        return False
    if not task_requires:
        return True
    return all(cap in node.capabilities for cap in task_requires)


class FairScheduler:
    """Planner that picks the least-loaded capable online node with spare capacity.

    Ties -> least-recently-picked, which gives round-robin rotation across
    an identical-capacity cluster. The cursor is per store instance so two
    clusters do not share rotation state.

    Methods in this class do not call back into the store, so a store may
    hold its own lock while planning without deadlock risk.
    """

    def __init__(self) -> None:
        self._pick_round: int = 0
        self._last_picked_at: Dict[str, int] = {}

    def choose(
        self,
        task_requires: List[str],
        online_nodes: List[Node],
        active_counts: Dict[str, int],
    ) -> Optional[Node]:
        """Pick the node for a task, or ``None`` when no candidate can take it.

        A candidate must (a) match the task's capabilities, and (b) not be
        at ``max_concurrent``. Among candidates the winner minimises
        ``(active_count, last_picked_round)`` — fewest active tasks first,
        round-robin on ties.
        """
        best: Optional[Node] = None
        best_key: Optional[tuple] = None
        for node in online_nodes:
            if not node_can_run(task_requires, node):
                continue
            active = active_counts.get(node.id, 0)
            if node_at_capacity(node, active):
                continue
            key = (active, self._last_picked_at.get(node.id, -1))
            if best_key is None or key < best_key:
                best_key = key
                best = node
        return best

    def mark_picked(self, node_id: str) -> None:
        """Record that *node_id* took the current tick (round-robin cursor)."""
        self._last_picked_at[node_id] = self._pick_round
        self._pick_round += 1