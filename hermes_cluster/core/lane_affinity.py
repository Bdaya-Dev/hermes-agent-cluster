"""Lane-affinity scheduling core (shared/claude-plugins#858, defect 2).

Single source of truth for *which* node a READY task may go to, given the
stateful-lane placement recorded in the ``lanes`` table.

LFP-1 (one lane = one long-lived session on one machine) makes a lane's
node part of the task's identity: the session state and the working clone
live on that machine, so a second node would build a second session and a
second clone of the same branch. Scheduling a lane task anywhere else is
not a load-balancing tradeoff — it is the defect this file exists to kill.

Rule (shared by every store's ``schedule_pending_detailed``):

1. A task whose ``lane_key`` has a live ``lanes`` row with a node is
   PINNED to that node. The pinned node must be ONLINE and must not be at
   its ``max_concurrent`` ceiling, or the task is PARKED — it stays
   ``ready`` and waits for a later trigger.
2. Only an unplaced lane (no row, or a row that names no node) and any
   lane-less task are free to schedule anywhere, via the fair planner.

Why parking (defensible-answer decision the brief asks for):

- Silently re-pinning to another node is the bug restated: it builds the
  second session/clone the lane exists to prevent.
- Failing the task is worse: the lane node may be back in one heartbeat
  interval (offline detection is watchdog-driven), and a failure with a
  transient cause trains the operator to ignore failures.
- Parking preserves the queue order (priority, then created_at — the same
  keys the ready-scan already sorts on) and the lane's existing affinity.
  When the node returns, the task lands exactly where it belongs.

The planner is pure: callers pass the pinned node's online/capacity status
and this module never touches a store, so it is safe to call while a store
lock is held.
"""

from __future__ import annotations

from typing import List, Optional

from ..models import Node

from .scheduler import FairScheduler, node_at_capacity, node_can_run


class AffinityScheduler(FairScheduler):
    """Fair planner with lane-to-node affinity.

    A lane-pinned task is never handed to the load-balancing ``choose``:
    either it gets its lane's node or it stays parked (the caller leaves it
    ``ready`` — see the module docstring for why parking is the answer).
    Only unpinned tasks use the inherited least-loaded/round-robin logic.

    Returns ``(node, pinned)`` from :meth:`choose_pinned` so callers can
    label their scheduling decision honestly.
    """

    def choose_pinned(
        self,
        task_requires: List[str],
        online_nodes: List[Node],
        active_counts: dict,
        pinned_node_id: str = "",
    ) -> tuple[Optional[Node], bool]:
        """Pick the node for a task; ``(None, True)`` means PARK, not share.

        ``pinned_node_id`` is the node named by the task's lane row (empty =
        unplaced lane or lane-less task → normal fair scheduling).
        """
        if not pinned_node_id:
            return self.choose(task_requires, online_nodes, active_counts), False
        # Pinned: the lane's node only — never a load-balanced substitute.
        for node in online_nodes:
            if node.id != pinned_node_id:
                continue
            if not node_can_run(task_requires, node):
                # The lane node no longer satisfies the task's capabilities:
                # park rather than move; re-registering the node with the
                # right caps (or an operator re-pin) is the only way out.
                return None, True
            if node_at_capacity(node, active_counts.get(node.id, 0)):
                return None, True  # park until the node has a free slot
            return node, True
        return None, True  # lane node offline/degraded: park
