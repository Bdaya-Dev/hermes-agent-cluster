"""Revoker — revokes all leases for a failed node.

Mirrors Go's internal/recovery/revoker.go:
  - RevokeAllForNode(node_id) → list of task_ids whose leases were revoked
  - Logs each revocation as a RecoveryEvent
"""

from __future__ import annotations

import secrets
import threading
from typing import TYPE_CHECKING, List

from ..models import LeaseStatus, RecoveryEvent

if TYPE_CHECKING:
    from ..state import ClusterState


def _gen_id(prefix: str = "recovery") -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


class Revoker:
    """Revokes all active leases belonging to a failed node."""

    def __init__(self, state: "ClusterState") -> None:
        self._state = state
        self._lock = threading.Lock()

    def revoke_all_for_node(self, node_id: str) -> List[str]:
        """Revoke all leases held for *node_id* and log each revocation.

        #916: an EXPIRED lease counts too. The recovery pipeline is keyed
        on lease expiry (the scanner fires when a lease dies), but by then
        the lease's status IS 'expired' — filtering to active-only meant
        the dead sitting's own reclaim pass saw nothing and the task
        stayed ``running`` on its lane seat forever. An expired lease is
        definitionally not a live worker's ownership, so revoking it can
        never step on a live lane. Every expired row held by the node is
        stale ownership and fair game, regardless of what active rows the
        node still holds.

        Returns:
            List of task_ids whose leases were revoked.
        """
        revoked_task_ids: List[str] = []
        seen: set = set()

        # Live ones (manual trigger_recovery on a node going offline) and
        # expired-but-unreclaimed ones (the TTL-driven path, #916).
        # get_active_leases marks past-TTL leases expired as a side
        # effect, so it is read FIRST and the expired set afterwards.
        for lease in (list(self._state.get_active_leases())
                      + list(self._state.get_expired_leases())):
            if lease.node_id != node_id or lease.id in seen:
                continue
            seen.add(lease.id)
            # Revoke the lease
            success = self._state.revoke_lease(lease.id)
            if success:
                revoked_task_ids.append(lease.task_id)
                # Log the revocation event
                event = RecoveryEvent(
                    id=_gen_id(),
                    task_id=lease.task_id,
                    node_id=node_id,
                    action="revoke_lease",
                    status="completed",
                    message=f"Revoked lease {lease.id} for task {lease.task_id}",
                )
                self._state.append_recovery_event(event)

        return revoked_task_ids
