"""Worker disk preflight — refuse to claim on a full disk (#892 resilience).

The 2026-09-14 05:32Z incident on node_windows_pc_worker: with 0 bytes free the
node kept claiming assigned tasks and every lane died mid-run
(``OSError: [Errno 28] No space left on device`` in concurrent_log_handler,
later rc=120) while the main kept routing ``review`` work to it. A worker cannot
stop the scheduler from marking tasks ``running`` for it, but it CAN refuse to
spawn a lane it has no disk to run — the task then survives for re-dispatch
instead of burning a delivery.

Guard placement: ``_claim_and_spawn`` filters the main's assigned-task list; the
floor is checked once per poll, and when the volume holding the executor's
``working_dir`` (HERMES_HOME / the hermes-results + hermes-briefs tree) is below
``node.min_free_disk_gb`` (cluster YAML — owner ruling: never an env var;
default 5 GB) the worker claims NOTHING this cycle and logs exactly one line.

Behaviour preservation: an unreadable volume (None) claims as before — the
worker-side floor is a measured refusal, never a guess. A disabled floor
(``min_free_disk_gb: 0``) disables the guard too (same semantics as the main's
degrade rule, see core/disk_preflight.py).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .disk_preflight import effective_min_free_gb, free_disk_gb, is_below_floor

logger = logging.getLogger(__name__)


def disk_gate_blocks(working_dir: str, min_free_gb: Optional[float]) -> bool:
    """True when this poll cycle must not claim (disk below the configured floor).

    Checks the volume holding *working_dir* — on every supported OS that is the
    same volume the executor writes briefs, results and lane transcripts to.
    Logs one clear line per call that blocks; stays silent otherwise so a
    healthy worker's poll loop keeps its log volume.
    """
    floor = effective_min_free_gb(min_free_gb)
    free = free_disk_gb(working_dir or ".")
    if not is_below_floor(free, floor):
        return False
    logger.warning(
        "disk preflight: refusing to claim — %.2f GB free at %s is below "
        "node.min_free_disk_gb=%.2f (tasks stay unclaimed for re-dispatch; "
        "shared/claude-plugins#892)",
        free, Path(working_dir or "."), floor,
    )
    return True
