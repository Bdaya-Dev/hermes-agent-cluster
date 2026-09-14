"""Disk preflight — free-space floor for worker claim + main-side degradation.

FACTORY RESILIENCE (2026-09-14 incident, shared/claude-plugins#892): a worker
with a FULL disk kept claiming tasks and killed every lane it took — five lanes
died with ``OSError: [Errno 28] No space left on device`` in concurrent_log_handler
while the node stayed ``online`` with all capabilities (every heartbeat forces
status=online), so every ``review`` task in the fleet kept routing to it.

This module holds the two PURE rules; the wiring lives in:
  - hermes_cluster/core/worker_connector.py — reports ``disk_free_gb`` in every
    join/heartbeat POST,
  - hermes_cluster/core/agent_executor.py — refuses to claim below the floor,
  - hermes_cluster/state (+ watchdog) — main marks the node ``degraded`` while
    the reported free space is below the floor and restores it automatically.

The floor is YAML config ONLY (``node.min_free_disk_gb`` in cluster.yaml —
owner ruling: never an env var). When the config says nothing, the default is
DEFAULT_MIN_FREE_DISK_GB.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional, Union

# The incident's measured failure mode starts at multi-hundred-MB log writes;
# 5 GB leaves room for lane transcripts, session DBs and result files on any
# node volume that was healthy enough to install hermes on.
DEFAULT_MIN_FREE_DISK_GB = 5.0

_BYTES_PER_GB = 1024 ** 3


def free_disk_gb(path: Union[str, Path, None]) -> Optional[float]:
    """Free space in GB on the volume holding *path*, or None when unknown.

    Unreadable path (missing tree, statvfs failure, permission error) returns
    None — callers MUST treat None as "unknown", never as "fine" (a disk
    preflight that fails open is the preflight that never fires).
    """
    if not path:
        return None
    try:
        return shutil.disk_usage(str(path)).free / _BYTES_PER_GB
    except Exception:
        return None


def is_below_floor(free_gb: Optional[float], min_free_gb: float) -> bool:
    """True when a KNOWN reading sits below the floor.

    ``free_gb is None`` (unreadable) or ``<= 0`` (floor disabled) is False:
    absence of a reading must not change behaviour for nodes that do not (or
    cannot yet) report disk — older workers keep their exact pre-#892 semantics.
    """
    if free_gb is None or min_free_gb <= 0:
        return False
    return free_gb < min_free_gb


def effective_min_free_gb(config_value: Optional[float]) -> float:
    """Resolve the YAML floor: None/absent -> DEFAULT_MIN_FREE_DISK_GB,
    a number (incl. 0 = disabled) -> that number."""
    if config_value is None:
        return DEFAULT_MIN_FREE_DISK_GB
    return float(config_value)


def disk_reason(free_gb: Optional[float], min_free_gb: float) -> str:
    """Human-readable degradation reason for a below-floor reading ('' when
    the rule does not fire). Surfaced via GET /api/v1/nodes Node.status_reason."""
    if not is_below_floor(free_gb, min_free_gb):
        return ""
    return (
        f"disk below floor: {free_gb:.2f} GB free < {min_free_gb:.1f} GB "
        f"(node.min_free_disk_gb, shared/claude-plugins#892)"
    )


def disk_free_gb(path: Union[str, Path, None]) -> Optional[float]:
    """Report value for join/heartbeat payloads: free GB rounded to 3 decimals,
    or None when the volume is unreadable (the field is then OMITTED — an
    absent field is what marks an older worker on the main)."""
    gb = free_disk_gb(path)
    return None if gb is None else round(gb, 3)
