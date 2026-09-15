"""Worker health self-report — the node reports its own fitness (#879).

The observability gap this closes: a worker whose node is sick — CPU
pinned, a duplicate executor busy-spawning every lane twice — keeps
heartbeating *from its own connector thread*, and every heartbeat on main
forces ``status=online`` with an empty ``status_reason`` (the same
unconditional force #892 fixed for disk). Measured on
``windows_desktop_worker``: 100% CPU with a duplicate executor, yet
``GET /api/v1/nodes`` reported the node fully schedulable with no reason.

The fix follows the #892 precedent exactly: the *worker* measures raw
facts, rides them on join + every heartbeat, and the *main* applies the
thresholds and owns the status decision. Absent fields (older worker) mean
"no information" — every rule stays inert, byte-for-byte pre-#879
semantics. A health degradation clears automatically on the next healthy
beat and the staleness rules keep owning dead workers, so a self-report
can never wedge a node degraded forever; the only operator-revocable
state stays ``drained`` (main-side, #907).

Worker-side measurements (stdlib only — no psutil dependency):

  * ``cpu_load_pct`` (0.0–1.0) — busy CPU fraction. Windows (the incident
    platform — ``os.getloadavg`` is not available there) is sampled via
    ``kernel32!GetSystemTimes`` deltas between consecutive beats; POSIX
    uses the 1-minute load average normalised by CPU count. Unmeasurable →
    field omitted.
  * ``lane_count`` — live tracked spawns of the local executor, INCLUDING
    the ones #899's persisted-refresh re-attaches from a second executor
    writing the same store. The main thresholds it against the node's own
    declared ``max_concurrent`` (which its claim loop enforces, so an
    overage can only mean spawns it did not admit — the
    duplicate-executor/spawn-storm fingerprint measured 2026-09-14:
    every lane doubled, 81 orphan processes).
  * ``duplicate_executor`` — the SURVIVOR's own observation (#899's
    persisted-spawn refresh) that spawn records appeared in the shared
    store which it did not write: a second executor for this node id is
    live. The refused challenger never registers and never beats (its 409
    is loud in its own log and main's); the survivor carries the truth to
    the main so the node shows degraded WITH A REASON instead of online
    with an empty status_reason while every lane spawns twice.

Config (YAML only — owner ruling from #892: never an env var)::

    node:
      min_free_disk_gb: 5.0    # existing #892 floor
      max_cpu_load: 0.9        # NEW: degrade while cpu_load_pct >= this
                               #     (0/absent = rule disabled by default;
                               #     fleet configs opt in)
"""

from __future__ import annotations

import ctypes
import logging
import os
import sys
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Rule disabled unless the YAML opts in (mirrors #892's explicit-0 rule;
# unlike disk there is no safe universal default: a build box legitimately
# pins its cores, so only the lane-count/duplicate facts fire unconditionally).
DEFAULT_MAX_CPU_LOAD: float = 0.0


# ---------------------------------------------------------------------------
# Worker-side measurement
# ---------------------------------------------------------------------------

class CpuSampler:
    """Windows GetSystemTimes-delta CPU busy fraction; load-average on POSIX.

    ``measure()`` returns the fraction (0.0–1.0+) since the previous call —
    None when a measurement is not (yet) possible: before the first sample,
    when the platform has no source, or when a read fails. Callers OMIT the
    field on None; absence is "unknown", never "healthy".
    """

    def __init__(self) -> None:
        self._last: Optional[Tuple[int, int, int]] = None

    def measure(self) -> Optional[float]:
        if sys.platform == "win32":
            raw = self._system_times()
            if raw is None:
                return None
            prev, self._last = self._last, raw
            return self._busy_from_samples(prev, raw)
        return self._measure_posix()

    @staticmethod
    def _busy_from_samples(prev, cur):
        """Pure delta math (testable seam): busy fraction between two
        (idle, kernel, user) GetSystemTimes tuples; kernel includes idle.
        None before the first sample or on a degenerate window."""
        if prev is None:
            return None
        d_idle = cur[0] - prev[0]
        d_sys = cur[1] - prev[1]      # includes idle
        d_user = cur[2] - prev[2]
        total = d_sys + d_user
        if total <= 0 or d_idle < 0:
            return None
        busy = (total - d_idle) / float(total)
        return max(0.0, min(1.0, busy))

    # -- Windows: idle/kernel/user 64-bit jiffies deltas ------------------
    def _system_times(self):
        """(idle, kernel, user) raw jiffies or None (test seam)."""
        try:
            k32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            idle = ctypes.c_ulonglong()
            kern = ctypes.c_ulonglong()
            user = ctypes.c_ulonglong()
            if not k32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kern),
                                      ctypes.byref(user)):
                return None
            return (idle.value, kern.value, user.value)
        except Exception:
            logger.debug("GetSystemTimes CPU probe failed", exc_info=True)
            return None

    # -- POSIX: 1-minute load average / logical CPUs -----------------------
    def _measure_posix(self) -> Optional[float]:
        try:
            getloadavg = getattr(os, "getloadavg", None)
            if getloadavg is None:
                return None
            load1 = float(getloadavg()[0])
        except (OSError, ValueError, IndexError):
            return None
        ncpu = os.cpu_count() or 0
        if ncpu <= 0:
            return None
        return round(load1 / float(ncpu), 3)


def cpu_load_pct(sampler: Optional[CpuSampler] = None) -> Optional[float]:
    """One-shot CPU busy fraction via the shared sampler (None = unknown)."""
    s = sampler if sampler is not None else _default_sampler
    pct = s.measure()
    return None if pct is None else round(float(pct), 3)


_default_sampler = CpuSampler()


# ---------------------------------------------------------------------------
# Main-side rules (thresholds live here, never in the worker)
# ---------------------------------------------------------------------------

def is_cpu_saturated(cpu: Optional[float], max_cpu_load: float) -> bool:
    """True when a KNOWN reading sits at/above the configured ceiling.

    ``cpu is None`` (older worker / unreadable) or ``max_cpu_load <= 0``
    (rule disabled) is False — the #892 absence-means-nothing semantics.
    """
    if cpu is None or max_cpu_load <= 0:
        return False
    return cpu >= max_cpu_load


def is_lane_backlog(lane_count: Optional[int], max_concurrent: int) -> bool:
    """True when a reported live-lane count exceeds the node's own ceiling.

    ``lane_count`` None (older worker) or ``max_concurrent`` <= 0 (0 =
    unlimited — declared by the worker, and then no threshold exists) is
    False.
    """
    if lane_count is None or max_concurrent <= 0:
        return False
    return int(lane_count) > int(max_concurrent)


def health_reason(cpu: Optional[float], max_cpu_load: float,
                  lane_count: Optional[int], max_concurrent: int,
                  duplicate_executor: Optional[bool]) -> str:
    """Composed degradation cause for a beat ('' = nothing observed wrong).

    Joined with the caller's disk reason into one ``status_reason`` string;
    the order is fixed so repeated identical beats produce identical
    strings (NodeManager.set_status dedups on equality).
    """
    parts: List[str] = []
    if duplicate_executor:
        parts.append(
            "duplicate executor: another live executor shares this node id "
            "(#899) — lanes are being spawned twice"
        )
    if is_lane_backlog(lane_count, max_concurrent):
        parts.append(
            f"lane backlog: {int(lane_count)} live lanes > declared "
            f"max_concurrent {int(max_concurrent)} (#879 duplicate-executor "
            f"fingerprint)"
        )
    if is_cpu_saturated(cpu, max_cpu_load):
        parts.append(
            f"cpu saturated: {cpu:.2f} >= node.max_cpu_load "
            f"{max_cpu_load:.2f} (#879)"
        )
    return "; ".join(parts)
