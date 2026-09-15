"""Heartbeat watchdog — monitors node health via heartbeat staleness.

Python port of Go's internal/heartbeat/watchdog.go.
Runs as a background thread, checks heartbeat age periodically,
and emits status change events (online → degraded → offline).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class WatchdogEvent:
    """Event emitted when a node's status changes."""

    __slots__ = ("node_id", "event_type", "timestamp")

    def __init__(self, node_id: str, event_type: str):
        self.node_id = node_id
        self.event_type = event_type  # "online", "degraded", "offline"
        self.timestamp = datetime.utcnow()


class HeartbeatNode:
    """Minimal node info needed by the watchdog."""

    __slots__ = ("id", "last_heartbeat", "status", "disk_free_gb",
                 "cpu_load_pct", "lane_count", "max_concurrent",
                 "duplicate_executor")

    def __init__(self, node_id: str, last_heartbeat: datetime, status: str,
                 disk_free_gb: Optional[float] = None,
                 cpu_load_pct: Optional[float] = None,
                 lane_count: Optional[int] = None,
                 max_concurrent: int = 0,
                 duplicate_executor: Optional[bool] = None):
        self.id = node_id
        self.last_heartbeat = last_heartbeat
        self.status = status
        # #892: free GB as last reported by this worker (None = never reported
        # / older worker → the disk rule never fires for this node).
        self.disk_free_gb = disk_free_gb
        # #879: health self-report as last observed by this node (each None =
        # never reported → that rule never fires; max_concurrent 0 = unlimited
        # → the lane rule stays inert).
        self.cpu_load_pct = cpu_load_pct
        self.lane_count = lane_count
        self.max_concurrent = max_concurrent
        self.duplicate_executor = duplicate_executor


class WatchdogRegistry:
    """Interface the watchdog needs from the cluster state."""

    def get_all_heartbeat_nodes(self) -> List[HeartbeatNode]:
        raise NotImplementedError

    def update_node_status(self, node_id: str, status: str,
                           reason: str = "") -> None:
        """Change a node's status; ``reason`` explains a non-online status
        (#892: disk floor / heartbeat staleness), surfaced via
        GET /api/v1/nodes in Node.status_reason."""
        raise NotImplementedError


class Watchdog:
    """Monitors node heartbeats and emits status change events.

    Parameters:
        registry: provides node heartbeat info and status updates
        check_interval: how often to check (seconds)
        degraded_after: seconds without heartbeat to mark degraded
        offline_after: seconds without heartbeat to mark offline
        callback: called with WatchdogEvent on status change
        min_free_disk_gb: #892 floor — a node whose LAST REPORTED disk_free_gb
            sits below this is marked "degraded" (excluded from scheduling)
            with the reason recorded, and restored automatically once a later
            report is back above the floor. 0 disables the disk rule entirely;
            a node that never reported disk (older worker → disk_free_gb None)
            is never affected — the pre-#892 staleness-only behaviour holds.
    """

    def __init__(
        self,
        registry: WatchdogRegistry,
        check_interval: float = 5.0,
        degraded_after: float = 15.0,
        offline_after: float = 30.0,
        callback: Optional[Callable[[WatchdogEvent], None]] = None,
        min_free_disk_gb: float = 0.0,
        max_cpu_load: float = 0.0,
    ):
        self._registry = registry
        self._check_interval = check_interval
        self._degraded_after = degraded_after
        self._offline_after = offline_after
        self._min_free_disk_gb = float(min_free_disk_gb or 0.0)
        # #879: main-side CPU ceiling consulted on each check against the
        # node's LAST REPORTED cpu_load_pct (0 = rule disabled; lane-backlog
        # and duplicate-executor rules are threshold-free and always armed).
        self._max_cpu_load = float(max_cpu_load or 0.0)
        self._callback = callback
        self._running = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the watchdog loop in a background thread."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="cluster-watchdog"
        )
        self._thread.start()
        logger.info(
            "watchdog started: check_interval=%.1fs degraded_after=%.1fs offline_after=%.1fs",
            self._check_interval,
            self._degraded_after,
            self._offline_after,
        )

    def stop(self) -> None:
        """Stop the watchdog loop."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("watchdog stopped")

    def update_intervals(
        self,
        check_interval: float,
        degraded_after: float,
        offline_after: float,
    ) -> None:
        """Dynamically update timing parameters."""
        self._check_interval = check_interval
        self._degraded_after = degraded_after
        self._offline_after = offline_after

    def check_now(self) -> List[WatchdogEvent]:
        """Run a single check cycle and return events. Useful for testing."""
        return self._check()

    def _loop(self) -> None:
        while self._running:
            self._check()
            self._stop_event.wait(timeout=self._check_interval)

    def disk_rule(self) -> float:
        """Effective #892 floor in GB (0 = disabled)."""
        return self._min_free_disk_gb

    def set_min_free_disk_gb(self, value: float) -> None:
        self._min_free_disk_gb = float(value or 0.0)

    def _check(self) -> List[WatchdogEvent]:
        from .disk_preflight import is_below_floor  # local: keep import cheap
        from .health_selfreport import health_reason  # #879, same pattern

        now = datetime.utcnow()
        nodes = self._registry.get_all_heartbeat_nodes()
        events: List[WatchdogEvent] = []

        for node in nodes:
            elapsed = (now - node.last_heartbeat).total_seconds()
            reason = ""
            if elapsed >= self._offline_after:
                new_status = "offline"
            elif elapsed >= self._degraded_after:
                new_status = "degraded"
                reason = f"heartbeat stale {elapsed:.0f}s"
            elif is_below_floor(node.disk_free_gb, self._min_free_disk_gb):
                # #892: fresh heartbeat, but the worker REPORTED disk below the
                # floor — degrade it (the scheduler only ever picks online
                # nodes, so this excludes it from dispatch without touching it
                # further). Older workers never report disk → rule can't fire.
                new_status = "degraded"
                reason = (
                    f"disk below floor: {node.disk_free_gb:.2f} GB free "
                    f"< {self._min_free_disk_gb:.1f} GB "
                    f"(node.min_free_disk_gb, shared/claude-plugins#892)"
                )
            else:
                # #879: fresh heartbeat, healthy disk — but the worker's last
                # health self-report may still condemn it (saturated CPU,
                # duplicate executor, lane backlog). Without this branch the
                # watchdog's own online force would UNDO the degradation the
                # heartbeat path applied, within one check interval.
                h_reason = health_reason(
                    getattr(node, "cpu_load_pct", None), self._max_cpu_load,
                    getattr(node, "lane_count", None),
                    int(getattr(node, "max_concurrent", 0) or 0),
                    getattr(node, "duplicate_executor", None),
                )
                if h_reason:
                    new_status = "degraded"
                    reason = h_reason
                else:
                    new_status = "online"

            if new_status != node.status:
                self._registry.update_node_status(node.id, new_status, reason)
                evt = WatchdogEvent(node.id, new_status)
                events.append(evt)
                if self._callback:
                    try:
                        self._callback(evt)
                    except Exception:
                        logger.exception("watchdog callback error for node %s", node.id)

        return events
