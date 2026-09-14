"""Pydantic models matching all Go backend JSON API contracts.

Maps 45 Go structs to Python Pydantic v2 models. Each model mirrors the
corresponding Go struct with JSON tags. Duration fields are represented as
strings (e.g. "30s", "5m") matching the Go API's configJSON representation.

Organized by domain:
  1. Enums
  2. Node (cluster/node.go)
  3. Task (scheduler/taskstore.go)
  4. Lease (lease/manager.go)
  5. Sync (sync/protocol.go)
  6. Recovery (recovery/log.go, detector.go, reconnect.go)
  7. Scheduling (scheduler/scheduler.go)
  8. Workflow (workflow/resolver.go)
  9. Hooks (hooks/manager.go, payload.go)
  10. Federation (federation/registry.go, client.go)
  11. Status (status/status.go)
  12. Heartbeat (heartbeat/watchdog.go)
  13. Visualization (visualization/*.go)
  14. Config (config/config.go, api/api.go JSON variants)
  15. Capability (capability/scorer.go)
  16. API requests/responses
  17. Health / Summary
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ===========================================================================
# 1. Enums
# ===========================================================================

class NodeStatus(str, Enum):
    online = "online"
    degraded = "degraded"
    offline = "offline"


class TaskStatus(str, Enum):
    pending = "pending"
    ready = "ready"
    assigned = "assigned"
    running = "running"
    completed = "completed"
    failed = "failed"
    blocked = "blocked"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"


class LeaseStatus(str, Enum):
    active = "active"
    expired = "expired"
    revoked = "revoked"


class SyncEventType(str, Enum):
    task_created = "task_created"
    task_assigned = "task_assigned"
    task_completed = "task_completed"
    task_failed = "task_failed"
    task_cancel_requested = "task_cancel_requested"
    task_cancelled = "task_cancelled"


class EventType(str, Enum):
    task_created = "task_created"
    task_assigned = "task_assigned"
    task_completed = "task_completed"
    task_failed = "task_failed"
    node_offline = "node_offline"
    task_cancel_requested = "task_cancel_requested"
    task_cancelled = "task_cancelled"


class FederationClusterStatus(str, Enum):
    available = "available"
    unavailable = "unavailable"


class DeliveryStatus(str, Enum):
    delivered = "delivered"
    failed = "failed"
    pending = "pending"
    retrying = "retrying"


# ===========================================================================
# 2. Node (internal/cluster/node.go)
# ===========================================================================

class Node(BaseModel):
    """Go struct: cluster.Node"""
    id: str
    name: str
    capabilities: List[str] = []
    status: NodeStatus = NodeStatus.online
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    load: float = 0.0  # 0.0 - 1.0
    max_concurrent: int = 0  # max simultaneously-assigned tasks; 0 = unlimited
    # #892 factory resilience: free GB on the volume holding the worker's
    # lanes/HERMES_HOME, reported in every join/heartbeat. None = the worker
    # did not report it (older worker) — the main's disk rules never fire on
    # an absent field, so behaviour is byte-for-byte the pre-#892 one.
    disk_free_gb: Optional[float] = None
    # Why a node is not schedulable (status != online): the watchdog's
    # staleness reason, or the disk-floor reason ("disk below floor...").
    # Surfaced by GET /api/v1/nodes so the lead sees WHY without log-diving.
    status_reason: str = ""


# ===========================================================================
# 3. Task (internal/scheduler/taskstore.go)
# ===========================================================================

# Documented default band when a submitter says nothing (#866): the sort is
# ascending (ORDER BY priority, created_at), bands run 0 (top) .. 5.
DEFAULT_PRIORITY = 3


class Task(BaseModel):
    """Go struct: scheduler.Task"""
    id: str
    title: str
    requires: List[str] = []
    depends_on: List[str] = Field(default_factory=list, alias="depends_on")
    priority: int = DEFAULT_PRIORITY  # 0=top band, 1..5 documented, default 3
    status: TaskStatus = TaskStatus.pending
    assigned_to: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    version: int = 0
    fail_reason: Optional[str] = None
    # #870: deliveries re-queued back to this node after a non-deliverable
    # result (provider error / echoed brief). The retry cap lives HERE — on
    # main, the scheduler's source of truth — so an executor restart or a
    # node handoff cannot reset it. Bumped on every requeue=true /fail.
    attempts: int = 0
    lane_key: str = ""  # stateful lane identity; empty = per-task session
    role: str = "author"  # "author" (profile default model) | "reviewer" (opus tier)
    # #874: the lane's DELIVERABLE, carried on the task row so it survives the
    # node that produced it. Before this, a result was written to
    # <that node's working_dir>/hermes-results/<id>.result.md and `/complete`
    # posted an empty {} -- so a verdict produced on one machine was
    # unreadable from every other, and "produced nothing" was
    # indistinguishable from "produced something unreachable".
    result: Optional[str] = None
    # #762 grouped intake: the lane's FULL brief (a bundle task's description
    # carries every issue id + the sitting discipline; the title stays a
    # one-line summary). Empty for legacy per-issue tasks.
    description: str = ""
    # #762 grouped intake: bundle membership as "<project/path>#<iid>" issue
    # ids (empty for legacy tasks). Restart-safe: intake's DEDUP FIRST + the
    # cardinality guard read live issues from the STORE, never from a
    # process-local map alone.
    issues: List[str] = Field(default_factory=list)
    # #894: the lane's decision BALLOT (question + options + class + asked_at,
    # and the owner's answer once it lands). Set when a headless lane escalates
    # (clarify/needs-decision): the task flips to `blocked` WITH this record so
    # the hosted gateway can render it to the owner's phone and POST the tap
    # back to /answer, which stores the answer and unblocks the SAME lane
    # session. Shape + validation: hermes_cluster.core.ballot. None = no
    # ballot outstanding.
    ballot: Optional[Dict[str, Any]] = None

    model_config = {"populate_by_name": True}


# ===========================================================================
# 4. Lease (internal/lease/manager.go)
# ===========================================================================

class Lease(BaseModel):
    """Go struct: lease.Lease"""
    id: str
    task_id: str
    node_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime = Field(default_factory=datetime.utcnow)
    status: LeaseStatus = LeaseStatus.active


# ===========================================================================
# 5. Sync (internal/sync/protocol.go)
# ===========================================================================

class TaskSync(BaseModel):
    """Go struct: sync.TaskSync"""
    task_id: str
    title: str
    status: str
    assigned_to: Optional[str] = None
    version: int = 0


class SyncMessage(BaseModel):
    """Go struct: sync.SyncMessage"""
    version: int = 0
    sender_node: str = ""
    task_state: Optional[TaskSync] = None
    event_type: SyncEventType = SyncEventType.task_created
    timestamp: int = 0


class BatchSyncMessage(BaseModel):
    """Go struct: sync.BatchSyncMessage"""
    messages: List[SyncMessage] = []


# ===========================================================================
# 6. Recovery (internal/recovery/)
# ===========================================================================

class RecoveryEvent(BaseModel):
    """Go struct: recovery.RecoveryEvent"""
    id: str
    task_id: str = ""
    node_id: str = ""
    action: str = ""  # "revoke_lease", "reschedule", "mark_failed"
    status: str = ""  # "completed", "partial", "failed"
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class OfflineEvent(BaseModel):
    """Go struct: recovery.OfflineEvent"""
    node_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ReconnectConfig(BaseModel):
    """Go struct: recovery.ReconnectConfig"""
    initial_interval: timedelta = timedelta(seconds=1)
    max_interval: timedelta = timedelta(seconds=60)
    multiplier: float = 2.0


class ReconnectState(BaseModel):
    """Go struct: recovery.ReconnectState"""
    target: str
    current_interval: timedelta = timedelta(seconds=1)
    last_attempt: datetime = Field(default_factory=datetime.utcnow)
    consecutive_fails: int = 0
    connected: bool = False


# ===========================================================================
# 7. Scheduling (internal/scheduler/scheduler.go)
# ===========================================================================

class SchedulingDecision(BaseModel):
    """Go struct: scheduler.SchedulingDecision"""
    task_id: str
    task_title: str
    priority: int
    node_id: str
    score: float
    reason: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SchedulingStats(BaseModel):
    """Go struct: scheduler.SchedulingStats"""
    total_decisions: int = 0
    decisions_by_priority: Dict[int, int] = {}
    avg_wait_time_ms: float = 0.0
    failed_schedules: int = 0
    failure_reasons: Dict[str, int] = {}
    last_decisions: List[SchedulingDecision] = []


# ===========================================================================
# 8. Workflow (internal/workflow/resolver.go)
# ===========================================================================

class GraphNode(BaseModel):
    """Go struct: workflow.GraphNode"""
    id: str
    title: str
    status: str  # TaskStatus.value


class GraphEdge(BaseModel):
    """Go struct: workflow.GraphEdge"""
    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")

    model_config = {"populate_by_name": True}


class DependencyGraph(BaseModel):
    """Go struct: workflow.DependencyGraph"""
    nodes: List[GraphNode] = []
    edges: List[GraphEdge] = []


# ===========================================================================
# 9. Hooks (internal/hooks/)
# ===========================================================================

class Hook(BaseModel):
    """Go struct: hooks.Hook"""
    id: str
    url: str
    events: List[EventType] = []
    secret: Optional[str] = None  # HMAC-SHA256 secret (omitted in list responses)
    active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class HookPayload(BaseModel):
    """Go struct: hooks.Payload"""
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Any = None


class Delivery(BaseModel):
    """Go struct: hooks.Delivery — extended with delivery tracking fields."""
    id: str
    hook_id: str
    event_type: str
    url: str = ""
    payload: Dict[str, Any] = {}
    status: DeliveryStatus = DeliveryStatus.delivered
    status_code: int = 0
    error: str = ""
    attempts: int = 0
    max_attempts: int = 3
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ===========================================================================
# 10. Federation (internal/federation/)
# ===========================================================================

class RemoteCluster(BaseModel):
    """Go struct: federation.RemoteCluster"""
    id: str
    name: str
    endpoint: str
    status: FederationClusterStatus = FederationClusterStatus.available
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_ping: datetime = Field(default_factory=datetime.utcnow)
    ping_latency: float = 0.0  # seconds


class FederationStatusEntry(BaseModel):
    """Go struct: federation.StatusEntry"""
    node_id: str
    node_name: str
    status: str
    capability: str = ""
    task_id: str = ""
    task_title: str = ""


class FederationStatusSummary(BaseModel):
    """Go struct: federation.StatusSummary"""
    total_nodes: int = 0
    online_nodes: int = 0
    total_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0


class FederationStatusResponse(BaseModel):
    """Go struct: federation.StatusResponse"""
    entries: List[FederationStatusEntry] = []
    summary: FederationStatusSummary = FederationStatusSummary()


class ForwardTaskRequest(BaseModel):
    """Go struct: federation.ForwardTaskRequest"""
    title: str
    requires: List[str] = []
    idempotency_key: Optional[str] = None


class ForwardTaskResponse(BaseModel):
    """Go struct: federation.ForwardTaskResponse"""
    id: str
    title: str
    status: str


# ===========================================================================
# 11. Status (internal/status/status.go)
# ===========================================================================

class StatusEntry(BaseModel):
    """Go struct: status.StatusEntry — combined task+node view."""
    task_id: str = ""
    task_title: str = ""
    task_status: str = ""
    node_id: str = ""
    node_name: str = ""
    node_status: str = ""
    capabilities: List[str] = []
    requires: List[str] = []
    lease_status: str = ""
    fail_reason: str = ""


class StatusSummary(BaseModel):
    """Go struct: status.Summary"""
    total_nodes: int = 0
    online_nodes: int = 0
    total_tasks: int = 0
    tasks_by_status: Dict[str, int] = {}
    active_leases: int = 0


class StatusFilter(BaseModel):
    """Go struct: status.Filter"""
    node_id: str = ""
    status: str = ""
    capability: str = ""


# ===========================================================================
# 12. Heartbeat (internal/heartbeat/watchdog.go)
# ===========================================================================

class HeartbeatNode(BaseModel):
    """Go struct: heartbeat.HeartbeatNode"""
    node_id: str
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    status: str = "online"


class WatchdogEvent(BaseModel):
    """Go struct: heartbeat.Event"""
    node_id: str
    event_type: str  # "online", "degraded", "offline"


# ===========================================================================
# 13. Visualization (internal/visualization/)
# ===========================================================================

class TimelineEvent(BaseModel):
    """Go struct: visualization.TimelineEvent"""
    type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = ""
    task_id: str = ""
    description: str = ""


class ClusterTimeline(BaseModel):
    """Go struct: visualization.ClusterTimeline"""
    events: List[TimelineEvent] = []


class TopologyNode(BaseModel):
    """Go struct: visualization.TopologyNode"""
    id: str
    name: str
    status: str  # NodeStatus.value
    capabilities: List[str] = []
    load: float = 0.0
    assigned_tasks: int = 0


class TopologyTask(BaseModel):
    """Go struct: visualization.TopologyTask"""
    id: str
    title: str
    status: str  # TaskStatus.value
    assigned_to: str = ""
    dependency_count: int = 0


class TopologyEdge(BaseModel):
    """Go struct: visualization.TopologyEdge"""
    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")
    edge_type: str = Field(default="dependency", alias="type")  # "assignment" or "dependency"

    model_config = {"populate_by_name": True}


class ClusterTopology(BaseModel):
    """Go struct: visualization.ClusterTopology"""
    nodes: List[TopologyNode] = []
    tasks: List[TopologyTask] = []
    edges: List[TopologyEdge] = []


class NodeMetric(BaseModel):
    """Go struct: visualization.NodeMetric"""
    id: str
    name: str
    status: str
    tasks_assigned: int = 0
    load: float = 0.0


class TaskMetric(BaseModel):
    """Go struct: visualization.TaskMetric"""
    total: int = 0
    by_status: Dict[str, int] = {}
    completion_rate: float = 0.0


class LeaseMetric(BaseModel):
    """Go struct: visualization.LeaseMetric"""
    active_count: int = 0
    expired_count: int = 0


class ClusterMetrics(BaseModel):
    """Go struct: visualization.ClusterMetrics"""
    nodes: List[NodeMetric] = []
    tasks: TaskMetric = TaskMetric()
    leases: LeaseMetric = LeaseMetric()


# ===========================================================================
# 14. Config (internal/config/config.go + api/api.go JSON variants)
# ===========================================================================

# --- Internal config (YAML / duration objects) ---

class ClusterConfig(BaseModel):
    id: str = "cluster_default"
    role: str = "main"  # "main" or "worker"
    endpoint: str = ""
    token: str = ""


class NodeConfig(BaseModel):
    id: str = "node_main"
    name: str = "main-node"
    capabilities: List[str] = []
    # #892: floor (GB) of free disk on the volume holding HERMES_HOME / the
    # lanes dir. Below it the worker refuses to claim AND the main marks the
    # node degraded (excluded from scheduling) until it recovers. YAML only —
    # owner ruling: never an env var. 0 disables the rule entirely.
    min_free_disk_gb: float = 5.0


class ServerConfig(BaseModel):
    bind: str = "0.0.0.0"
    port: int = 8787


class LeaseConfig(BaseModel):
    ttl: timedelta = timedelta(seconds=60)
    scan_rate: timedelta = timedelta(seconds=10)


class WatchdogConfig(BaseModel):
    check_interval: timedelta = timedelta(seconds=5)
    degraded_after: timedelta = timedelta(seconds=15)
    offline_after: timedelta = timedelta(seconds=30)


class TLSConfig(BaseModel):
    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""


class HeartbeatConfig(BaseModel):
    interval: timedelta = timedelta(seconds=30)
    lease_timeout: timedelta = timedelta(seconds=120)


class ReconnectConfigYAML(BaseModel):
    initial_interval: timedelta = timedelta(seconds=1)
    max_interval: timedelta = timedelta(seconds=60)
    multiplier: float = 2.0


class FederationConfig(BaseModel):
    enabled: bool = True
    ping_interval: timedelta = timedelta(seconds=30)
    token: str = ""


class TelemetryConfig(BaseModel):
    enabled: bool = False
    exporter: str = "otlp"  # "otlp", "stdout", "none"
    endpoint: str = ""
    service_name: str = "hermes-cluster"
    sample_rate: float = 1.0
    batch_timeout: timedelta = timedelta(seconds=5)


class ClusterConfigFull(BaseModel):
    """Go struct: config.Config — full YAML config."""
    cluster: ClusterConfig = ClusterConfig()
    node: NodeConfig = NodeConfig()
    server: ServerConfig = ServerConfig()
    lease: LeaseConfig = LeaseConfig()
    watchdog: WatchdogConfig = WatchdogConfig()
    tls: TLSConfig = TLSConfig()
    heartbeat: HeartbeatConfig = HeartbeatConfig()
    reconnect: ReconnectConfigYAML = ReconnectConfigYAML()
    federation: FederationConfig = FederationConfig()
    telemetry: TelemetryConfig = TelemetryConfig()


class ValidationError(BaseModel):
    """Go struct: config.ValidationError"""
    field: str
    message: str
    suggestion: str = ""


# --- JSON API variants (string durations) ---

class ClusterConfigJSON(BaseModel):
    id: str = "cluster_default"
    role: str = "main"
    endpoint: str = ""
    token: str = ""


class NodeConfigJSON(BaseModel):
    id: str = "node_main"
    name: str = "main-node"
    capabilities: List[str] = []


class ServerConfigJSON(BaseModel):
    bind: str = "0.0.0.0"
    port: int = 8787


class LeaseConfigJSON(BaseModel):
    ttl: str = "60s"
    scan_rate: str = "10s"


class WatchdogConfigJSON(BaseModel):
    check_interval: str = "5s"
    degraded_after: str = "15s"
    offline_after: str = "30s"


class TLSConfigJSON(BaseModel):
    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""


class HeartbeatConfigJSON(BaseModel):
    interval: str = "30s"
    lease_timeout: str = "120s"


class ReconnectConfigJSON(BaseModel):
    initial_interval: str = "1s"
    max_interval: str = "60s"
    multiplier: float = 2.0


class FederationConfigJSON(BaseModel):
    enabled: bool = True
    ping_interval: str = "30s"
    token: str = ""


class TelemetryConfigJSON(BaseModel):
    enabled: bool = False
    exporter: str = "otlp"
    endpoint: str = ""
    service_name: str = "hermes-cluster"
    sample_rate: float = 1.0
    batch_timeout: str = "5s"


class ConfigJSON(BaseModel):
    """Go struct: api.configJSON — JSON API config representation.

    extra="allow" (hermes-factory-intake): unknown top-level sections — e.g.
    the runtime ``intake.gitlab`` policy — must SURVIVE a PUT /api/v1/config
    round-trip. With the default (drop-unknown), the dashboard's "save config"
    would silently wipe any runtime section the Go-shaped model doesn't know.
    """
    model_config = {"extra": "allow"}

    cluster: ClusterConfigJSON = ClusterConfigJSON()
    node: NodeConfigJSON = NodeConfigJSON()
    server: ServerConfigJSON = ServerConfigJSON()
    lease: LeaseConfigJSON = LeaseConfigJSON()
    watchdog: WatchdogConfigJSON = WatchdogConfigJSON()
    tls: TLSConfigJSON = TLSConfigJSON()
    heartbeat: HeartbeatConfigJSON = HeartbeatConfigJSON()
    reconnect: ReconnectConfigJSON = ReconnectConfigJSON()
    federation: FederationConfigJSON = FederationConfigJSON()
    telemetry: TelemetryConfigJSON = TelemetryConfigJSON()


# ===========================================================================
# 15. Capability (internal/capability/scorer.go)
# ===========================================================================

class NodeInfo(BaseModel):
    """Go struct: capability.NodeInfo — scoring input."""
    id: str
    capabilities: List[str] = []
    load: float = 0.0  # 0.0-1.0, lower is better
    heartbeat_age: float = 0.0  # seconds since last heartbeat
    active_tasks: int = 0
    max_capacity: int = 0  # 0 = unlimited
    avg_completion: float = 0.0  # seconds, lower is better


# ===========================================================================
# 16. API requests/responses (internal/api/api.go)
# ===========================================================================

class HeartbeatRequest(BaseModel):
    node_id: str
    # #892: optional free-disk reading on the volume holding the worker's
    # HERMES_HOME / lanes dir (GB). Absent (None) from an older worker's
    # payload means "no disk info" — the main keeps its exact pre-#892
    # heartbeat semantics (unconditionally online) for those.
    disk_free_gb: Optional[float] = None


class JoinRequest(BaseModel):
    node_name: str
    capabilities: List[str] = []
    endpoint: str = ""
    max_concurrent: int = 0  # 0 = unlimited; scheduler honours this ceiling
    disk_free_gb: Optional[float] = None  # #892, same absent-means-unknown rule


class JoinResponse(BaseModel):
    node_id: str
    status: str = "registered"


class UpdateCapabilitiesRequest(BaseModel):
    capabilities: List[str]


class SubmitTaskRequest(BaseModel):
    title: str
    requires: List[str] = []
    # Bands for the ascending scheduler sort (ORDER BY priority, created_at):
    # 0=top band (most urgent), 1..5 documented bands, unset -> default 3.
    # None is the not-supplied sentinel (#866): 0 used to double as it, so a
    # caller sending 0 "to mean what the sort says" was silently rewritten
    # to 3 — two bands in the wrong direction. Out-of-band values are
    # rejected with a 422 here rather than coerced in the router: a loud
    # failure beats a silent substitution.
    priority: Optional[int] = Field(default=None, ge=0, le=5)
    lane_key: str = ""  # stateful lane identity (e.g. "shared/claude-plugins#feat/x")
    role: str = "author"  # "author" | "reviewer"


class CompleteTaskRequest(BaseModel):
    """Body for POST /tasks/{id}/complete (#874).

    Optional so existing callers that post no body keep working; when present
    the deliverable is persisted on the task row and becomes readable from any
    node, not just the one that ran the lane.
    """
    result: Optional[str] = None


class FailTaskRequest(BaseModel):
    # #858: reason is REQUIRED (was defaulted to "failed"). A failure
    # recorded without a real reason is indistinguishable from the
    # busy-lane incident — failed task, error None, zero-byte result —
    # i.e. a failure the operator cannot investigate. Callers that have
    # nothing to say must say something ("no reason captured"), never
    # nothing.
    reason: str
    # #870: set by a worker reporting a NON-DELIVERABLE result body (provider
    # error / echoed brief — see agent_executor.deliverable_guard). Under
    # main's retry cap the task goes back to ready (attempts bumped) instead
    # of being consumed as failed; at/over the cap main falls back to the
    # consuming failure. Never set by plain failure callers.
    requeue: bool = False


class CancelTaskRequest(BaseModel):
    reason: str = "cancelled"


class BlockTaskRequest(BaseModel):
    """Body for POST /tasks/{id}/block (#894) — a lane's decision escalation.

    The executor posts this when a headless worker hits clarify/needs-decision:
    the ballot (question + options + explicit class) is recorded on the task
    and the task flips to blocked, ready for the gateway relay to render it to
    the owner's phone.
    """
    question: str
    options: List[str] = []
    # "technical" (default — the safe guess is the owner's DM) or "product"
    # (routed to the Bdaya Business group). Owner ruling 2026-09-14: the
    # ballot MUST carry its class so the relay never guesses.
    cls: Optional[str] = Field(default=None, alias="class")
    lane_key: str = ""

    model_config = {"populate_by_name": True}


class AnswerTaskRequest(BaseModel):
    """Body for POST /tasks/{id}/answer (#894) — the owner's decision.

    `answer` is the chosen option text (or free text from "Other");
    `answered_by` records the owner's chat identity for the audit trail.
    """
    answer: str
    answered_by: str = ""


class SetDependenciesRequest(BaseModel):
    depends_on: List[str]


class CreateLeaseRequest(BaseModel):
    task_id: str
    node_id: str
    ttl_seconds: int = 60


class RecoveryTriggerRequest(BaseModel):
    node_id: str


class FederationRegisterRequest(BaseModel):
    name: str
    endpoint: str


class FederationForwardRequest(BaseModel):
    cluster_id: str
    title: str
    requires: List[str] = []


class RegisterHookRequest(BaseModel):
    url: str
    events: List[EventType] = []
    secret: Optional[str] = None


class ClaimTaskRequest(BaseModel):
    node_id: str


class ReleaseTaskRequest(BaseModel):
    node_id: str
    reason: Optional[str] = None


# ===========================================================================
# 17. Health / Summary
# ===========================================================================

class HealthResponse(BaseModel):
    status: str = "ok"
    cluster_id: str = ""
    node_id: str = ""
    role: str = ""
    uptime_seconds: int = 0
    version: str = "python-2.0.0"
