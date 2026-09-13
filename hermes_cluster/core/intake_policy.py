"""GitLab intake policy — the RUNTIME view of what to ingest and how to rank it.

Why this module exists (hermes-factory-intake, P0):

The factory used to take its ENTIRE intake configuration from environment
variables (``GITLAB_INTAKE_PROJECT`` / ``GITLAB_INTAKE_LABEL`` / ...) wired in
the deployment manifest. Env vars fail the owner requirement "priorities and
project scope must be settable at RUNTIME, nothing configured from env vars":
changing them is a pod-restart GitOps redeploy, and a ConfigMap that a Stakater
reloader restarts is the same restart with extra steps.

The policy lives instead in the cluster's native runtime config — the
``intake.gitlab`` section of the ``kv_store`` ``cluster_config`` row, read/write
through ``GET/PUT /api/v1/config`` (the store every backend already persists:
in-memory, SQLite and Postgres alike). That is the same store the dashboard's
config editor already writes, so the business team can change it at 2am without
an engineer, a redeploy or a pod restart: the poller re-reads it every cycle
and the webhook re-reads it on every event.

Shape (all keys optional; a missing section behaves exactly like the old
env-var wiring, so an unconfigured main node does not change behavior)::

    intake:
      gitlab:
        enabled: true|false        # poller gate; unset -> GITLAB_INTAKE_TOKEN set
        endpoint: "https://gitlab.bdaya-dev.com"
        interval_seconds: 60
        requires: ["tooling"]      # capability on created tasks
        scopes:                    # what to ingest ("Auto-work everything open")
          - {type: project, path: "shared/claude-plugins", label: "hermes-factory", enabled: true}
          - {type: group,   path: "invora",               label: "",              enabled: true}
        priority:                  # ranking, resolved at intake time
          default: 3
          author_ids: {12: 0, 8: 0}    # GitLab user id -> band 0 (top)
          project_paths: {"invora": 1}  # exact scope-path -> band
          labels: {"priority::p0": 1}   # issue label -> band
          label_prefixes: ["business-now"]  # -> band 0
        dedup_scope: iid|full      # iid (default) = legacy behaviour

Label semantics: a scope with an empty/absent ``label`` ingests EVERY open
issue in its scope; a scope with a label filters to issues carrying it, which
is what the legacy ``hermes-factory`` path does.

``priority.band_for`` returns 0..5 (bands match the scheduler's ascending
sort, 0 = top). Author band always wins — it is the owner requirement that
business-team issues are queued instantly at top priority.

Pure stdlib + pydantic only: safe to import from routers, the core, tests.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

VALID_PRIORITY_BANDS = frozenset(range(0, 6))


class IntakeScope(BaseModel):
    """One GitLab scope the poller walks (project or group)."""

    type: str = "project"
    path: str = ""
    label: str = ""  # empty == ingest every open issue in the scope
    enabled: bool = True

    @field_validator("type")
    @classmethod
    def _check_type(cls, v: str) -> str:
        if v not in ("project", "group"):
            raise ValueError(f"scope type must be 'project' or 'group', got {v!r}")
        return v


class IntakePriority(BaseModel):
    """Runtime priority rules (bands 0..5; 0 = top, scheduler sorts ascending)."""

    default: int = 3
    author_ids: Dict[int, int] = Field(default_factory=dict)
    project_paths: Dict[str, int] = Field(default_factory=dict)
    labels: Dict[str, int] = Field(default_factory=dict)
    label_prefixes: List[str] = Field(default_factory=list)

    @field_validator("default")
    @classmethod
    def _check_default(cls, v: int) -> int:
        if v not in VALID_PRIORITY_BANDS:
            raise ValueError(f"priority.default must be 0..5, got {v}")
        return v

    @field_validator("author_ids", "project_paths", "labels")
    @classmethod
    def _check_bands(cls, v: Dict[Any, int]) -> Dict[Any, int]:
        for k, band in v.items():
            if band not in VALID_PRIORITY_BANDS:
                raise ValueError(f"priority band for {k!r} must be 0..5, got {band}")
        return v

    def band_for(
        self,
        *,
        author_id: Optional[int] = None,
        project_path: str = "",
        scope_path: str = "",
        labels: Optional[List[str]] = None,
    ) -> int:
        """Resolve the band for one issue. Author wins; then project, label,
        label prefix; else the configured default."""
        if author_id is not None and int(author_id) in self.author_ids:
            return self.author_ids[int(author_id)]
        for path in (project_path, scope_path):
            if path and path in self.project_paths:
                return self.project_paths[path]
        lbls = labels or []
        for label in lbls:
            if label in self.labels:
                return self.labels[label]
        for prefix in self.label_prefixes:
            if any(l.startswith(prefix) for l in lbls):
                return 0
        return self.default


class GitLabIntakePolicy(BaseModel):
    """The full ``intake.gitlab`` runtime policy."""

    enabled: Optional[bool] = None  # None == "not configured" -> token fallback
    endpoint: str = ""  # empty == boot default
    interval_seconds: int = 0  # 0 == boot default
    requires: List[str] = ["tooling"]  # capability, NOT the filter label (#873)
    scopes: List[IntakeScope] = Field(default_factory=list)
    priority: IntakePriority = Field(default_factory=IntakePriority)
    dedup_scope: str = "iid"  # "iid" (legacy) or "full" (project path + iid)

    # Derived by load_policy(): True when the store actually carries an
    # intake.gitlab section (vs all-defaults). Not part of the stored shape.
    configured: bool = Field(default=False, exclude=True)

    @field_validator("dedup_scope")
    @classmethod
    def _check_dedup(cls, v: str) -> str:
        if v not in ("iid", "full"):
            raise ValueError(f"dedup_scope must be 'iid' or 'full', got {v!r}")
        return v

    @field_validator("requires")
    @classmethod
    def _check_requires(cls, v: List[str]) -> List[str]:
        cleaned = [x.strip() for x in v if x and x.strip()]
        if not cleaned:
            raise ValueError("requires must name at least one capability")
        return cleaned


def policy_from_raw(store_config: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Extract the raw ``intake.gitlab`` dict from a cluster config, or None."""
    if not store_config:
        return None
    intake = store_config.get("intake")
    if not isinstance(intake, dict):
        return None
    gitlab = intake.get("gitlab")
    return gitlab if isinstance(gitlab, dict) else None


def load_policy(
    state: Any,
    *,
    default_endpoint: str = "https://gitlab.bdaya-dev.com",
    default_interval: int = 30,
) -> GitLabIntakePolicy:
    """Read + validate the live policy from the store; fall back to defaults.

    Never raises for a missing/None config (a fresh node has no stored config
    row). Raises ValueError on a MALFORMED section — validated at the write
    API, so a raise here means someone bypassed it; the poller logs and skips
    the cycle rather than crashing the process.
    """
    raw = None
    try:
        store_cfg = state.get_config() if state is not None else None
    except Exception:
        store_cfg = None
    gitlab = policy_from_raw(store_cfg)
    if gitlab is None:
        p = GitLabIntakePolicy(endpoint=default_endpoint,
                               interval_seconds=default_interval)
    else:
        p = GitLabIntakePolicy.model_validate(gitlab)
        if not p.endpoint:
            p.endpoint = default_endpoint
        if not p.interval_seconds:
            p.interval_seconds = default_interval
    # Informational: whether the store actually carried an intake.gitlab
    # section (True) vs every field being a default (False).
    p.configured = gitlab is not None
    return p
