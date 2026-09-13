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
        endpoint: "https://gitlab.bdaya-dev.com"   # ALLOWLIST-VALIDATED (see below)
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
        allowed_endpoints: []      # FILE-SEED ONLY (see below) — extra hosts

Label semantics: a scope with an empty/absent ``label`` ingests EVERY open
issue in its scope; a scope with a label filters to issues carrying it, which
is what the legacy ``hermes-factory`` path does.

``priority.band_for`` returns 0..5 (bands match the scheduler's ascending
sort, 0 = top). Author band always wins — it is the owner requirement that
business-team issues are queued instantly at top priority.

ENDPOINT IS A CREDENTIAL-BOUNDARY FIELD (PR#36 review, finding 2): the
policy ``endpoint`` becomes the base URL for every GitLab API call the poller
makes, and the poller authenticates those calls with ``GITLAB_INTAKE_TOKEN``
(a real PAT). A runtime-writable URL is therefore a runtime-writable
credential destination. So ``endpoint`` is validated against a host
allowlist whose default contains ONLY the boot/env endpoint
(GITLAB_INTAKE_ENDPOINT); extra hosts can enter the allowlist ONLY through
the authenticated config-file seed (``intake.gitlab.allowed_endpoints``,
an infra-github change — never through the policy API itself). Nothing ever
keeps an un-allowlisted endpoint: a rejected value falls back to the boot
default at load time and is a loud 422 at the write gate, so even a
hand-crafted store row cannot point the token at a foreign host. The
allowlist is never echoed to a policy caller — the write gate fails with a
generic message so the privileged surface cannot be used to enumerate hosts.

Pure stdlib + pydantic only: safe to import from routers, the core, tests.
"""

from __future__ import annotations

import os
import urllib.parse
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

VALID_PRIORITY_BANDS = frozenset(range(0, 6))

DEFAULT_GITLAB_ENDPOINT = "https://gitlab.bdaya-dev.com"


def _endpoint_host(value: str) -> str:
    """Normalized hostname of an endpoint URL ('' when it has none)."""
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError:
        return ""
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    host = (parsed.hostname or "").lower().rstrip(".")
    if host and parsed.port not in (None,):
        # Port is part of the destination identity — keep it explicit.
        return f"{host}:{parsed.port}"
    return host


def _allowed_endpoint_hosts(boot_default: str = "") -> set:
    """Hosts a policy endpoint may point the GitLab PAT at.

    Default = ONLY the boot/env endpoint (GITLAB_INTAKE_ENDPOINT, else the
    production constant), never wildcarded. Extra hosts enter via
    GITLAB_INTAKE_ALLOWED_ENDPOINTS, which is read exclusively from the
    authenticated config-FILE seed (deployment tree, infra-github reviewed)
    and re-exported into the env by the deployment wiring — the runtime
    policy API has no path to it.
    """
    hosts = set()
    effective_default = (boot_default
                         or os.environ.get("GITLAB_INTAKE_ENDPOINT", "")
                         or DEFAULT_GITLAB_ENDPOINT)
    for raw in (effective_default, os.environ.get("GITLAB_INTAKE_ALLOWED_ENDPOINTS", "")):
        for part in (raw or "").split(","):
            part = part.strip()
            if not part:
                continue
            host = _endpoint_host(part if "://" in part else "https://" + part)
            if host:
                hosts.add(host)
    return hosts


def _sanitize_endpoint(value: str, *, strict: bool = False) -> str:
    """Return the endpoint unchanged if allowlisted, '' (fall back to boot
    default) otherwise; raise ValueError when strict (write-path).

    Empty stays empty — empty means 'use the boot default', which is by
    construction the operator's own chosen GitLab.
    """
    if not value:
        return ""
    host = _endpoint_host(value)
    allowed = _allowed_endpoint_hosts()
    if host and host in allowed:
        return value
    if strict:
        raise ValueError(
            "endpoint must be the boot-configured GitLab endpoint")
    return ""


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
    endpoint: str = ""  # empty == boot default; non-empty must be allowlisted
    interval_seconds: int = 0  # 0 == boot default
    requires: List[str] = ["tooling"]  # capability, NOT the filter label (#873)
    scopes: List[IntakeScope] = Field(default_factory=list)
    priority: IntakePriority = Field(default_factory=IntakePriority)
    dedup_scope: str = "iid"  # "iid" (legacy) or "full" (project path + iid)

    # File-seed-only: extra hosts allowed for `endpoint` (see module
    # docstring). Loaded into the env at boot by the file-seed path; never
    # writable through the runtime policy API (routers/intake.py strips it
    # from every write body).
    allowed_endpoints: List[str] = Field(default_factory=list)

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

    @field_validator("endpoint")
    @classmethod
    def _check_endpoint(cls, v: str) -> str:
        # LOAD-path sanitizer: never raise here (a malformed legacy store row
        # must not crash load_policy); fall back to '' == boot default.
        # The WRITE path re-validates strictly in routers/intake.py so a bad
        # endpoint is a loud 422 at the operator, not a silent revert.
        return _sanitize_endpoint(v, strict=False)


def policy_from_raw(store_config: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Extract the raw ``intake.gitlab`` dict from a cluster config, or None."""
    if not store_config:
        return None
    intake = store_config.get("intake")
    if not isinstance(intake, dict):
        return None
    gitlab = intake.get("gitlab")
    return gitlab if isinstance(gitlab, dict) else None


def validate_policy_write(gitlab_section: Dict[str, Any]) -> "GitLabIntakePolicy":
    """Strict pre-persist validation for the PUT /policy write path.

    Same shape rules as model validation PLUS the endpoint allowlist as a
    hard error (the load-path sanitizer would only silently fall back, which
    would leave an operator thinking their endpoint took effect). Raises
    ValueError with a GENERIC message — the response must not enumerate the
    allowlist (the caller may be an attacker probing the surface).
    """
    endpoint = gitlab_section.get("endpoint") or ""
    if endpoint and _endpoint_host(endpoint) not in _allowed_endpoint_hosts():
        raise ValueError("endpoint is not allowed for runtime configuration")
    return GitLabIntakePolicy.model_validate(gitlab_section)


def load_policy(
    state: Any,
    *,
    default_endpoint: str = DEFAULT_GITLAB_ENDPOINT,
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
        # File seeds may carry the extra-host allowlist; lift it into the
        # env view BEFORE validation so a legitimately seeded endpoint passes
        # the field validator. (Runtime PUT bodies are stripped of this key
        # in routers/intake.py, so the only writer is the boot-time seed.)
        extra = gitlab.get("allowed_endpoints")
        if isinstance(extra, list) and extra:
            os.environ.setdefault(
                "GITLAB_INTAKE_ALLOWED_ENDPOINTS", ",".join(str(x) for x in extra))
        p = GitLabIntakePolicy.model_validate(gitlab)
        if not p.interval_seconds:
            p.interval_seconds = default_interval
    # A wiped endpoint (allowlist reject on a hand-crafted store row, or the
    # boot default not yet in env) falls back to the boot default — which is
    # by construction in the allowlist. The PAT can never be loaded toward
    # a host outside it (PR#36 finding 2).
    if not p.endpoint:
        p.endpoint = default_endpoint
    # Informational: whether the store actually carried an intake.gitlab
    # section (True) vs every field being a default (False).
    p.configured = gitlab is not None
    return p
