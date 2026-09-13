"""GitLab issue → cluster-task intake — /api/v1/intake/gitlab

Two ingestion paths (both produce the same outcome: a cluster Task):
  1. Webhook: POST /api/v1/intake/gitlab/webhook — GitLab push hook payload
  2. Poll:    POST /api/v1/intake/gitlab/poll    — manual trigger; runs one
             policy cycle now

RUNTIME POLICY (hermes-factory-intake): what to ingest and how to rank it
lives in the cluster's native runtime config (kv_store `cluster_config`,
section ``intake.gitlab``) — changeable via GET/PUT /api/v1/intake/gitlab/policy
without any redeploy; the poller re-reads it every cycle. Env vars remain ONLY
as bootstrap defaults for an UNconfigured node (legacy behavior byte-for-byte)
and as the secret source (the GitLab PAT itself stays an env/secret-injected
credential; a runtime store readable through the API must not hold tokens).
See core/intake_policy.py for the section shape and semantics.

POLICY ENDPOINT SECURITY (PR#36 review, findings 1 & 2):
  * WRITE (PUT) requires EITHER a valid peer-HMAC signature (the same trust
    domain every other privileged endpoint uses; scripts/
    sign_policy_request.py is the operator signing helper) OR the DEDICATED
    operator credential ``GITLAB_INTAKE_POLICY_SECRET`` — a plain header, so
    a business-team operator still changes priorities at runtime with one
    curl and no engineer. It is deliberately NOT the webhook secret: a leak
    of the secret GitLab posts to a public webhook must not hand over intake
    configuration (credential separation).
  * Policy writes are RATE LIMITED (per source IP) and AUDITED (decision,
    method, source IP, timestamp, scope count, enabled flag, credential
    class — never a secret value; logger ``hermes_cluster.intake_policy_audit``).
  * READ (GET) stays peer-auth-exempt and token-free: the response carries
    no secrets (only a token_present bool), and the operator's control loop
    is read→edit→write — gating the read by a credential an attacker would
    already need for the write adds zero security while breaking the
    unaided-operator requirement. WRITES are the privileged surface.
  * ``endpoint`` is allowlist-validated on write against the boot/env
    endpoint (+ authenticated file-seed extras); it can never be pointed at
    a third-party host, so the GitLab PAT can never be sent to one — even
    if the policy credential leaks.

DEPLOYMENT REQUIREMENTS (PR#36 round-2 review — operator-facing; the full
section lives in README.md "Deployment Requirements: GitLab intake policy
surface" and MUST be kept in sync with the notes here):
  * The per-IP rate limiter keys on ``X-Forwarded-For``'s FIRST entry (see
    ``_client_ip``), which is only meaningful behind a trusted L7 proxy that
    REWRITES the header — GCLB in the hosted deployment. Exposing the pod
    directly lets any caller spoof XFF per request and bypass per-IP
    limiting (and poison the audit's source_ip). This is defense-in-depth,
    NOT primary auth: peer-HMAC / GITLAB_INTAKE_POLICY_SECRET remain the
    real gate. The dependency is documented, deliberately NOT enforced in
    code (enforcement = behaviour change needing its own review).
  * Credential posture: with NO intake credential configured at all (fresh
    dev node: no policy secret, no webhook secret, peer-auth off), the
    policy-write surface is open BY DESIGN — the exact contract the webhook
    has always had — and every such write is audited as ``credential=none``.
    Acceptable on a trusted dev network; NOT acceptable in production,
    where GITLAB_INTAKE_POLICY_SECRET (dedicated — never the webhook
    secret) + peer-auth MUST be set so writes fail closed. ``init()`` logs
    this posture in every deployment's boot output.

Legacy env wiring (kept working when no policy is configured):
  GITLAB_INTAKE_TOKEN     enables the background poller
  GITLAB_INTAKE_ENDPOINT  default https://gitlab.bdaya-dev.com
  GITLAB_INTAKE_PROJECT   project path (URL-encoded), default shared%2Fclaude-plugins
  GITLAB_INTAKE_LABEL     filter label, default "tooling"
  GITLAB_INTAKE_INTERVAL  poll seconds, default 30
  GITLAB_INTAKE_REQUIRES  capability for created tasks (comma-sep; #873 —
                          deliberately INDEPENDENT of the filter label)

The poller queries PROJECT-scoped /projects/:id/issues or GROUP-scoped
/groups/:id/issues per scope entry. Group fan-out is required because intake
must cover whole client groups (invora, metaphor/bayader, metaphor/morshdy);
a group path 404s on the project endpoint, so it cannot be "just" an env var.

Webhook authentication: set `GITLAB_INTAKE_WEBHOOK_SECRET` to validate the
`X-Gitlab-Token` header. When unset, the webhook accepts any POST (logs a warning).
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import secrets
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from ..core.intake_policy import (
    GitLabIntakePolicy,
    IntakeScope,
    load_policy,
    policy_from_raw,
    validate_policy_write,
)
from ..models import Task
from ..state import ClusterState

logger = logging.getLogger("hermes_cluster.intake")
# Dedicated audit logger for the policy surface (PR#36 finding 1: an audit
# trail must exist and must be greppable/ship-able independent of noise).
audit_logger = logging.getLogger("hermes_cluster.intake_policy_audit")

router = APIRouter(prefix="/api/v1/intake/gitlab", tags=["intake"])

_state: Optional[ClusterState] = None
_poller: Optional["_GitLabPoller"] = None
# Dedup map: key is str(iid) in legacy/iid mode or f"{path}#{iid}" in full
# mode. Strings so the two spellings can never collide.
_issue_dedup_to_task_id: Dict[str, str] = {}
# Backward-compat alias kept for existing tests/imports (iid-keyed only).
_issue_iid_to_task_id = _issue_dedup_to_task_id


def _boot_defaults() -> Dict[str, Any]:
    """Env-var bootstrap defaults — the LEGACY wiring, unchanged."""
    return {
        "token": os.environ.get("GITLAB_INTAKE_TOKEN", ""),
        "endpoint": os.environ.get("GITLAB_INTAKE_ENDPOINT", "https://gitlab.bdaya-dev.com"),
        "project": os.environ.get("GITLAB_INTAKE_PROJECT", "shared%2Fclaude-plugins"),
        "label": os.environ.get("GITLAB_INTAKE_LABEL", "tooling"),
        "interval": int(os.environ.get("GITLAB_INTAKE_INTERVAL", "30")),
        "requires": [r for r in (
            x.strip() for x in os.environ.get("GITLAB_INTAKE_REQUIRES", "tooling").split(",")
        ) if r],
    }


def _seed_policy_from_config_file(state: ClusterState) -> None:
    """GitOps bootstrap seed: if the cluster.yaml file carries an
    ``intake.gitlab`` section AND the runtime store has none, persist it once.

    This is the declarative initial value (the config file is the ArgoCD
    source tree — infra-github/cluster-config/hermes-main), NOT ongoing
    configuration: the moment a section exists in the store the file is
    ignored, so subsequent changes go through PUT /intake/gitlab/policy with
    no redeploy and no restart. That ordering is what satisfies "nothing
    configured from env vars / ConfigMap edits needn't restart a pod": the
    file only bootstraps an empty store.

    The file is ALSO the only writer of ``allowed_endpoints`` (the endpoint
    host allowlist beyond the boot default): it lives in the infra-github
    review path, never behind the policy API (PR#36 finding 2).
    """
    path = state.get_config_path() if hasattr(state, "get_config_path") else ""
    if not path or not os.path.exists(path):
        return
    try:
        import yaml
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        return  # a malformed seed file must not break boot; poller runs legacy
    seed = policy_from_raw(cfg)
    if seed is None:
        return
    # Lift the file-only endpoint allowlist into the env view at boot. The
    # policy PUT path strips this key from any body, so the file is the only
    # writer — an operator rotates GitLab hosts via an infra PR, not a token.
    extra = seed.get("allowed_endpoints")
    if isinstance(extra, list) and extra:
        os.environ.setdefault(
            "GITLAB_INTAKE_ALLOWED_ENDPOINTS", ",".join(str(x) for x in extra))
    existing = state.get_config() or {}
    if policy_from_raw(existing) is not None:
        return  # runtime store already authoritative — never clobber
    merged = dict(existing)
    intake = dict(merged.get("intake") or {})
    intake["gitlab"] = seed
    merged["intake"] = intake
    state.set_config(merged)
    logger.info("Seeded intake policy from config file %s (runtime store now authoritative)", path)


def init(state: ClusterState):
    global _state, _poller
    _state = state
    _seed_policy_from_config_file(state)
    defaults = _boot_defaults()

    webhook_secret = os.environ.get("GITLAB_INTAKE_WEBHOOK_SECRET", "")
    if not webhook_secret:
        logger.warning(
            "GITLAB_INTAKE_WEBHOOK_SECRET not set — webhook has NO auth "
            "(peer-auth does NOT gate this path; X-Gitlab-Token is its only auth). "
            "Set this env var in production."
        )
    if not os.environ.get("GITLAB_INTAKE_POLICY_SECRET", ""):
        logger.warning(
            "GITLAB_INTAKE_POLICY_SECRET not set — policy WRITES are only "
            "possible with a peer-HMAC signature (an operator with plain curl "
            "cannot write). Set a dedicated operator secret to keep the "
            "runtime control surface usable unaided. NEVER reuse the webhook "
            "secret here (PR#36 finding 1)."
        )
    if not _policy_write_auth_configured():
        # Deployment-posture log (PR#36 round-2 review): spell out the
        # fail-open contract in the boot output every operator sees.
        logger.warning(
            "NO intake credential configured (GITLAB_INTAKE_POLICY_SECRET / "
            "GITLAB_INTAKE_WEBHOOK_SECRET unset, peer-auth off) — the intake "
            "POLICY-WRITE surface is OPEN by design, matching the webhook's "
            "long-standing contract; every such write is audited as "
            "credential=none. Acceptable on a trusted dev node; NOT for "
            "production (see README 'Deployment Requirements: GitLab intake "
            "policy surface')."
        )

    # The poller starts when EITHER the legacy token wiring says so OR the
    # runtime policy enables it (a policy may enable polling even with the
    # token env present; `enabled: false` in a policy kills the loop but the
    # webhook path keeps working).
    if defaults["token"]:
        _poller = _GitLabPoller(state=state, defaults=defaults)
        _poller.start()
        logger.info(
            "GitLab intake poller started (legacy scope project=%s label=%s; "
            "runtime policy overrides once configured)",
            defaults["project"], defaults["label"],
        )


# ---------------------------------------------------------------------------
# Runtime policy API (the "no env vars" control surface)
# ---------------------------------------------------------------------------

@router.get("/policy")
async def get_policy(request: Request):
    """Return the LIVE intake policy as resolved per cycle, plus whether a
    runtime section exists (`configured`). Secrets are never part of it —
    only a token_present bool — so the READ surface stays peer-auth-exempt
    and un-tokened (the operator's read→edit→write loop must work from a
    plain browser/curl; gating it adds no security over the write gate).

    The effective endpoint reported here is allowlist-sanitized (see
    core/intake_policy.load_policy), so it never even displays a value that
    could redirect the PAT.
    """
    defaults = _boot_defaults()
    policy = load_policy(_state, default_endpoint=defaults["endpoint"],
                         default_interval=defaults["interval"])
    body = policy.model_dump(mode="json")
    body.pop("allowed_endpoints", None)  # deployment shape is not operator-visible
    return {
        "configured": policy.configured,
        "policy": body,
        "legacy_defaults": {
            "endpoint": defaults["endpoint"],
            "project": defaults["project"],
            "label": defaults["label"],
            "interval": defaults["interval"],
            "requires": defaults["requires"],
            "token_present": bool(defaults["token"]),
        },
    }


# --- policy-write gate: rate limiting + separation + audit (PR#36 finding 1)

# Fixed window per source IP: at most _RATE_LIMIT_COUNT write attempts per
# _RATE_LIMIT_WINDOW seconds. In-process, deny-first (an attempt is recorded
# BEFORE it is evaluated, so a burst cannot slip through the boundary).
_RATE_LIMIT_WINDOW = 60.0
_RATE_LIMIT_COUNT = 20
_policy_write_attempts: Dict[str, Deque[float]] = {}


def _reset_policy_rate_limiter() -> None:
    """Test/ops hook: drop all recorded attempts."""
    _policy_write_attempts.clear()


def _policy_write_rate_limited(ip: str, now: float) -> bool:
    dq = _policy_write_attempts.setdefault(ip, deque())
    while dq and now - dq[0] > _RATE_LIMIT_WINDOW:
        dq.popleft()
    dq.append(now)  # deny-first: record the attempt before evaluating
    return len(dq) > _RATE_LIMIT_COUNT


def _client_ip(request: Request) -> str:
    """Best-effort source IP for audit + rate limiting.

    X-Forwarded-For's FIRST entry is used when present (the main node runs
    behind GCLB in production, so request.client is the proxy). XFF is
    spoofable by the direct caller; behind GCLB it is rewritten per-hop, so
    for an off-network attacker this is the right key. Worst case it is the
    LB's own IP: a coarse shared bucket, still bounded DoS amplification.

    DEPLOYMENT REQUIREMENT (PR#36 round-2 review): this means per-IP
    limiting is only meaningful behind a trusted proxy that REWRITES
    X-Forwarded-For (GCLB). If the pod is ever exposed directly — e.g. a
    dev node behind no LB — an attacker can spoof XFF per request and
    bypass the rate limit entirely (and the audit's source_ip is
    attacker-chosen). That is defense-in-depth erosion, not an auth
    bypass: peer-HMAC / GITLAB_INTAKE_POLICY_SECRET remain the real gate.
    Enforcement via a trusted-proxy config check is deliberately NOT done
    here (behaviour change, own review); it is documented — see the module
    docstring and README "Deployment Requirements: GitLab intake policy
    surface".
    """
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    client = getattr(request, "client", None)
    return client.host if client and client.host else "unknown"


def _peer_signed_ok(request: Request, body: bytes) -> bool:
    """Verify a peer-HMAC signature on a policy write (the SAME trust domain
    auth_middleware enforces everywhere else). The policy path is on the
    public list so a plain operator GET never needs signing; a PUT may ALSO
    arrive signed, and then it is honored without the operator token —
    that is fix option (a) for free (cluster nodes / the signing helper
    scripts/sign_policy_request.py write through this same route).

    Falls back to the module-default PeerAuthState (configured by create_app
    when peer auth is enabled), mirroring the middleware's own fallback."""
    from ..core import peer_auth as peer_auth_mod

    state = peer_auth_mod.get_default_state()
    if not state.is_configured():
        return False
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    headers = {
        "X-Peer-Node": request.headers.get("X-Peer-Node", ""),
        "X-Peer-Timestamp": request.headers.get("X-Peer-Timestamp", ""),
        "X-Peer-Signature": request.headers.get("X-Peer-Signature", ""),
    }
    ok, _err = state.verify_request(
        method=request.method, path=path, body=body, headers=headers)
    return ok


def _policy_token_matches(request: Request) -> bool:
    """Dedicated operator credential for POLICY WRITES:
    GITLAB_INTAKE_POLICY_SECRET. This is NOT the webhook secret — the reuse
    of GITLAB_INTAKE_WEBHOOK_SECRET here was the finding. Constant-time
    compare on bytes, same discipline as the webhook check.

    Fail-closed: with no policy secret configured, no X-Gitlab-Token value
    ever opens the write path (only signatures do)."""
    policy_secret = os.environ.get("GITLAB_INTAKE_POLICY_SECRET", "")
    if not policy_secret:
        return False
    token = request.headers.get("X-Gitlab-Token", "")
    if not token:
        return False
    try:
        return hmac.compare_digest(token.encode("utf-8"), policy_secret.encode("utf-8"))
    except (TypeError, UnicodeEncodeError):
        return False


def _policy_write_auth_configured() -> bool:
    """True when ANY credential exists in this deployment — i.e. production
    posture. With none configured, writes are open-but-audited, the EXACT
    contract the webhook has always had (boot logs the warning); silently
    requiring nothing for the webhook while requiring a secret the operator
    cannot have is not an option."""
    from ..core import peer_auth as peer_auth_mod
    return (bool(os.environ.get("GITLAB_INTAKE_POLICY_SECRET", ""))
            or bool(os.environ.get("GITLAB_INTAKE_WEBHOOK_SECRET", ""))
            or peer_auth_mod.get_default_state().is_configured())


def _audit_policy_write(ip: str, decision: str, credential: str, *,
                        scopes: Optional[int] = None,
                        enabled: Optional[Any] = None) -> None:
    """One structured line per policy-write ATTEMPT (allow or deny): who
    (source IP + credential class), when (UTC ISO), outcome. Secret VALUES
    never appear — only the static strings 'peer-hmac' / 'policy-token' /
    'none' as credential class."""
    fields = [f"{datetime.now(timezone.utc).isoformat()}",
              f"intake-policy-write {decision}",
              f"source_ip={ip or 'unknown'}",
              f"credential={credential}"]
    if scopes is not None:
        fields.append(f"scopes={scopes}")
    if enabled is not None:
        fields.append(f"enabled={enabled}")
    line = " ".join(fields)
    (audit_logger.info if decision == "allow" else audit_logger.warning)(line)


@router.put("/policy")
async def put_policy(request: Request):
    """Write the ``intake.gitlab`` runtime policy — no restart, no redeploy.

    Auth (PR#36 fix, option (a)+(b) combined — see module docstring): a
    valid peer-HMAC signature OR the dedicated GITLAB_INTAKE_POLICY_SECRET
    presented as X-Gitlab-Token. The webhook secret authenticates NOTHING
    here. Rate-limited per source IP and audited on every attempt.

    Merged into the stored cluster config under intake.gitlab (all other
    sections preserved). Validates before persisting; a bad body is a 422
    with the field errors. The poller picks it up on its next cycle
    (and the webhook resolves priorities against it per event).
    """
    body = await request.body()
    ip = _client_ip(request)
    now = time.monotonic()

    if _policy_write_rate_limited(ip, now):
        _audit_policy_write(ip, "deny", "rate-limited")
        raise HTTPException(
            status_code=429,
            detail="policy write rate limit exceeded; retry later")

    signed = _peer_signed_ok(request, body)
    credential = "peer-hmac" if signed else (
        "policy-token" if _policy_token_matches(request) else "none")
    if not signed and credential != "policy-token":
        # Same boot contract as the webhook: with NO credential configured
        # anywhere (fresh dev node), the surface is open and the boot warning
        # is loud — but every such write is audited as unauthenticated.
        # As soon as any intake/peer credential exists, writes are fail-closed
        # and the webhook secret is NOT one of them (separation).
        if _policy_write_auth_configured():
            _audit_policy_write(ip, "deny", "none")
            raise HTTPException(
                status_code=401,
                detail="policy writes require a peer signature or the operator policy token")
        audit_logger.warning(
            "%s intake-policy-write allow source_ip=%s credential=none "
            "reason=no-intake-credential-configured",
            datetime.now(timezone.utc).isoformat(), ip or "unknown")

    try:
        payload = json.loads(body) if body else None
    except json.JSONDecodeError:
        _audit_policy_write(ip, "deny", credential)
        raise HTTPException(status_code=422, detail="body must be valid JSON")
    if not isinstance(payload, dict):
        _audit_policy_write(ip, "deny", credential)
        raise HTTPException(status_code=422, detail="body must be a JSON object")

    # Accept either the bare gitlab-policy object or an {intake:{gitlab:...}}
    # envelope, so callers can paste the same shape GET returns / the config
    # file carries.
    gitlab_section = payload
    if "gitlab" in payload and isinstance(payload["gitlab"], dict):
        gitlab_section = payload["gitlab"]
    elif "intake" in payload and isinstance(payload["intake"], dict):
        gitlab_section = payload["intake"].get("gitlab", {})
    else:
        gitlab_section = payload
    # File-only field (endpoint host allowlist) is stripped from every write
    # body — the runtime API cannot widen the credential destination set.
    gitlab_section = {k: v for k, v in gitlab_section.items()
                      if k != "allowed_endpoints"}
    try:
        policy = validate_policy_write(gitlab_section)
    except ValidationError as e:
        # jsonable detail: pydantic error ctx holds ValueError objects that
        # FastAPI's default JSON encoder would choke on (500 not 422).
        _audit_policy_write(ip, "deny", credential)
        raise HTTPException(status_code=422, detail=json.loads(e.json()))
    except ValueError as e:
        # Generic message on purpose: never echo the allowlist to the
        # (possibly hostile) caller.
        _audit_policy_write(ip, "deny", credential)
        raise HTTPException(status_code=422, detail=str(e))

    # Persist: merge into the store config, preserving every other section.
    existing = _state.get_config() or {}
    merged = dict(existing)
    intake = dict(merged.get("intake") or {})
    # exclude=True keeps `configured` (a derived field) out of storage.
    stored = policy.model_dump(mode="json", exclude={"configured"})
    stored.pop("allowed_endpoints", None)
    intake["gitlab"] = stored
    merged["intake"] = intake
    _state.set_config(merged)

    # Kill switch / start switch: if no poller thread exists but the policy
    # enables polling with a token present, the boot wiring already started
    # one; `enabled: false` is honoured per-cycle inside the loop, so no
    # thread juggling is needed here.
    logger.info(
        "GitLab intake policy updated at runtime: %d scope(s), enabled=%s",
        len(policy.scopes), policy.enabled,
    )
    _audit_policy_write(ip, "allow", credential,
                        scopes=len(policy.scopes), enabled=policy.enabled)
    return {
        "status": "saved",
        "scopes": len(policy.scopes),
        "poller_running": _poller is not None,
    }


# ---------------------------------------------------------------------------
# Webhook handler
# ---------------------------------------------------------------------------

def _webhook_secret_matches(request: Request) -> bool:
    webhook_secret = os.environ.get("GITLAB_INTAKE_WEBHOOK_SECRET", "")
    if not webhook_secret:
        return True  # unauthenticated mode (logged at init)
    token = request.headers.get("X-Gitlab-Token", "")
    # R3-2 fix: compare as BYTES — hmac.compare_digest(str,str) raises
    # TypeError on non-ASCII, which would 500 the auth check.
    try:
        return hmac.compare_digest(token.encode("utf-8"), webhook_secret.encode("utf-8"))
    except (TypeError, UnicodeEncodeError):
        return False


def _require_policy_token(request: Request) -> None:
    """GATE REMOVED BY DESIGN (PR#36 finding 1): the policy surface no longer
    accepts the webhook secret as its operator credential — credential reuse
    was the vulnerability. The write gate lives in put_policy (signature or
    GITLAB_INTAKE_POLICY_SECRET); reads carry no secrets and stay open.
    Kept as an explicit deny stub so any external caller/import cannot
    silently reintroduce the old behavior."""
    raise HTTPException(
        status_code=401,
        detail="policy endpoint no longer accepts the webhook secret")


def _in_scope(path_with_namespace: str, scopes: List[IntakeScope]) -> Optional[IntakeScope]:
    """The enabled policy scope matching this project path, or None.

    Project scopes match exactly; group scopes match any project beneath them
    (path == group or path starts with group + '/'). The FIRST match wins so
    a narrower project scope listed above a broad group scope is honoured.
    """
    for s in scopes:
        if not s.enabled or not s.path:
            continue
        if s.type == "project" and s.path == path_with_namespace:
            return s
        if s.type == "group" and (
            path_with_namespace == s.path
            or path_with_namespace.startswith(s.path + "/")
        ):
            return s
    return None


@router.post("/webhook")
async def webhook(request: Request):
    """GitLab webhook receiver — issue events.

    With NO runtime policy configured, behavior is the legacy contract: keep
    issues carrying GITLAB_INTAKE_LABEL whose open/reopen actions fire, task
    priority 3, dedup by iid.

    With a policy configured, every open/reopen issue from an ENABLED scope
    is ingested (label-filtered only when the matching scope names a label),
    and the task's priority band is resolved from the policy — author band
    first, so a business-team issue (Sami/emad) enters the queue at band 0
    and is scheduled for assignment on the spot.
    """
    if not _webhook_secret_matches(request):
        logger.warning("Webhook rejected: invalid or missing X-Gitlab-Token")
        raise HTTPException(status_code=401, detail="Invalid webhook token")

    body = await request.json()
    event_type = body.get("object_kind")
    if event_type != "issue":
        return {"status": "ignored", "reason": f"event={event_type}"}

    attrs = body.get("object_attributes", {})
    action = attrs.get("action")
    labels = [lbl.get("title", "") for lbl in body.get("labels", [])]
    if action not in ("open", "reopen"):
        return {"status": "ignored", "reason": f"action={action}"}

    issue_iid = attrs.get("iid")
    # Validate iid is an integer (prevents None-key collision in dedup map).
    # type(iid) is int rejects bool (True==1 would collide with real issue #1).
    if type(issue_iid) is not int:
        logger.warning("Webhook rejected: missing or non-integer iid=%r", issue_iid)
        raise HTTPException(status_code=400, detail=f"Invalid or missing iid: {issue_iid!r}")

    defaults = _boot_defaults()
    policy = load_policy(_state, default_endpoint=defaults["endpoint"],
                         default_interval=defaults["interval"])

    project_obj = body.get("project") or {}
    path_with_namespace = project_obj.get("path_with_namespace", "") or ""
    author = body.get("user") or {}
    author_id = author.get("id")

    if policy.configured and policy.scopes:
        # --- policy mode ---
        if policy.enabled is False:
            return {"status": "ignored", "reason": "intake disabled by policy"}
        matched = _in_scope(path_with_namespace, policy.scopes)
        if matched is None:
            return {"status": "ignored", "reason": f"project {path_with_namespace!r} not in any enabled scope"}
        if matched.label and matched.label not in labels:
            return {"status": "ignored", "reason": f"scope label {matched.label!r} not in {labels}"}
        priority = policy.priority.band_for(
            author_id=author_id if type(author_id) is int else None,
            project_path=path_with_namespace,
            scope_path=matched.path,
            labels=labels,
        )
        requires = policy.requires
        dedup_key = _dedup_key(policy.dedup_scope, path_with_namespace, issue_iid)
    else:
        # --- legacy mode: GITLAB_INTAKE_LABEL gate, band 3, iid dedup ---
        if defaults["label"] not in labels:
            return {"status": "ignored", "reason": f"label={defaults['label']} not in {labels}"}
        priority = 3
        requires = defaults["requires"]
        dedup_key = _dedup_key("iid", "", issue_iid)

    title = attrs.get("title", "")
    task, is_new = _create_task_from_issue(
        dedup_key=dedup_key,
        display_iid=issue_iid,
        title=title,
        priority=priority,
        requires=requires,
    )
    status = "created" if is_new else "deduped"
    return {"status": status, "task_id": task.id, "priority": task.priority,
            "task": task.model_dump(mode="json")}


def _dedup_key(mode: str, path: str, iid: int) -> str:
    """Dedup key. 'iid' reproduces the legacy single-project semantics;
    'full' scopes by project path — REQUIRED for group fan-out, where two
    projects in different groups routinely share issue iids (invora#420 and
    morshdy-backend#420 are different issues)."""
    if mode == "full" and path:
        return f"{path}#{iid}"
    return str(iid)


# ---------------------------------------------------------------------------
# Manual poll trigger
# ---------------------------------------------------------------------------

@router.post("/poll")
async def poll():
    """Manually trigger one intake cycle NOW. Returns list of task IDs created.

    Runs the same cycle as the background poller: runtime policy when
    configured (all enabled scopes), else the legacy env-var single project.
    """
    if _poller is None:
        raise HTTPException(
            status_code=503,
            detail="GitLab intake poller not configured (set GITLAB_INTAKE_TOKEN)")
    try:
        res = await _poller.poll_once()
        created = res["created"] if isinstance(res, dict) else res
        return {"status": "ok", "created": len(created), "task_ids": created}
    except httpx.HTTPStatusError as e:
        logger.error("GitLab poll failed: HTTP %s", e.response.status_code)
        raise HTTPException(status_code=502, detail=f"GitLab API error: {e.response.status_code}")
    except httpx.RequestError as e:
        logger.error("GitLab poll failed: %s", e)
        raise HTTPException(status_code=503, detail=f"GitLab connection error: {e}")
    except Exception as e:
        logger.exception("GitLab poll failed unexpectedly")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.get("/status")
async def status():
    """Return intake poller status."""
    if _poller is None:
        return {"configured": False}
    return {
        "configured": True,
        "last_poll": _poller.last_poll.isoformat() if _poller.last_poll else None,
        "issues_seen": _poller.issues_seen,
        "tasks_created": _poller.tasks_created,
        "last_errors": dict(_poller.last_errors),
    }


# ---------------------------------------------------------------------------
# Issue → task mapping
# ---------------------------------------------------------------------------

def _intake_requires() -> list[str]:
    """Capability a task from this path REQUIRES — not the ingest filter label.

    The label selects WHICH issues to ingest; ``requires`` selects WHICH worker
    may claim the resulting task. Conflating them mints a requirement no node
    advertises, and the task sits ``ready`` forever with nothing to surface it
    (#873: with GITLAB_INTAKE_LABEL=hermes-factory, this path had never once
    produced a claimable task).
    """
    return [r for r in (
        x.strip() for x in os.environ.get("GITLAB_INTAKE_REQUIRES", "tooling").split(",")
    ) if r]


def _create_task_from_issue(
    dedup_key: str,
    display_iid: int,
    title: str,
    priority: int = 3,
    requires: Optional[list] = None,
    state: Any = None,
) -> tuple[Task, bool]:
    """Create a cluster task from a GitLab issue, dedup by key.

    ``state`` defaults to the module-bound ClusterState (set by init()); the
    poller passes its own bound state so a directly-constructed poller (tests,
    embedded use) works without global init.

    Returns (task, is_new) where is_new=True if a new task was created,
    False if an existing task was returned (dedup hit).

    After creation the scheduler is kicked immediately (trigger + schedule),
    which is the cluster's native 'queue it now' move — an ingested issue
    does not wait for the next watchdog tick; a band-0 business-team issue
    races every other pending task for the first free capable slot.
    """
    st: Any = state if state is not None else _state
    if dedup_key in _issue_dedup_to_task_id:
        existing = st.get_task(_issue_dedup_to_task_id[dedup_key])
        if existing is not None:
            return existing, False

    task_id = "task_" + secrets.token_hex(8)
    task = st.create_task(
        task_id=task_id,
        title=f"[#{display_iid}] {title}",
        requires=requires if requires is not None else _intake_requires(),
        priority=priority,
    )
    _issue_dedup_to_task_id[dedup_key] = task_id
    st.trigger_pending_tasks()
    st.schedule_pending()
    return task, True


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

class _GitLabPoller:
    """Background thread running one intake cycle per interval.

    Each cycle re-reads the RUNTIME policy from the store: scopes, labels,
    priorities, interval, and the enabled kill switch all take effect on the
    next cycle without touching this thread. With no policy configured the
    cycle is the legacy single-project env wiring, byte-for-byte.
    """

    def __init__(self, state: ClusterState, defaults: Dict[str, Any],
                 transport: Optional[httpx.AsyncBaseTransport] = None):
        self.state = state
        self.defaults = defaults
        self.token = defaults["token"]
        # Injectable transport so tests can drive real HTTP semantics
        # (404s, pagination) without a network.
        self._transport = transport
        self.last_poll: Optional[datetime] = None
        self.issues_seen = 0
        self.tasks_created = 0
        self.last_errors: Dict[str, str] = {}
        self._seen_keys: set = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def interval(self) -> int:
        return int(self.defaults["interval"])

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name="gitlab-intake-poller")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            interval = self.interval
            try:
                loop = asyncio.new_event_loop()
                cycle = loop.run_until_complete(self.poll_once())
                interval = cycle.get("interval_seconds", self.interval)
                loop.close()
            except Exception:
                logger.exception("GitLab intake poll cycle failed")
            # Clamp so a pathological policy value cannot spin or stall the
            # loop; bounds are operational safety, not configuration.
            self._stop.wait(max(5, min(int(interval or 30), 3600)))

    def _client(self) -> httpx.AsyncClient:
        kwargs: Dict[str, Any] = {"timeout": 30}
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return httpx.AsyncClient(**kwargs)

    async def _fetch_issues(self, client: httpx.AsyncClient, base: str,
                            endpoint: str, label: str) -> List[Dict[str, Any]]:
        """Walk every page of one issues endpoint for a scope."""
        headers = {"PRIVATE-TOKEN": self.token}
        out: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = {
                "state": "opened",
                "per_page": 100,
                "page": page,
                "order_by": "created_at",
                "sort": "asc",
            }
            if label:
                params["labels"] = label
            resp = await client.get(f"{endpoint}/api/v4/{base}", params=params, headers=headers)
            resp.raise_for_status()
            batch = resp.json()
            out.extend(batch)
            if len(batch) < 100:
                break
            page += 1
            if page > 20:  # 2000 open issues in a scope is a runaway; stop paging.
                logger.warning("intake: scope %s exceeded 20 pages; truncating cycle", base)
                break
        return out

    async def poll_once(self) -> Dict[str, Any]:
        """One intake cycle: policy scopes when configured, else legacy env.

        Returns {created: [task ids], interval_seconds: effective interval}.
        """
        defaults = self.defaults
        policy = load_policy(self.state, default_endpoint=defaults["endpoint"],
                             default_interval=defaults["interval"])
        result: Dict[str, Any] = {"created": [], "interval_seconds": policy.interval_seconds}

        if policy.enabled is False:
            self.last_poll = datetime.now(timezone.utc)
            return result

        # load_policy() has already allowlist-sanitized this: a foreign host
        # is impossible here even if the store row was hand-crafted, so the
        # GITLAB_INTAKE_TOKEN can never leave the boot endpoint (PR#36 #2).
        endpoint = policy.endpoint or defaults["endpoint"]
        # Second, independent guard: belt AND braces at the actual request
        # site — if the resolved endpoint host is outside the allowlist
        # (impossible via the public API; only a monkeypatched loader could
        # get here), refuse the cycle rather than send the PAT anywhere.
        from ..core.intake_policy import _allowed_endpoint_hosts, _endpoint_host
        if _endpoint_host(endpoint) not in _allowed_endpoint_hosts(defaults["endpoint"]):
            logger.error("intake: refusing cycle — endpoint %r is outside the "
                         "allowlist; GITLAB_INTAKE_TOKEN was NOT sent", endpoint)
            self.last_errors["endpoint"] = "endpoint outside allowlist; cycle refused"
            self.last_poll = datetime.now(timezone.utc)
            return result
        # (fetch base path, label, project path, scope) tuples to walk.
        work: List[Tuple[str, str, str, Optional[IntakeScope]]] = []
        if policy.configured and policy.scopes:
            for s in policy.scopes:
                if not s.enabled or not s.path:
                    continue
                quoted = urllib.parse.quote(s.path, safe="")
                base = (f"projects/{quoted}" if s.type == "project"
                        else f"groups/{quoted}") + "/issues"
                work.append((base, s.label, s.path, s))
        else:
            # Legacy: the one env-configured project with the env label.
            work.append((f"projects/{defaults['project']}/issues",
                         defaults["label"], "", None))

        created_ids: List[str] = []
        async with self._client() as client:
            for base, label, scope_path, scope in work:
                try:
                    issues = await self._fetch_issues(client, base, endpoint, label)
                except httpx.HTTPStatusError as e:
                    # A broken scope must not starve the others.
                    self.last_errors[base] = f"HTTP {e.response.status_code}"
                    logger.error("intake: scope %s failed: HTTP %s", base, e.response.status_code)
                    continue
                except httpx.RequestError as e:
                    self.last_errors[base] = str(e)
                    logger.error("intake: scope %s failed: %s", base, e)
                    continue
                self.last_errors.pop(base, None)

                for issue in issues:
                    iid = issue["iid"]
                    self.issues_seen += 1
                    # Resolve each issue's real project path (group pages mix
                    # projects; iid dedup alone would collide across them).
                    proj_path = ""
                    refs = issue.get("references") or {}
                    full = refs.get("full", "")
                    if "#" in full:
                        proj_path = full.rsplit("#", 1)[0]
                    if not proj_path and scope and scope.type == "project":
                        proj_path = scope.path
                    author = issue.get("author") or {}
                    author_id = author.get("id")

                    if scope is not None:
                        priority = policy.priority.band_for(
                            author_id=author_id if type(author_id) is int else None,
                            project_path=proj_path,
                            scope_path=scope.path,
                            labels=issue.get("labels") or [],
                        )
                        requires = policy.requires
                        dedup_key = _dedup_key(policy.dedup_scope, proj_path, iid)
                    else:
                        priority = 3
                        requires = defaults["requires"]
                        dedup_key = _dedup_key("iid", "", iid)

                    if dedup_key in self._seen_keys:
                        continue
                    if dedup_key in _issue_dedup_to_task_id:
                        self._seen_keys.add(dedup_key)
                        continue
                    self._seen_keys.add(dedup_key)
                    task, is_new = _create_task_from_issue(
                        dedup_key=dedup_key,
                        display_iid=iid,
                        title=issue["title"],
                        priority=priority,
                        requires=requires,
                        state=self.state,
                    )
                    if is_new:
                        self.tasks_created += 1
                        created_ids.append(task.id)

        self.last_poll = datetime.now(timezone.utc)
        result["created"] = created_ids
        return result
