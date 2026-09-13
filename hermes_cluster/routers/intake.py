"""GitLab issue → cluster-task intake — /api/v1/intake/gitlab

Two ingestion paths (both produce the same outcome: a cluster Task):
  1. Webhook: POST /api/v1/intake/gitlab/webhook — GitLab push hook payload
  2. Poll:    POST /api/v1/intake/gitlab/poll    — manual trigger; queries GitLab
             for issues labelled `tooling` created since last poll

Background poller: if `GITLAB_INTAKE_TOKEN` env var is set, a background thread
polls every 30s. Configure endpoint via `GITLAB_INTAKE_ENDPOINT` env var
(default: https://gitlab.bdaya-dev.com) and project via `GITLAB_INTAKE_PROJECT`
(default: shared%2Fclaude-plugins).

All ingested tasks carry `requires: ["tooling"]` (override with
`GITLAB_INTAKE_REQUIRES`, comma-separated) and a title prefixed with the
issue iid (e.g., `[#123] Issue title`). This is deliberately INDEPENDENT of
`GITLAB_INTAKE_LABEL`, which only filters which issues are ingested — see #873.

Webhook authentication: set `GITLAB_INTAKE_WEBHOOK_SECRET` to validate the
`X-Gitlab-Token` header. When unset, the webhook accepts any POST (logs a warning).
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import secrets
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request

from ..models import Task
from ..state import ClusterState

logger = logging.getLogger("hermes_cluster.intake")

router = APIRouter(prefix="/api/v1/intake/gitlab", tags=["intake"])

_state: Optional[ClusterState] = None
_poller: Optional["_GitLabPoller"] = None
_issue_iid_to_task_id: Dict[int, str] = {}


def init(state: ClusterState):
    global _state, _poller
    _state = state
    token = os.environ.get("GITLAB_INTAKE_TOKEN", "")
    endpoint = os.environ.get("GITLAB_INTAKE_ENDPOINT", "https://gitlab.bdaya-dev.com")
    project = os.environ.get("GITLAB_INTAKE_PROJECT", "shared%2Fclaude-plugins")
    label = os.environ.get("GITLAB_INTAKE_LABEL", "tooling")
    interval = int(os.environ.get("GITLAB_INTAKE_INTERVAL", "30"))

    webhook_secret = os.environ.get("GITLAB_INTAKE_WEBHOOK_SECRET", "")
    if not webhook_secret:
        logger.warning(
            "GITLAB_INTAKE_WEBHOOK_SECRET not set — webhook has NO auth "
            "(peer-auth does NOT gate this path; X-Gitlab-Token is its only auth). "
            "Set this env var in production."
        )

    if token:
        _poller = _GitLabPoller(
            state=state,
            token=token,
            endpoint=endpoint,
            project=project,
            label=label,
            interval=interval,
        )
        _poller.start()
        logger.info("GitLab intake poller started (project=%s, label=%s)", project, label)


# ---------------------------------------------------------------------------
# Webhook handler
# ---------------------------------------------------------------------------

@router.post("/webhook")
async def webhook(request: Request):
    """GitLab webhook receiver.

    Accepts `issue` events. For `open`/`reopen` actions on issues carrying the
    configured label, creates a cluster task. Idempotent: if a task already
    exists for this issue (by iid), returns the existing task with status="deduped".

    Authentication: if GITLAB_INTAKE_WEBHOOK_SECRET is set, validates the
    X-Gitlab-Token header using constant-time comparison.
    """
    # Validate webhook secret if configured
    webhook_secret = os.environ.get("GITLAB_INTAKE_WEBHOOK_SECRET", "")
    if webhook_secret:
        token = request.headers.get("X-Gitlab-Token", "")
        # R3-2 fix: hmac.compare_digest raises TypeError on non-ASCII str,
        # which would 500 the auth check. Compare as bytes to accept any token.
        try:
            ok = hmac.compare_digest(
                token.encode("utf-8"),
                webhook_secret.encode("utf-8"),
            )
        except (TypeError, UnicodeDecodeError):
            ok = False
        if not ok:
            logger.warning("Webhook rejected: invalid or missing X-Gitlab-Token")
            raise HTTPException(status_code=401, detail="Invalid webhook token")

    body = await request.json()
    event_type = body.get("object_kind")
    if event_type != "issue":
        return {"status": "ignored", "reason": f"event={event_type}"}

    attrs = body.get("object_attributes", {})
    action = attrs.get("action")
    labels = [lbl.get("title", "") for lbl in body.get("labels", [])]
    label = os.environ.get("GITLAB_INTAKE_LABEL", "tooling")
    if label not in labels:
        return {"status": "ignored", "reason": f"label={label} not in {labels}"}
    if action not in ("open", "reopen"):
        return {"status": "ignored", "reason": f"action={action}"}

    issue_iid = attrs.get("iid")
    # Validate iid is an integer (prevents None-key collision in dedup map).
    # type(iid) is int rejects bool (True==1 would collide with real issue #1).
    if type(issue_iid) is not int:
        logger.warning("Webhook rejected: missing or non-integer iid=%r", issue_iid)
        raise HTTPException(status_code=400, detail=f"Invalid or missing iid: {issue_iid!r}")

    title = attrs.get("title", "")
    task, is_new = _create_task_from_issue(
        issue_iid=issue_iid, title=title, label=label,
        description=attrs.get("description", "") or "",
    )
    status = "created" if is_new else "deduped"
    return {"status": status, "task_id": task.id, "task": task.model_dump(mode="json")}


# ---------------------------------------------------------------------------
# Manual poll trigger
# ---------------------------------------------------------------------------

@router.post("/poll")
async def poll():
    """Manually trigger a GitLab poll. Returns list of task IDs created."""
    if _poller is None:
        raise HTTPException(status_code=503, detail="GitLab intake poller not configured (set GITLAB_INTAKE_TOKEN)")
    try:
        created = await _poller.poll_once()
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
    issue_iid: int,
    title: str,
    label: str,
    description: str = "",
) -> tuple[Task, bool]:
    """Create a cluster task from a GitLab issue, dedup by iid.

    #872 (deeper half): the issue BODY is the brief and rides in the task's
    own `description` column; the title stays the one-line `[#iid] Issue
    title` goal. Before this the body had nowhere to go, which is exactly the
    conflation that let one lane's brief land in another lane's title.

    Returns (task, is_new) where is_new=True if a new task was created,
    False if an existing task was returned (dedup hit).
    """
    # Dedup by issue iid (module-level mapping survives within-process restarts)
    if issue_iid in _issue_iid_to_task_id:
        existing = _state.get_task(_issue_iid_to_task_id[issue_iid])
        if existing is not None:
            return existing, False

    task_id = "task_" + secrets.token_hex(8)
    task = _state.create_task(
        task_id=task_id,
        title=f"[#{issue_iid}] {title}",
        requires=_intake_requires(),
        priority=3,
        description=description or "",
    )
    _issue_iid_to_task_id[issue_iid] = task_id
    _state.trigger_pending_tasks()
    return task, True


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

class _GitLabPoller:
    """Background thread that polls GitLab for new labelled issues."""

    def __init__(
        self,
        state: ClusterState,
        token: str,
        endpoint: str,
        project: str,
        label: str,
        interval: int,
    ):
        self.state = state
        self.token = token
        self.endpoint = endpoint.rstrip("/")
        self.project = project
        self.label = label
        self.interval = interval
        self.last_poll: Optional[datetime] = None
        self.issues_seen = 0
        self.tasks_created = 0
        self._seen_iids: set = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name="gitlab-intake-poller")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self.poll_once())
                loop.close()
            except Exception:
                logger.exception("GitLab intake poll cycle failed")
            self._stop.wait(self.interval)

    async def poll_once(self) -> List[str]:
        """Query GitLab for issues labelled `self.label`, create tasks for unseen ones."""
        url = f"{self.endpoint}/api/v4/projects/{self.project}/issues"
        params = {
            "labels": self.label,
            "state": "opened",
            "per_page": 50,
            "order_by": "created_at",
            "sort": "asc",
        }
        headers = {"PRIVATE-TOKEN": self.token}
        created_ids: List[str] = []
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            issues = resp.json()

        self.last_poll = datetime.now(timezone.utc)
        for issue in issues:
            iid = issue["iid"]
            self.issues_seen += 1
            if iid in self._seen_iids:
                continue
            # Check module-level dedup map (shared with webhook path)
            if iid in _issue_iid_to_task_id:
                self._seen_iids.add(iid)
                continue
            self._seen_iids.add(iid)
            task, is_new = _create_task_from_issue(
                issue_iid=iid,
                title=issue["title"],
                label=self.label,
                # #872: the issue body IS the brief — give it the column it
                # now has instead of leaving it nowhere.
                description=issue.get("description", "") or "",
            )
            if is_new:
                self.tasks_created += 1
                created_ids.append(task.id)
        return created_ids
