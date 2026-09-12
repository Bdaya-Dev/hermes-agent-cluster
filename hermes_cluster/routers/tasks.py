"""Task management endpoints — /api/v1/tasks"""

import logging
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)

from ..models import (
    DEFAULT_PRIORITY,
    SubmitTaskRequest,
    CompleteTaskRequest,
    FailTaskRequest,
    CancelTaskRequest,
    SetDependenciesRequest,
    ClaimTaskRequest,
    ReleaseTaskRequest,
    Task,
    TaskStatus,
)
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

_state: ClusterState = None
_lease_manager = None

def init(state: ClusterState, lease_manager=None):
    global _state, _lease_manager
    _state = state
    _lease_manager = lease_manager


def _generate_task_id() -> str:
    import secrets
    return "task_" + secrets.token_hex(8)


def _lane_target(lane_key: str):
    """The PR/MR or issue number a lane_key names, if it names one (#872).

    `infra-github!274-rev-b` -> "274".  `claude-plugins!912-rev2` -> "912".
    A branch-shaped key like `claude-plugins#feat/869-seat-by-paste` names no
    number (the segment after # is not digits) and returns None -- those lanes
    carry no target to disagree with.
    """
    m = re.search(r"[!#](\d+)", lane_key or "")
    return m.group(1) if m else None


def _brief_names_target(title: str, target: str) -> bool:
    """True if the brief mentions the target as a NUMBER, not a substring.

    Bounded so "274" is not satisfied by "1274" or "2740" -- an unbounded match
    would let a brief about a different PR pass while appearing to guard.
    """
    return re.search(r"(?<!\d)" + re.escape(target) + r"(?!\d)", title or "") is not None


# Instance 2 of #872: lane `infra-github!275-rev-c` got a brief that MENTIONED
# pr#275 (so the presence check above passed) while instructing the lane to
# `gh pr comment 273`. The lane silently overrode its own brief and guessed
# right -- a lane correcting its brief is luck, not a control.
#
# Two competing drafts each closed half the gap (shared/claude-plugins#872,
# notes 135712/135713): one was strict but CLI-only (missed prose actions like
# `then comment on 273`); the other was verb-broad but rejected only when the
# lane's number was absent from the whole action set (`review 275 and gh pr
# comment 273` slipped through) while false-rejecting dates and SHAs. This is
# the combination both independent reviews recommended: #32's broad verbs with
# #31's strict-per-number rule -- EVERY verb-bound number must equal the lane
# target.
#
# Three adjacency guards keep the broad verbs off ordinary prose (each one a
# measured false-rejection row from the 82-brief corpus):
#   1. `(?!\w)` after the number -- `merge 995d5600` is an SHA fragment, not a
#      ref to PR 995.
#   2. `merge`/`close` require `the` or an explicit pr/mr/issue referrer --
#      standing rules like `NEVER approve or merge` and `please merge when
#      green` carry no bound number and match nothing.
#   3. A digit-followed token after the number is a date -- `merged
#      2026-09-11` is not PR 2026.
# Bare cross-references that name no action (`Refs #872.`) stay legal: a
# mention is context, not the job instruction.
_ACTION_REF_RE = re.compile(
    r"""(?ix)
    \b(?:
      # CLI form -- the measured incident: `gh pr comment 273`
        (?: gh | glab ) \s+ (?: pr | mr | issue ) \s+
            (?: comment | review | close | merge | edit | approve | ready )
      # prose posting/reviewing actions
      | comment (?: s | ed )? \s+ on
      | post (?: s | ed )? (?: \s+ the \s+ \w+ )? \s+ (?: to | on | in )
      | verdict \s+ (?: on | for )
      | review (?: s | ed )? (?: \s+ the )? \s+ (?: pr | mr | issue ) \s* [!#]?
      # merge/close are action-ish ONLY with a referrer or `the`; the
      # past-tense forms additionally require `the` so `Closed issue #831
      # last week` stays narrative, and `NEVER approve or merge` /
      # `merge when green` carry no number to bind.
      | (?: merge | close ) \s+ (?: the \s+ )? (?: pr | mr | issue ) \s* [!#]?
      | (?: merged | closed ) \s+ the \s+ (?: pr | mr | issue )?
    )
    \s* [!#]? \s*
    ( \d+ ) (?! \w )      # guard: `995d5600` is an SHA fragment, not a ref
    """,
)


def _brief_action_numbers(brief: str) -> list:
    """Numbers the brief binds a posting/reviewing ACTION to.

    `gh pr comment 273`, `then comment on 273`, `merge the MR 273`, `review
    PR#275`, `post the verdict to 273` -- each yields its number. Verb-only
    phrases with no attached number (issue's own "NEVER approve or merge")
    yield nothing.
    """
    return _ACTION_REF_RE.findall(brief or "")


def _action_ref_mismatches(brief: str, target: str) -> list:
    """Action-bound numbers that disagree with the lane's target.

    Bounded comparison (same rule as _brief_names_target): an action on "27"
    does not satisfy a target of "274" and IS a mismatch.
    """
    return [n for n in _brief_action_numbers(brief) if not _brief_names_target(n, target)]


@router.post("")
async def submit_task(req: SubmitTaskRequest):
    # #872: a task's `title` IS its brief -- the schema has no description
    # column. On 2026-09-12 the authoring path wrote one task's brief verbatim
    # into another task's title: the reviewer lane for PR#274 received an
    # IMPLEMENTATION brief naming no PR at all. The lane did exactly as asked
    # and posted nothing; the lead read `completed` with no verdict, concluded
    # the result was lost, and paid for a re-review plus a 13-agent diagnosis.
    # There was never a lost verdict -- only a brief that did not match its lane.
    target = _lane_target(req.lane_key)
    if target and not _brief_names_target(req.title, target):
        raise HTTPException(
            status_code=422,
            detail=(
                f"brief/target disagreement (#872): lane_key {req.lane_key!r} "
                f"names target {target}, but the title -- which IS the brief -- "
                f"never mentions it. The lane would run the wrong job and report "
                f"completed. Fix the brief, or the lane_key."
            ),
        )
    # Instance 2 of #872: the brief MENTIONED its target (275) while
    # instructing `gh pr comment 273`. The presence check above accepted it;
    # only the lane overriding its own brief saved that verdict. Every number
    # the brief binds a posting/reviewing action to must equal the lane's
    # target -- broad verbs (CLI + prose), strict per number.
    if target:
        mismatches = _action_ref_mismatches(req.title, target)
        if mismatches:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"brief/action disagreement (#872): lane_key "
                    f"{req.lane_key!r} names target {target}, but the title -- "
                    f"which IS the brief -- binds a posting/reviewing action "
                    f"to {', '.join(sorted(set(mismatches)))}. The lane would "
                    f"act on the wrong target -- or silently override its own "
                    f"brief and get lucky. Fix the number in the brief, or "
                    f"the lane_key."
                ),
            )

    task_id = _generate_task_id()
    # Default only when the caller said nothing (None). 0 is a legal band —
    # the top one — and must survive to the store untouched (#866). Range
    # 0..5 is validated by SubmitTaskRequest, so out-of-band is already 422.
    priority = DEFAULT_PRIORITY if req.priority is None else req.priority
    task = _state.create_task(
        task_id,
        req.title,
        req.requires,
        priority,
        lane_key=req.lane_key,
        role=req.role,
    )
    # Promote pending → ready (tasks with no deps go to ready immediately)
    # But do NOT auto-assign to nodes — use /schedule/trigger for that
    _state.trigger_pending_tasks()
    return task


@router.get("")
async def list_tasks():
    return _state.get_all_tasks()


@router.get("/{task_id}")
async def get_task(task_id: str):
    """Read ONE task, including its deliverable (#874).

    Until now the only read path was the full listing -- so fetching a single
    lane's result meant pulling every task in the cluster and filtering client
    side, and the lead had no per-task read at all. Retrievability is the whole
    point of #874; a result you cannot address is barely stored.
    """
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    return task


@router.post("/{task_id}/complete")
async def complete_task(task_id: str, req: Optional[CompleteTaskRequest] = None):
    """Close a task, optionally carrying its deliverable (#874).

    `req` is optional so callers that post no body keep working unchanged. When
    a result IS supplied it is stored on the task row, which is what makes a
    lane's output readable from a node other than the one that produced it.
    """
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")

    # B2 fix: terminal states (completed/failed/cancelled) → 409
    # cancel_requested is allowed through (worker ack path)
    if task.status in (TaskStatus.completed, TaskStatus.failed, TaskStatus.cancelled):
        raise HTTPException(
            status_code=409,
            detail=f"task is already terminal (status={task.status.value})",
        )

    # Revoke lease
    if _lease_manager:
        lease = _lease_manager.get_by_task(task_id)
        if lease:
            _lease_manager.revoke(lease.id)

    # If task was cancel_requested, worker ack closes it to cancelled
    if task.status == TaskStatus.cancel_requested:
        _state.set_task_status(task_id, TaskStatus.cancelled, fail_reason="cancelled")
        return {"status": "cancelled"}

    # Record the deliverable BEFORE the status flip, so a reader that sees
    # `completed` never sees it without the result that completion refers to.
    stored = False
    if req is not None and req.result is not None:
        stored = _state.set_task_result(task_id, req.result)

    _state.set_task_status(task_id, TaskStatus.completed)
    # Auto-transition downstream tasks
    _trigger_downstream(task_id)
    return {"status": "completed", "result_stored": stored}


@router.post("/{task_id}/fail")
async def fail_task(task_id: str, req: FailTaskRequest = None):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    reason = req.reason if req else "failed"

    # B2 fix: terminal states (completed/failed/cancelled) → 409
    # cancel_requested is allowed through (worker ack path)
    if task.status in (TaskStatus.completed, TaskStatus.failed, TaskStatus.cancelled):
        raise HTTPException(
            status_code=409,
            detail=f"task is already terminal (status={task.status.value})",
        )

    # If task was cancel_requested, worker ack closes it to cancelled
    if task.status == TaskStatus.cancel_requested:
        _state.set_task_status(task_id, TaskStatus.cancelled, fail_reason=reason)
        return {"status": "cancelled", "blocked": []}

    # #870: a worker reporting a NON-DELIVERABLE result body (provider error
    # / echoed brief) asks for a re-queue instead of consumption. Main owns
    # the retry cap — requeue_task bumps `attempts` and returns the task to
    # ready atomically, refusing at/over the cap; on refusal (or a terminal
    # task) we fall through to the consuming failure below, so the cap is
    # enforced in exactly one place: the store's guarded UPDATE.
    if req and getattr(req, "requeue", False):
        requeued = False
        try:
            requeued = _state.requeue_task(task_id, reason=reason)
        except Exception:
            logger.exception("requeue_task failed for %s — consuming instead",
                             task_id)
        if requeued:
            # Mirror the recovery rescheduler: the queue gets an immediate
            # chance to re-place the task without waiting for an external
            # /schedule/trigger (the whole point of re-queueing is that the
            # work continues). Best-effort: the task stays `ready` either
            # way and a later trigger still picks it up.
            try:
                _state.schedule_pending()
            except Exception:
                logger.exception("schedule_pending after requeue of %s failed",
                                 task_id)
            return {"status": "requeued", "requeued": True, "reason": reason}

    # N2 fix: revoke lease on /fail (same as /complete does)
    if _lease_manager:
        lease = _lease_manager.get_by_task(task_id)
        if lease:
            _lease_manager.revoke(lease.id)

    _state.set_task_status(task_id, TaskStatus.failed, fail_reason=reason)
    # S4 fix: transitive cascade-cancel ALL non-terminal dependents
    blocked = _cascade_cancel_dependents(task_id, f"parent {task_id} failed")
    return {"status": "failed", "blocked": blocked}


def _cascade_cancel_dependents(task_id: str, reason: str) -> list:
    """S4 fix: transitively cancel ALL non-terminal dependents of task_id.

    Walks the dependent tree depth-first. Cancels any dependent that is not
    already terminal (completed/failed/cancelled/cancel_requested). Running
    dependents get their leases revoked and are set to cancel_requested.
    Returns list of all dependent task_ids found (for API compatibility).
    """
    all_dependents = _state.get_trigger_chain(task_id)  # transitive, depth-first
    for dep_id in all_dependents:
        dep_task = _state.get_task(dep_id)
        if not dep_task:
            continue
        terminal = {TaskStatus.completed, TaskStatus.failed, TaskStatus.cancelled, TaskStatus.cancel_requested}
        if dep_task.status in terminal:
            continue
        # R3-1 fix: branch on lease existence (like cancel_task S2), not status.
        # Running + unleased (scheduler-assigned) → cancelled immediately, not cancel_requested zombie.
        lease = _lease_manager.get_by_task(dep_id) if _lease_manager else None
        if lease is not None:
            _lease_manager.revoke(lease.id)
            _state.set_task_status(dep_id, TaskStatus.cancel_requested, fail_reason=reason)
        else:
            _state.set_task_status(dep_id, TaskStatus.cancelled, fail_reason=reason)
    return all_dependents


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str, req: CancelTaskRequest = None):
    """Cancel a task — two-phase for running tasks, immediate for unclaimed.

    Branching is on lease existence (S2), not status:
    - No lease → cancelled immediately (regardless of status)
    - Has lease → lease revoked, → cancel_requested; worker's next
      /complete or /fail closes to cancelled
    - Terminal (completed/failed/cancelled/cancel_requested) → 409
    After a successful cancel, transitively cancels all non-terminal dependents (S4).
    """
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")

    reason = req.reason if req else "cancelled"

    # Terminal states → 409 (ERR_TASK_NOT_CANCELABLE)
    if task.status in (
        TaskStatus.completed,
        TaskStatus.failed,
        TaskStatus.cancelled,
        TaskStatus.cancel_requested,
    ):
        raise HTTPException(
            status_code=409,
            detail=f"task is not cancelable (status={task.status.value})",
        )

    # S2 fix: branch on lease existence, not status (per #799 note 131686 item 8)
    # No lease → cancelled immediately; has lease → revoke, → cancel_requested
    lease = _lease_manager.get_by_task(task_id) if _lease_manager else None
    if lease is None:
        _state.set_task_status(task_id, TaskStatus.cancelled, fail_reason=reason)
        # S4 fix: cascade-cancel dependents
        _cascade_cancel_dependents(task_id, f"parent {task_id} cancelled")
        return {"status": "cancelled", "phase": "immediate"}

    # Has lease → revoke it, → cancel_requested
    _lease_manager.revoke(lease.id)
    _state.set_task_status(task_id, TaskStatus.cancel_requested, fail_reason=reason)
    # S4 fix: cascade-cancel dependents
    _cascade_cancel_dependents(task_id, f"parent {task_id} cancelled")
    return {"status": "cancel_requested", "phase": "pending_ack"}


@router.post("/{task_id}/unblock")
async def unblock_task(task_id: str):
    if not _state.unblock_task(task_id):
        raise HTTPException(status_code=400, detail="task not in blocked state")
    return {"status": "unblocked"}


@router.post("/{task_id}/advance")
async def manual_advance(task_id: str):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    # B1 fix: reject advance for terminal/cancel states
    if task.status in (
        TaskStatus.completed,
        TaskStatus.failed,
        TaskStatus.cancelled,
        TaskStatus.cancel_requested,
    ):
        raise HTTPException(
            status_code=409,
            detail=f"task is terminal (status={task.status.value}), cannot advance",
        )
    # Try to resolve dependencies
    if task.depends_on:
        all_done = all(
            (dep := _state.get_task(dep_id)) is not None
            and dep.status == TaskStatus.completed
            for dep_id in task.depends_on
        )
        if not all_done:
            raise HTTPException(status_code=400, detail="dependencies not met")
    _state.set_task_status(task_id, TaskStatus.ready)
    _state.schedule_pending()
    return {"status": "advanced"}


@router.post("/{task_id}/dependencies")
async def set_dependencies(task_id: str, req: SetDependenciesRequest):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    _state.set_dependencies(task_id, req.depends_on)
    return _state.get_task(task_id)


@router.get("/{task_id}/dependents")
async def get_dependents(task_id: str):
    dependents = _state.get_dependents(task_id)
    return {"task_id": task_id, "dependents": dependents, "count": len(dependents)}


@router.get("/{task_id}/trigger-chain")
async def get_trigger_chain(task_id: str):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    chain = _state.get_trigger_chain(task_id)
    return {"task_id": task_id, "chain": chain, "count": len(chain)}


def _trigger_downstream(task_id: str):
    """When a task completes, check if any dependent tasks can now be promoted."""
    dependents = _state.get_dependents(task_id)
    for dep_id in dependents:
        dep_task = _state.get_task(dep_id)
        if not dep_task or dep_task.status != TaskStatus.pending:
            continue
        # Check if all dependencies of this dependent are met
        all_done = all(
            (d := _state.get_task(d_id)) is not None
            and d.status == TaskStatus.completed
            for d_id in dep_task.depends_on
        )
        if all_done:
            _state.set_task_status(dep_id, TaskStatus.ready)
    # Then schedule any newly ready tasks
    _state.schedule_pending()


@router.post("/{task_id}/claim")
async def claim_task(task_id: str, req: ClaimTaskRequest):
    """Worker claims a task."""
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    if task.status != TaskStatus.ready:
        raise HTTPException(status_code=409, detail=f"task is not claimable (status={task.status.value})")
    if task.assigned_to is not None:
        raise HTTPException(status_code=409, detail="task is already claimed")
    _state.set_task_status(task_id, TaskStatus.running)
    # Set assigned_to directly via task object
    with _state._tasks_lock:
        t = _state._tasks[task_id]
        t.assigned_to = req.node_id
        t.updated_at = __import__("datetime").datetime.utcnow()
        t.version += 1

    # Create lease
    if _lease_manager:
        _lease_manager.create(task_id=task_id, node_id=req.node_id)

    claimed_task = _state.get_task(task_id)
    return {
        **claimed_task.model_dump(),
        "claimed_at": claimed_task.updated_at.isoformat(),
    }


@router.post("/{task_id}/release")
async def release_task(task_id: str, req: ReleaseTaskRequest):
    """Worker releases a claimed task."""
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    if task.assigned_to != req.node_id:
        raise HTTPException(status_code=403, detail="task is not assigned to this node")

    # Revoke lease
    if _lease_manager:
        lease = _lease_manager.get_by_task(task_id)
        if lease:
            _lease_manager.revoke(lease.id)

    # S7 residual fix: honor the terminality guard — if set_task_status rejects the
    # transition (e.g. cancel_requested/cancelled), return 409 instead of 200.
    transitioned = _state.set_task_status(task_id, TaskStatus.ready)
    if not transitioned:
        raise HTTPException(
            status_code=409,
            detail=f"task is terminal (status={task.status.value}), cannot release to ready",
        )

    with _state._tasks_lock:
        t = _state._tasks[task_id]
        t.assigned_to = None
        t.updated_at = __import__("datetime").datetime.utcnow()
        t.version += 1
        if req.reason:
            t.fail_reason = req.reason
    released_task = _state.get_task(task_id)
    return released_task.model_dump()
