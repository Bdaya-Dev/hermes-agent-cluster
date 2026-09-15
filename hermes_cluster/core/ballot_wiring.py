"""#912 — the carrier <-> ballot wiring module (shared/claude-plugins#894 root cause).

TWO ballot writers exist on the estate and they never talked to each other:

  (1) the CARRIER store: the fork main's tasks.ballot column, written by
      POST /tasks/{id}/block and answered by POST /tasks/{id}/answer (#894).
      Owner taps land HERE — on the task row only.
  (2) the FORMAL store: the GitLab-tier decision, written by the pack's
      decision_create (a `bdaya-decision:<id>:<opt>` machine marker appended
      to the issue description + a `status::needs-decision` label) and ticked
      by decision_resolve (✅ PM DECISION note, struck losers, label flip,
      registry line).

The gateway relay carries the QUESTION down to the phone and the ANSWER back
up to (1) — but nothing carries the answer across to (2). Measured 2026-09-15:
16 answered carriers, 4 with no formal actuation at all (the ballot boxes
never ticked, the needs-decision label never flipped, the registry line never
written) and 2 whose carrier text named a target that no formal ballot on the
linked issue matches (the mislink class). The owner's decision is authoritative
in the store nobody reads; the dashboard/lead gate keeps the question open
until the audit lane re-fires it (owner-ballot-* cancel-after-answer is by
design — the ACTUATION was always the missing half; this module makes it
mechanical).

Wiring, server-side (the carrier store is the one that holds both sides):

  * ``formal_ballot_ref(ballot)`` — parse/validate the OPTIONAL
    ``decision_ref`` ("group/proj#iid" + optional ``decision_id``) a lane
    carries on its escalation. It is the SAME link the formal tier already
    uses; the carrier just never had a slot for it.
  * on /answer success, ``answer_task`` computes a
    ``formal_actuation`` directive for the ANSWERED ballot:
      - ref absent   -> status 'needs-actuation': the relay/actuation lane is
        TOLD the decision is authoritative and not yet formal, with the exact
        decision_resolve-shaped fields (project, issue_iid, answer,
        answered_by/at, carrier task id). Loud, never silent.
      - ref present  -> status 'formal-resolvable' + decision_id when present:
        the actuation path can decision_get / decision_resolve mechanically.
  * the directive rides the response, the stored ballot row, and the
    answer-delivery brief the next lane session reads — every consumer of the
    answer now sees the formal obligation in the same shape.

This module is deliberately stdlib-only and dependency-free like
core/ballot.py, so both routers and the gateway plugin can share it.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

# "invora/backend#34" / "metaphor/bayader/bayader-devops#52" — one or more
# path segments, then #iid. Same shape as the estate's qualified refs.
_REF_RE = re.compile(r"^(?P<proj>[\w.\-]+(?:/[\w.\-]+)+)#(?P<iid>\d+)$")
_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def parse_decision_ref(raw: Any) -> Optional[Dict[str, Any]]:
    """Validate a carrier ballot's formal-ballot reference. Returns None for
    absent/blank (NOT an error — most historical ballots have none), raises
    ValueError for a MALFORMED one (the submit/block path maps that to 422:
    a half-wired ref is exactly the mislink class this closes)."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("ballot.decision_ref must be a string")
    s = raw.strip()
    if not s:
        return None
    m = _REF_RE.match(s)
    if not m:
        raise ValueError(
            f"ballot.decision_ref {raw!r} must be 'group[/subgroup]/project#<iid>' "
            "— the issue carrying the FORMAL ballot (decision_create tier)")
    return {"project": m.group("proj"), "issue_iid": int(m.group("iid")), "raw": s}


def validate_decision_id(raw: Any) -> Optional[str]:
    """A decision id ('D1', 'D14') per the formal tier's rules — identifier,
    no whitespace/colon. None when absent; ValueError when malformed."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("ballot.decision_id must be a string")
    s = raw.strip()
    if not s:
        return None
    if not _ID_RE.match(s):
        raise ValueError(f"ballot.decision_id {raw!r} must be an identifier")
    return s


def formal_actuation(ballot: Dict[str, Any], *, task_id: str = "") -> Dict[str, Any]:
    """The directive computed at ANSWER time from the ballot's fields.

    Never None: the whole point is that an answered carrier ALWAYS carries an
    explicit formal obligation — silent-drop is the defect being fixed."""
    ref = ballot.get("decision_ref") or ""
    did = ballot.get("decision_id") or ""
    out: Dict[str, Any] = {
        "state": "needs-actuation",
        "formal_ref": None,
        "decision_id": None,
        "carrier_task": task_id or ballot.get("lane_key", ""),
        "answer": ballot.get("answer") or "",
        "answered_by": ballot.get("answered_by") or "",
        "answered_at": ballot.get("answered_at") or "",
        "question": ballot.get("question") or "",
    }
    try:
        parsed = parse_decision_ref(ref)
    except ValueError:
        # A stored row can only hold what the strict submit path let in; a
        # hand-mutated row reads as "unformalizable", still loud.
        out["state"] = "unformalizable-ref"
        out["detail"] = f"decision_ref {ref!r} is malformed"
        return out
    if parsed:
        out["formal_ref"] = parsed
        out["state"] = "formal-resolvable"
    try:
        did_v = validate_decision_id(did)
    except ValueError:
        did_v = None
        out["state"] = "unformalizable-ref"
        out["detail"] = out.get("detail") or f"decision_id {did!r} is malformed"
    out["decision_id"] = did_v
    return out


def attach_formal(ballot: Dict[str, Any], task_id: str = "") -> Dict[str, Any]:
    """Write the directive onto the ballot dict (the row shape the answer
    endpoint persists and every later reader sees)."""
    ballot["formal"] = formal_actuation(ballot, task_id=task_id)
    return ballot
