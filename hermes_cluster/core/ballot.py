"""Decision ballots on the task row (shared/claude-plugins#894).

A LANES decision ballot is the question a headless worker cannot answer for
itself — the `clarify`/needs-decision escalation — recorded ON THE TASK so the
hosted gateway can render it to the owner's phone (native Telegram inline
buttons) and carry the tap back via POST /api/v1/tasks/{id}/answer.

Shape (stored as JSON on ``tasks.ballot``; None column = no ballot outstanding):

  {
    "question":   str   — the decision, phrased for a thumb-sized answer
    "options":    [str] — the choices (native clarify renders one button each
                          plus "Other (type answer)")
    "class":      "technical" | "product"
                          — OWNER ROUTING RULE 2026-09-14 (shared/claude-plugins
                            #894 note 138210): product/business questions go to
                            the Bdaya Business group, technical to the owner's
                            DM. The ballot MUST carry its class explicitly so
                            the relay never guesses; default is technical.
    "lane_key":   str   — whose lane asked (informational; the task row owns it)
    "asked_at":   str   — ISO-8601 UTC at escalation (the relay's dedup key)
    "answer":     str|None    — filled by /answer
    "answered_at": str|None   — filled by /answer
    "answered_by": str        — the owner's chat identity (Telegram user id)
  }

This module is deliberately dependency-free (stdlib + json) so both the
server routers and the worker-side executor can share the exact same
validation without dragging fastapi/pydantic onto the worker path.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

BALLOT_CLASSES = ("technical", "product")
DEFAULT_CLASS = "technical"  # owner ruling 2026-09-14: default technical
_MAX_QUESTION = 4000
_MAX_OPTION = 500
_MAX_OPTIONS = 10
_MAX_ANSWER = 4000


class BallotError(ValueError):
    """Raised for a malformed ballot; routers map it to 422."""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def validate_ballot(raw: Any) -> Dict[str, Any]:
    """Validate a full stored/received ballot dict (strict; raises BallotError)."""
    if not isinstance(raw, dict):
        raise BallotError("ballot must be an object")
    return _validate_core(raw, require_asked_at=False)


def build_ballot(
    question: str,
    options: List[str],
    cls: Optional[str] = None,
    lane_key: str = "",
) -> Dict[str, Any]:
    """Build a fresh (unanswered) ballot from a lane's escalation.

    The class default is *technical* on purpose: the owner ruling says a
    ballot must carry its class explicitly so the relay never guesses, and
    the safe guess for a fleet question is the owner's DM, not the business
    group.
    """
    ballot: Dict[str, Any] = {
        "question": (question or "").strip(),
        "options": [str(o).strip() for o in (options or [])],
        "class": cls if cls else DEFAULT_CLASS,
        "lane_key": lane_key or "",
        "asked_at": _utcnow_iso(),
        "answer": None,
        "answered_at": None,
        "answered_by": "",
    }
    return _validate_core(ballot, require_asked_at=True)


def _validate_core(b: Dict[str, Any], require_asked_at: bool) -> Dict[str, Any]:
    question = b.get("question")
    if not isinstance(question, str) or not question.strip():
        raise BallotError("ballot.question is required")
    if len(question) > _MAX_QUESTION:
        raise BallotError(f"ballot.question exceeds {_MAX_QUESTION} chars")
    options = b.get("options")
    if not isinstance(options, list) or not options:
        raise BallotError("ballot.options must be a non-empty list")
    if len(options) > _MAX_OPTIONS:
        raise BallotError(f"ballot.options exceeds {_MAX_OPTIONS} choices")
    for o in options:
        if not isinstance(o, str) or not o.strip():
            raise BallotError("ballot.options entries must be non-empty strings")
        if len(o) > _MAX_OPTION:
            raise BallotError(f"ballot.options entry exceeds {_MAX_OPTION} chars")
    cls = b.get("class") or DEFAULT_CLASS
    if cls not in BALLOT_CLASSES:
        raise BallotError(
            f"ballot.class must be one of {list(BALLOT_CLASSES)}; "
            "the relay never guesses the destination chat")
    b["class"] = cls
    if require_asked_at and not b.get("asked_at"):
        raise BallotError("ballot.asked_at missing")
    return b


def validate_answer(raw: Any) -> Optional[str]:
    """Return a clean answer string or None when invalid/empty (router → 422)."""
    if not isinstance(raw, str):
        return None
    a = raw.strip()
    if not a or len(a) > _MAX_ANSWER:
        return None
    return a


def record_answer(ballot: Dict[str, Any], answer: str,
                  answered_by: str = "") -> Dict[str, Any]:
    """Attach the owner's answer to a ballot. Raises BallotError when the
    ballot was already answered — one decision, one record; a late second tap
    must not stomp the stored verdict."""
    if ballot.get("answer") not in (None, ""):
        raise BallotError("ballot already answered")
    clean = validate_answer(answer)
    if clean is None:
        raise BallotError("answer must be a non-empty string")
    ballot["answer"] = clean
    ballot["answered_at"] = _utcnow_iso()
    ballot["answered_by"] = str(answered_by or "")[:120]
    return ballot


def parse_ballot_column(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    """Decode the tasks.ballot TEXT column; tolerate junk (never crash a read
    path on one bad row — it reads as 'no ballot', which is fail-safe: the
    relay simply has nothing to render)."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None
