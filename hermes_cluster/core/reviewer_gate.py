"""#919 — the reviewer-completion verdict gate (board side, server seam).

The board's success signal for a reviewer task is the STATUS WORD: the
executor POSTs /complete, main flips the row to `completed`, and every
consumer downstream — the LFP-1 queue, sweeps, dashboards, a human scanning
/api/v1/tasks — reads that as "a review happened". It need not have:

  * the lead measured (hosted main, 2026-09-15, role=reviewer AND
    status=completed): 58 completed, 41 carry a verdict token, 17 carry NONE
    (~29%) — among them shared/devops/frappe-byo-storage!6 at 0 bytes and
    shared/claude-plugins!941 at 0 bytes;
  * re-censused at 21:1xZ the same class is 46 verdict-less of 372, and the
    per-MR note check splits it: 15 have a sha-pinned verdict note ON the MR
    (REPORTING shape — the review happened, the task-result report
    evaporated), 7 have no verdict note anywhere (GATE shape — the artifact
    is genuinely unreviewed while the board says completed: knowledge-base
    !66/!71/!75/!77/!83, common-grpc!5, aggregate!261);
  * bayader-backend !180's dispatched reviewer completed TRANSCRIPT-PROMOTED
    with 0 notes on the MR — the loop stalled silently until a later lane
    noticed by hand.

#914's landing_gate already refuses to promote a LANDING on a verdict-less
completed reviewer — but only at promotion, and only for lanes shaped like a
landing. The status word still lies on the board for every other reader.
This module refuses the lie at its source: the terminal transition.

The vocabulary is #914's, as the single source of truth (imported, never
re-derived), extended with NOT-READY — the reviewer-side Stage-1 bounce
verdict a draft-MR sweep emits (shared/claude-plugins#910); a lane that
correctly bounces has delivered its contract and must not be re-queued.

Landing rows are EXEMPT by artifact shape, not by role: several live landing
tasks carry role=reviewer (measured: land-kb-102-v3,
invora-devops!793-land-pc) and legitimately deliver merge results with no
verdict word. Gating them would hold healthy merges — the false-positive
class #870's doctrine rates worse than the bug itself. The recogniser is
#914's landing_artifact() PLUS the lane-key landing shapes the board
actually carries (lanes containing a `land` WORD: 'land-kb-102-v3',
'review-land-metering-926-pr41', 'invora-devops!793-land-pc' — none of
which is a `<repo>#land-<n>` lane or a LAND-leading title, yet all are
merge-result rows, measured among the 46 verdict-less completions).

A TRANSCRIPT-PROMOTED body never counts (#871's rule made mechanical at the
gate that matters): the marker means the executor copied a stdout transcript
the lane never wrote as a deliverable — verdict words inside it are quotes
from the lane's own printing, not a verdict-grade result.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

from .landing_gate import landing_artifact, parse_reviewer_verdict

# #910's stage-1 bounce verdict, admitted alongside #914's vocabulary. The
# regex is #914's shape; this only widens the alternation.
_NOT_READY_RE = re.compile(
    r"verdict[^a-zA-Z]{0,4}\*{0,2}\s*:?\s*\*{0,2}\s*(NOT-READY)",
    re.IGNORECASE,
)

# #871: the executor's promotion marker on a result.md it wrote itself.
_PROMOTED_RE = re.compile(r"TRANSCRIPT-PROMOTED", re.IGNORECASE)

# The full vocabulary as ANCHORED TOKENS (case-sensitive). #914's label
# grammar ("Verdict: PASS") is the primary form; the live board also carries
# honest verdicts written as a header/line ("# Review Result: PASS",
# "### !923 — PASS") that no label regex matches — the hazard audit over the
# 372 completed reviewers found such rows. The widened rule admits a
# vocabulary token ONLY at line scope (start / end / em-dash tail) with
# EXACT case, never mid-prose: a body merely quoting "the expected pass"
# cannot fire it.
_LINE_TOKEN_RE = re.compile(
    r"(?m)^\s*[\W_]*[^\n]{0,80}?"
    r"(?<![A-Za-z0-9_-])"
    r"(PASS|NEEDS-CHANGES|NEEDS_CHANGES|NEEDS-HUMAN|NEEDS_HUMAN"
    r"|INCOMPLETE-ROSTER|INCOMPLETE_ROSTER|NOT-READY|NOT_READY|APPROVE|APPROVED)"
    r"(?![A-Za-z0-9_-])"
    r"[^\n]{0,120}$"
)

# Secondary rule from the hazard audit: a case-EXACT vocabulary token on a
# line that says 'verdict' at all (the line self-describes as a verdict
# line). Covers the measured grammar the windowed rule misses: emoji between
# label and word ('**Verdict:** ✅ PASS — …' inside a table row). 'verdict'
# is case-insensitive; the TOKEN stays case-sensitive, so 'the verdict was
# that it passed' never fires.
_VERDICT_LINE_RE = re.compile(r"(?i)verdict")
_TOKEN_WORD_RE = re.compile(
    r"(?<![A-Za-z0-9_-])"
    r"(PASS|NEEDS-CHANGES|NEEDS_CHANGES|NEEDS-HUMAN|NEEDS_HUMAN"
    r"|INCOMPLETE-ROSTER|INCOMPLETE_ROSTER|NOT-READY|NOT_READY|APPROVE|APPROVED)"
    r"(?![A-Za-z0-9_-])"
)


def _is_landing_row(task) -> bool:
    """Landing by #914's artifact recogniser OR a `land` WORD in the lane.

    The word-boundary shape is deliberate: '-land' / 'land-' as a token in
    the lane key marks a merge-result delivery (measured live:
    land-kb-102-v3, review-land-metering-926-pr41, invora-devops!793-land-pc
    — all role=reviewer rows whose deliverable is a merge outcome)."""
    if landing_artifact(task) is not None:
        return True
    lane = (getattr(task, "lane_key", "") or "").lower()
    return bool(re.search(r"\blands?\b", lane.replace("_", "-")))


def reviewer_verdict(result: Optional[str]) -> Optional[str]:
    """The verdict word a reviewer's delivered body carries, or None.

    None means: no verdict was delivered — the body is empty, verdict-less
    prose, a blocker report, or a #871 transcript promotion. Callers must
    treat None as NOT a completion, never as absence-of-bad-news.
    """
    if not result or not result.strip():
        return None
    if _PROMOTED_RE.search(result):
        return None
    verdict, _sha = parse_reviewer_verdict(result)
    if verdict:
        return verdict
    hits = _NOT_READY_RE.findall(result)
    if hits:
        return hits[-1].upper()
    hits = _LINE_TOKEN_RE.findall(result)
    if hits:
        v = hits[-1].upper().replace("_", "-")
        return "PASS" if v in ("APPROVE", "APPROVED") else v
    for line in result.splitlines():
        if _VERDICT_LINE_RE.search(line):
            tok = _TOKEN_WORD_RE.findall(line)
            if tok:
                v = tok[-1].upper().replace("_", "-")
                return "PASS" if v in ("APPROVE", "APPROVED") else v
    return None


def reviewer_completion_veto(task, result: Optional[str]) -> Optional[str]:
    """Reason string when this completion must be REFUSED, else None.

    Only verdict-role (non-landing) tasks are examined. The returned reason
    is what the seam records and returns — fail loud, name the gate.
    """
    role = (getattr(task, "role", "") or "").strip().lower()
    if role != "reviewer":
        return None
    if _is_landing_row(task):
        return None  # a landing delivers a merge result, not a verdict
    if reviewer_verdict(result) is None:
        return ("reviewer completed with no verdict (#919): status=completed "
                "is not evidence a review happened — the delivered result "
                "carries no PASS / NEEDS-CHANGES / NEEDS-HUMAN / "
                "INCOMPLETE-ROSTER / NOT-READY verdict word (or is a #871 "
                "transcript promotion). The completion is refused; the task "
                "is re-queued for another delivery under the retry cap.")
    return None
