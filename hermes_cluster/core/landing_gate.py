"""Landing-verdict gate (#914) — the board-side supersede check.

A LANDING task (lane_key ``<repo>#land-<n>``, or a title that leads with
'LAND') exists to merge artifact ``<repo>!<n>`` on the strength of a
reviewer PASS (#902). The dependency gate (#905) made the landing WAIT for
the reviewer task to COMPLETE — but completion is not verdict-aware: a
reviewer that delivers NEEDS-CHANGES closes its task exactly like one that
PASSes, ``_trigger_downstream`` promotes the dependent to ready, and the
fleet merges work the independent gate refused.

Measured live (hosted board, signed GET /api/v1/tasks, times exact):

  * shared/knowledge-base !104 — lander task_477eba7bf2746d0e created
    2026-09-14T13:30:08 titled 'Land MR !104 at PASS head sha 19823d37…'
    while the only COMPLETED reviewer verdict at that instant was
    NEEDS-CHANGES (task_cd7a2df25846ea91, completed 13:15:12); the
    round-2 PASS (task_1e8d90078e749196) completed 13:30:59 — 51 seconds
    AFTER the lander was promoted. depends_on was empty (#905 never even
    applied), and the MR is authored by the bot identity bdaya-agent: a
    LEAD-AUTHORED MR with no author lane — exactly this defect's title.
    The MR merged at 13:43:10.
  * morshdy-flutter !2 — lander task_025562263081b23d created
    2026-09-15T15:20:33 titled '…— Verdict: PASS' while the reviewer whose
    PASS it cites (task_c7af3466eb55abf3) completed at 15:21:49, 76
    seconds LATER; a second lander (task_107f754c3c6b744e) at 15:52:34
    shows the same batch re-minting. Both belong to second-spaced sweep
    batches (13:30:08 / 15:20:33 / the 16:36 're-summon' reviewer wave):
    the sweep mints landing rows, main promotes whatever it mints the
    moment a dependency — or the absence of one — allows, and NOTHING
    ever reads the delivered verdict.
  * hermes-gateway-image PR #6 shows the supersede order the gate must
    respect: round 1 NEEDS-CHANGES on lane '!6', round 2 PASS on lane
    '!6-rev2', landing on the rev2 PASS — 'latest completed reviewer
    round, whatever its lane twin' is the only honest reading.

The rule, shared by every promotion boundary (the stores'
``trigger_pending_tasks``, the router's ``_trigger_downstream``):

  * a landing may reach ``ready`` only when the board's LATEST completed
    reviewer verdict on its artifact is PASS pinned at a sha the landing
    brief itself names;
  * UNPARSEABLE (the #871 transcript-promoted class) and MISSING both mean
    "no verdict" — the status word 'completed' is never a pass;
  * a later round supersedes an earlier one (rejection after PASS holds;
    PASS after rejection at the current sha proceeds — the fix round's
    fresh review un-holds the artifact, so a rejection cannot strand it);
  * identity (spec 2): the lander lane may not be the artifact's reviewer
    lane, nor a lane that authored the artifact (carries it in ``issues``)
    — enforced as 409 at submit, and re-checked here because authorship
    membership can land after the submit.

A held landing stays ``pending`` with ``fail_reason`` naming the violated
gate — fail-loud, never silently promoted, and an operator keeps the
conscious override (``POST /tasks/{id}/advance`` is the only ungated path).
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Tuple

from ..models import TaskStatus

# Landing lane keys: '<repo>#land-<n>' (the #893/#902 convention).
_LAND_LANE_RE = re.compile(r"^(?P<repo>.+)#land-(?P<n>\d+)$")

# A title leading with the landing verb is also a landing (both board
# shapes exist: 'LAND ...' / 'LANDING ...').
_LAND_TITLE_RE = re.compile(r"^\W*(LAND|LANDING)\b", re.IGNORECASE)

# The sha a landing claims to act at (first hex run 7..40).
_SHA_RE = re.compile(r"(?<![0-9a-f])([0-9a-f]{7,40})(?![0-9a-f])")

# Verdict word on a reviewer's delivered result (the vocabulary the
# bdaya-gitlab land gate and #871/#892 use).
_VERDICT_RE = re.compile(
    r"verdict[^a-zA-Z]{0,4}\*{0,2}\s*:?\s*\*{0,2}\s*"
    r"(PASS|NEEDS-CHANGES|NEEDS_CHANGES|NEEDS-HUMAN|INCOMPLETE-ROSTER)",
    re.IGNORECASE,
)
# The verdict's pinned head: 'SHA: x' / '**Head sha**: x' / 'Head SHA
# reviewed:** x' — label words ('reviewed', 'at head') may sit between the
# SHA label and the colon, so skip [a-z ]{0,14} there.
_VERDICT_SHA_RE = re.compile(
    r"(?:SHA|Head sha|Head)\*{0,2}\s*[a-z]{0,12}\*{0,2}\s*:?\s*\*{0,2}\s*:??"
    r"\s*\**\s*`?([0-9a-f]{7,40})`?",
    re.IGNORECASE,
)


def landing_artifact(task) -> Optional[Tuple[str, str]]:
    """(repo, mr_iid) when the task IS a landing, else None.

    The land lane key is authoritative; a title-led landing without it
    must name '<repo>!<n>' in the title — no ref, nothing to gate (the
    gate does not guess an artifact)."""
    lane = getattr(task, "lane_key", "") or ""
    m = _LAND_LANE_RE.match(lane)
    if m:
        return m.group("repo").strip(), m.group("n")
    title = getattr(task, "title", "") or ""
    if _LAND_TITLE_RE.match(title.strip()):
        t = re.search(r"([\w./-]+)!(\d+)", title)
        if t:
            return t.group(1).rstrip("/"), t.group(2)
    return None


def _status_value(t) -> str:
    s = getattr(t, "status", None)
    return str(getattr(s, "value", s))


def parse_reviewer_verdict(result: str) -> Tuple[Optional[str], Optional[str]]:
    """(verdict, pinned_sha) from a reviewer's delivered body;
    (None, None) when it carries no verdict word (#871 class)."""
    if not result:
        return None, None
    hits = _VERDICT_RE.findall(result)
    if not hits:
        return None, None
    verdict = hits[-1].upper().replace("_", "-")
    shas = _VERDICT_SHA_RE.findall(result)
    return verdict, (shas[-1].lower() if shas else None)


def _reviewer_of(t, repo: str, n: str) -> bool:
    """Does this completed reviewer task belong to artifact '<repo>!<n>'?

    The board carries every hand-off's lane shapes for one artifact:
    '<repo>!<n>' exactly, round twins '<repo>!<n>-rev2', the bundle flow's
    '<authoring-lane>-rev' whose TITLE names 'MR !224', and full-path lanes
    ('metaphor/x/repo!2') against short land lanes ('repo#land-2'). Match
    on the PROJECT TAIL + number — the same (tail, n) collision an
    unrelated project could theoretically share is defused by the
    verdict-SHA lock: a foreign verdict pins a head sha the landing's own
    artifact would not coincidentally carry in its brief.
    """
    if (getattr(t, "role", "") or "").strip().lower() != "reviewer":
        return False
    if _status_value(t) != TaskStatus.completed.value:
        return False
    lane = getattr(t, "lane_key", "") or ""
    title = getattr(t, "title", "") or ""
    tail = repo.rstrip("/").split("/")[-1]

    # Lane carries an explicit artifact ref: '<anything>!<n>' whose project
    # tail matches the artifact's.
    if "!" in lane:
        lane_repo, _, lane_num = lane.partition("!")
        m = re.match(r"^(\d+)", lane_num)
        if m and m.group(1) == n and \
           lane_repo.rstrip("/").split("/")[-1] == tail:
            return True

    # Title names the artifact: full ref, or the bundle-flow shape
    # '[lane:<x>-rev][REVIEW <repo> MR !224]' / 'PR #224' with the repo's
    # tail present.
    if re.search(rf"(?:{re.escape(repo)}|{re.escape(tail)})!{re.escape(n)}\b",
                 title):
        return True
    if re.search(rf"\b(?:MR|PR)\s*[#!]?\s*{re.escape(n)}\b", title) \
            and (tail in lane or tail in title):
        return True
    return False


def latest_reviewer_verdict(all_tasks: List, repo: str, n: str
                            ) -> Tuple[Optional[str], Optional[str], str]:
    """(verdict|None, sha|None, reviewer_task_id) for the LATEST completed
    reviewer of the artifact across ALL its reviewer-lane shapes.

    Latest = last completed (updated_at, ties by version): a newer round
    supersedes an older verdict — the measured flow (PR#6: round-1
    NEEDS-CHANGES, round-2 rev PASS at the same head) must gate on the
    REV2 PASS, so restricting to the plain lane would strand legitimate
    landings, and 'latest completion' is the honest supersede order.
    """
    done = [t for t in all_tasks if _reviewer_of(t, repo, n)]
    if not done:
        return None, None, ""

    def key(t):
        upd = getattr(t, "updated_at", None)
        return (upd.isoformat() if upd else "", getattr(t, "version", 0) or 0)

    latest = max(done, key=key)
    verdict, sha = parse_reviewer_verdict(getattr(latest, "result", "") or "")
    return verdict, sha, latest.id


def authoring_lanes(all_tasks: List, repo: str, n: str,
                    exclude_task_id: str = "") -> set:
    """Lanes whose (other) tasks carry '<repo>!<n>' as bundle membership —
    the authoring sittings of the artifact. The candidate landing itself is
    excluded: its own issues list is a claim, not authorship evidence."""
    ref = f"{repo}!{n}"
    lanes = set()
    for t in all_tasks:
        if getattr(t, "id", "") == exclude_task_id:
            continue
        if ref in (list(getattr(t, "issues", None) or [])):
            lane = (getattr(t, "lane_key", "") or "").strip()
            if lane:
                lanes.add(lane)
    return lanes


def hold_reason(all_tasks: List, task) -> Optional[str]:
    """Why a landing task must NOT reach ready right now (None = proceed).

    The returned string is stored as ``fail_reason`` on the held task: the
    violated gate, the artifact, and the reviewer task that carries (or
    should carry) the verdict — everything an operator needs to decide
    ``/advance`` or a re-review.
    """
    art = landing_artifact(task)
    if art is None:
        return None
    repo, n = art
    lane = (getattr(task, "lane_key", "") or "").strip()
    title = getattr(task, "title", "") or ""
    target = f"{repo}!{n}"

    # Spec 2 re-check (submit refuses these; the sweep re-checks because
    # authorship membership can be recorded after the landing's submit).
    if lane == target:
        return (f"landing held (#914): the lander lane {lane!r} is the "
                f"reviewer lane of {target} — a reviewer never lands what "
                f"it reviewed (RV-1)")
    if lane and lane in authoring_lanes(all_tasks, repo, n,
                                        exclude_task_id=getattr(task, "id", "")):
        return (f"landing held (#914): lane {lane!r} authored {target} "
                f"(carries it in issues) — author != lander (RV-1)")

    verdict, vsha, rid = latest_reviewer_verdict(all_tasks, repo, n)
    if verdict is None:
        return (f"landing held (#914): no completed reviewer verdict on "
                f"{target} carries a verdict word — 'completed' is not a "
                f"pass (#871); re-review or cite the PASS note's sha")
    if verdict != "PASS":
        return (f"landing held (#914): the LATEST reviewer verdict on "
                f"{target} is {verdict} (task {rid}) — a rejected review "
                f"is never overridden by a landing brief's claim")
    if not vsha:
        return (f"landing held (#914): the PASS on {target} (task {rid}) "
                f"pins no head sha — a sha-less verdict certifies no head")
    named = {s.lower() for s in _SHA_RE.findall(title)}
    # Git-convention sha comparison (reviewer round 1, NEEDS-CHANGES):
    # landings routinely abbreviate the verdict sha ('37935ca094a683dd93cb
    # 8071e2242a5f98a18ea1' -> '37935ca0'). _SHA_RE only matches runs of
    # 7-40 hex, so prefix equality in either direction is a match within
    # git's own collision bounds — while a DIFFERENT sha still can never
    # match, which is the security property the lock exists for.
    if not any(vsha.startswith(s) or s.startswith(vsha) for s in named):
        return (f"landing held (#914): reviewer PASS pins sha {vsha} "
                f"({target}, task {rid}) but the landing names "
                f"{', '.join(sorted(named)) or '(no sha)'} — a verdict at "
                f"one head does not certify another (stale/merge-forward "
                f"supersede)")
    return None
