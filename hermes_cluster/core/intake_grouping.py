"""Stateful-lane grouping — the INTAKE side of LFP-1 (shared/claude-plugins#762).

Why this module exists
----------------------
Until #762 landed here, intake created ONE task per GitLab issue: the
measured 2026-09-13 census was 872 per-issue tasks (bands {0:46, 1:3, 3:791}),
i.e. ~35x more review gates than the ruled design needs at ~25 client repos.
The owner ruled the opposite shape, verbatim (#762, D7-stateful):

  "we don't have infinite ram and compute to solve each issue and run its
   tests separately ... AI should solve multiple issues in bulk and reuse
   the existing open app!"
  "a single bayader-flutter lane can solve 30-40 issues in one setting and
   use less review rounds even if the MR is big!"

and the economic reason: "The review gate's cost is nearly fixed per merge
request, so a 40-issue MR costs roughly what a 1-issue MR costs at the gate.
This is the single largest throughput lever in the plan."

The executor half already exists (fork PR#16 ``lane_key``/``role`` + the
``lanes`` table; bdaya-enforcement ``lane_key_guard``/``lane_registry`` per
#833 note 133037 rule #1). What was missing is HERE: intake must emit ONE
grouped lane task per bundle of candidate issues, not one task per issue.

Grouping rules (each cites its doctrine home)
---------------------------------------------
* LANE KEY (LFP-1, #762 + #833 note 133037): one long-lived lane per client ×
  repo — ``<repo>#<branch>`` (author shape, bdaya-enforcement
  ``AUTHOR_LANE_KEY_RE``). Branch comes from the repo→branch map (client
  repos work on ``env/dev``; the fork itself uses ``main``), default
  configurable. One lane key per repo, not per issue, so the executor RESUMES
  the same hermes session delivery after delivery (#833 rule #1:
  "fix rounds are deliveries into the author session").
* AB-1 (bdaya-work SKILL §AB-1 / !685): never bundle across STACKS. A GitLab
  project == one repo == one stack here, so the per-repo lane key makes the
  stack boundary structural. Bundling ACROSS ``area::*`` labels inside the one
  stack is the LFP-1 carve-out — allowed when the diff stays reviewable.
* GitLab DAG keeps ordering authority (AB-1): an issue whose ``is_blocked_by``
  blocker (``blocked_by`` links API) is still open AND outside the bundle is
  not pulled in. It stays on the waitlist and joins a later bundle once its
  blockers merge. A blocker INSIDE the bundle is fine — one lane fixes both
  in order in one session.
* D-SPAWN-1: an issue already referenced by an OPEN MR is artifact-carrying —
  never grouped (the fix exists; re-spawning it duplicates an MR).
* Skip labels: ``blocked`` / ``needs-human`` / ``needs-decision`` / ``epic``
  (configurable patterns; the live corpus uses ``status::needs-decision`` /
  ``status::needs-human`` spellings too).
* CARDINALITY GUARD (#762 title — this is what makes it real, not
  aspirational): exactly one ACTIVE author lane per lane key is a closed set
  at all times, enforced in two places:
    1. INTAKE (here): ``bundle_for_lane`` returns the existing task while a
       bundle task for that key is non-terminal — at most one queued task per
       key, across pod restarts (dedup map + the store's task list).
    2. SCHEDULER (core/scheduler.lane_busy_task_ids, applied by every store's
       ``schedule_pending_detailed``): a ready bundle task is NOT assigned
       while another task with the same lane key is active (running/assigned)
       — so the second batch waits instead of racing a second session for the
       live key, which bdaya-enforcement would DENY at spawn time.
* BUNDLE SIZE / REVIEWABILITY: ``max_bundle_size`` caps a bundle (owner
  intent: 30-40 issues). When a repo has more ready candidates than the cap,
  intake packs the cap-size batch and the remainder stays in the lane
  waitlist — it is grouped into the NEXT bundle task on the SAME lane key
  once the current bundle task is terminal (one lane, many sittings, one MR
  per sitting). Which issues join is decided by area co-bundling (§area
  packing below) and band order, never by issue number.
* PROOF IS NOT DILUTED: the bundle brief tells the lane each issue still
  needs its own scoped proof and disposition, and the MR ``Refs`` — never
  ``Closes`` — every issue (VP-1 close gate fires on merge otherwise).
* BAND 0 STAYS INSTANT (#762 sitting discipline / #886 author-band rule): a
  candidate whose author band is 0 (business team — sami-hegazi id 12,
  emadsoliman id 8) is pulled into a bundle of its OWN at band 0 that is
  queued ahead of any band>0 bundle for the same lane: bundles are ordered
  (priority, created) by the scheduler, and the lane itself serialises its
  deliveries. A band-0 issue therefore waits at most for the bundle task
  that is *already running* — exactly as it would in the per-issue model —
  and never behind a freshly-assembled 40-issue band-3 bundle.

Area packing
------------
Inside one repo bundle, issues are grouped by shared surface first
(``area::*`` label cluster — #762: "Group by shared surface, not by issue
number"), then by band, then created order; the batch is cut at
``max_bundle_size``. Unlabeled issues form their own singleton-cluster group
(AB-1: "An issue with no area::* label forms a singleton bundle") and are
padded into the batch only when the cap leaves room.

Pure stdlib + pydantic: no network, no state — the caller (routers/intake.py)
supplies GitLab payloads, the lane view, and the open-MR ref set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pydantic import BaseModel, Field, field_validator

# The live instance's client repos work on env/dev (lane-operating-shape,
# land-area-mr template "pinned at the reviewed SHA" onto the dev branch);
# override per deployment via the policy's ``lane_branches`` map.
DEFAULT_LANE_BRANCH = "env/dev"

# Default doomed-label patterns (skip list). The brief names
# blocked/needs-human/needs-decision/epic; ``status::<same>`` spellings are
# measured on the live corpus (e.g. status::needs-decision).
DEFAULT_SKIP_LABEL_PATTERNS: Tuple[str, ...] = (
    r"^(status::)?blocked$",
    r"^(status::)?needs-human$",
    r"^(status::)?needs-decision$",
    r"^epic$",
    r"^type::epic$",
)


class GroupingConfig(BaseModel):
    """The ``intake.gitlab.grouping`` policy section — lane-bundle shape."""

    enabled: bool = False          # kill switch; default OFF keeps legacy behavior
    lane_branch: str = DEFAULT_LANE_BRANCH
    lane_branches: Dict[str, str] = Field(default_factory=dict)  # repo -> branch
    max_bundle_size: int = 40       # owner intent 30-40 issues per sitting (#762)
    # Reviewability cap on the SHAPE of the bundle, not its diff (intake
    # cannot see code): area-cluster groups per batch. A batch spanning more
    # clusters than this splits first. The lane enforces the DIFF cap at MR
    # authoring time per LFP-1 ("combined diff stays reviewable") and #762
    # ("split any bundle whose combined diff exceeds one review pass").
    max_area_clusters_per_bundle: int = 8
    skip_label_patterns: List[str] = Field(
        default_factory=lambda: list(DEFAULT_SKIP_LABEL_PATTERNS))
    # GitLab link types whose source blocks the target.
    blocker_link_types: List[str] = Field(default_factory=lambda: ["is_blocked_by", "blocked_by"])

    @field_validator("max_bundle_size")
    @classmethod
    def _check_cap(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_bundle_size must be >= 1")
        return v

    @field_validator("skip_label_patterns")
    @classmethod
    def _check_patterns(cls, v: List[str]) -> List[str]:
        compiled = []
        for pat in v:
            try:
                re.compile(pat)
            except re.error as e:
                raise ValueError(f"invalid skip label pattern {pat!r}: {e}")
        return v


@dataclass
class LaneView:
    """Non-terminal bundle tasks per lane key, partitioned by lifecycle stage
    (built from the cluster store's task list by routers/intake._grouping_view).
    The cardinality guard consumes this; it never guesses from memory alone so
    a main restart cannot double-queue a lane."""
    # lane_key -> [(task_id, band)] in pending/ready (queued, not dispatched)
    queued_bundles: Dict[str, List[Tuple[str, int]]] = field(default_factory=dict)
    # lane_key -> [(task_id, band)] in running/assigned (ACTIVE sitting)
    active_bundles: Dict[str, List[Tuple[str, int]]] = field(default_factory=dict)


@dataclass
class BundlePlan:
    """One grouped lane task to create."""
    lane_key: str
    iids: List[int]  # issue IIDs (ints), bundle order = brief order
    project_paths: List[str]
    priority: int
    issue_ids: List[str]          # "<path>#<iid>" dedup keys, in brief order
    reason: str                   # audit trail: how this batch was packed


def lane_repo(project_path: str) -> str:
    """The lane-key repo segment from a GitLab path_with_namespace: the LAST
    segment. ``invora/invora-flutter`` -> ``invora-flutter``; the lane is
    client×repo scoped (lane-operating-shape: 'scoped to one logical
    stack/client'), and the full path rides in the task title/brief."""
    return (project_path or "").rstrip("/").split("/")[-1] or "unknown-repo"


def lane_branch_for(project_path: str, config: GroupingConfig) -> str:
    repo = lane_repo(project_path)
    if repo in config.lane_branches:
        return config.lane_branches[repo]
    if project_path in config.lane_branches:
        return config.lane_branches[project_path]
    return config.lane_branch  # env/dev unless the repo is mapped elsewhere


def lane_key_for(project_path: str, config: GroupingConfig) -> str:
    """LFP-1 author lane key ``<repo>#<branch>`` (bdaya-enforcement
    AUTHOR_LANE_KEY_RE). One per client×repo — the cardinality unit."""
    return f"{lane_repo(project_path)}#{lane_branch_for(project_path, config)}"


def is_doomed_label(label: str, config: GroupingConfig) -> bool:
    return any(re.search(p, label or "") for p in config.skip_label_patterns)


def issue_is_doomed(issue: Dict[str, Any], config: GroupingConfig) -> bool:
    return any(is_doomed_label(l, config) for l in (issue.get("labels") or []))


def _area_cluster(issue: Dict[str, Any]) -> str:
    for l in issue.get("labels") or []:
        if l.startswith("area::"):
            return l
    return ""


def filter_candidates(
    issues: Sequence[Tuple[str, Dict[str, Any]]],
    *,
    config: GroupingConfig,
    band_for: Callable[[Dict[str, Any]], int],
) -> List[Tuple[str, Dict[str, Any], int]]:
    """Apply the pure skip rules; return (issue_id, issue, band) triples.

    The GitLab-state rules — D-SPAWN-1 (open-MR refs), the DAG hold
    (open out-of-batch is_blocked_by blockers) and DEDUP FIRST (issues
    already held by a non-terminal/completed task) — need live I/O and run
    in routers/intake.py's grouped cycle, not here; this function decides
    only from the payloads themselves.
    """
    out: List[Tuple[str, Dict[str, Any], int]] = []
    for iid, iss in issues:
        if issue_is_doomed(iss, config):
            continue
        out.append((iid, iss, band_for(iss)))
    return out


def pack_repo_candidates(
    lane_key: str,
    candidates: Sequence[Tuple[str, Dict[str, Any], int]],
    config: GroupingConfig,
) -> Tuple[List[Tuple[str, Dict[str, Any], int]], str]:
    """Pick the FIRST batch for a lane from its candidates: band order first
    (0 before 3 — the business-team fast path), then area-cluster co-location
    (#762: group by shared surface), cut at the size/reviewability caps.
    Returns (batch, reason); the remainder is waitlisted by the caller."""
    if not candidates:
        return [], "empty"
    top_band = min(b for _, _, b in candidates)
    band_members = [c for c in candidates if c[2] == top_band]

    clusters: Dict[str, List] = {}
    for c in band_members:
        clusters.setdefault(_area_cluster(c[1]), []).append(c)
    # Big clusters first, named clusters before the unlabeled singleton;
    # stable within each group (created order preserved by the caller).
    order = sorted(clusters.items(), key=lambda kv: (-len(kv[1]), kv[0] == "", kv[0]))

    batch: List = []
    used_clusters: Set[str] = set()
    for name, group in order:
        if len(used_clusters) >= config.max_area_clusters_per_bundle and name not in used_clusters:
            break
        for c in group:
            if len(batch) >= config.max_bundle_size:
                break
            batch.append(c)
            used_clusters.add(name)
        if len(batch) >= config.max_bundle_size:
            break
    reason = (f"band={top_band} clusters={sorted(used_clusters)} "
              f"batch={len(batch)}/{config.max_bundle_size} cap")
    return batch, reason


def bundle_plan_for_repo(
    lane_key: str,
    candidates: Sequence[Tuple[str, Dict[str, Any], int]],
    config: GroupingConfig,
    *,
    view: LaneView,
    project_of: Callable[[str], str],
) -> Optional[BundlePlan]:
    """Cardinality guard, intake half (#762 — 'exactly one active lane per
    client-repo, enforced at dispatch by dedup class 2'):

    * normal (band>0) backlog gets AT MOST ONE queued bundle task per lane at
      any time: while a non-terminal bundle task exists for the key, new
      candidates wait for the next cycle (the lane reaps them into the NEXT
      sitting once the current one is terminal).
    * BAND 0 EXCEPTION (owner rule, #886 author bands): a business-team
      (band 0) batch may create its own bundle task even when a queued band>0
      bundle exists — it must never be stranded behind the backlog. It still
      waits, via the scheduler lane guard, only for the sitting that is
      ACTIVE on the lane (same as per-issue model: no second session on one
      repo — #833 rule #1, lane_key_guard DENY).
    * Returns None when there is nothing to create.
    """
    if not candidates:
        return None
    top_band = min(b for _, _, b in candidates)
    queued = view.queued_bundles.get(lane_key, [])   # (task_id, band) pending/ready
    active = view.active_bundles.get(lane_key, [])   # (task_id, band) running/assigned
    nonterminal = len(queued) + len(active)
    if nonterminal:
        if top_band > 0:
            return None                     # one queued sitting at a time
        if any(b == 0 for _, b in queued):
            return None                     # band-0 bundle already queued
    batch, reason = pack_repo_candidates(lane_key, candidates, config)
    if not batch:
        return None
    return BundlePlan(
        lane_key=lane_key,
        iids=[int(iss["iid"]) for _, iss, _ in batch],
        project_paths=sorted({project_of(k) for k, _, _ in batch}),
        priority=top_band,
        issue_ids=[i for i, _, _ in batch],
        reason=reason,
    )


def bundle_title(bundle: BundlePlan) -> str:
    if len(bundle.iids) == 1:
        return f"[lane:{bundle.lane_key}][##{bundle.iids[0]}]"
    joined = "-".join(str(i) for i in bundle.iids)
    return f"[lane:{bundle.lane_key}][##{bundle.iids[0]}…{bundle.iids[-1]} x{len(bundle.iids)}]"


def reviewer_handoff_brief(bundle: BundlePlan, mr_url: str, head_sha: str,
                           reviewer_lane_key: Optional[str] = None) -> str:
    """#893 item 5: the ONE place the reviewer-task wording lives. The author
    lane pastes the rendered text as the TITLE of the reviewer task it
    submits itself via ``kanban_cluster_submit`` — so the brief the reviewer
    lane boots on is fully independent: it names the artifact under review
    (MR URL + pinned head sha), the roster it must check, the rubric, the
    posting instruction, and CLOSE-THE-LOOP (reviewer lands on PASS —
    reviewer != author and fresh-context, so RV-1 holds — and hands back to
    the ORIGINAL author lane on NEEDS-CHANGES). The author NEVER appears on
    the approve/merge side of this text."""
    issues = ", ".join(f"#{i}" for i in bundle.iids)
    rev_key = reviewer_lane_key or f"{bundle.lane_key}-rev"
    return (
        f"[lane:{rev_key}][REVIEW {bundle.lane_key} MR !{mr_url.rstrip('/').split('/')[-1]}]\n"
        f"\n"
        f"## INDEPENDENT REVIEW (shared/claude-plugins#893) — fresh context, reviewer role\n"
        f"\n"
        f"Artifact under review: {mr_url}\n"
        f"Head sha (verdict pins to this sha ONLY; anything else = stale): {head_sha}\n"
        f"Author lane: `{bundle.lane_key}`  |  Your lane key: `{rev_key}`\n"
        f"\n"
        f"Issues this bundle delivers (every one must be Refs'd by the MR and\n"
        f"carry its own per-issue disposition note — PROOF IS NOT DILUTED BY\n"
        f"BUNDLING): {issues}\n"
        f"(projects: {', '.join(bundle.project_paths)})\n"
        f"\n"
        f"Rubric — block ONLY on a CONFIRMED in-diff finding:\n"
        f"  * correctness bug you can point at in the diff,\n"
        f"  * acceptance-criteria violation for a listed issue (missing per-issue\n"
        f"    proof/disposition counts as the finding — name the issue number),\n"
        f"  * a secret in the diff,\n"
        f"  * a CLAUDE.md-rule violation. Style opinions are not findings.\n"
        f"  * If SocratiCode (or any roster tool the review needs) is short or\n"
        f"    unavailable, verdict is INCOMPLETE-ROSTER — never a PASS you\n"
        f"    guessed around the gap.\n"
        f"  * A `needs-human` label anywhere on this bundle is a hard stop:\n"
        f"    verdict NEEDS-HUMAN, and it is NEVER overridden — not by you,\n"
        f"    not by the author.\n"
        f"\n"
        f"Post your verdict: an MR note pinned to the head sha, via\n"
        f"`bdaya-glab mr note` (npx -y -p @shared/bdaya-gitlab@latest ...).\n"
        f"The note is the durable oracle the landing gate reads.\n"
        f"\n"
        f"CLOSE THE LOOP (#893) — after posting the note:\n"
        f"  * On PASS at head: YOU land the MR yourself:\n"
        f"    `bdaya-glab mr land --project <p> --mr <n> --sha {head_sha}`\n"
        f"    (sha pinned; the tool refuses a stale head). Reviewer != author\n"
        f"    and this lane is fresh-context, so RV-1 holds — a `needs-human`\n"
        f"    label is a hard stop: NEVER land past it.\n"
        f"  * On NEEDS-CHANGES: do NOT land. Submit a follow-up AUTHOR task on\n"
        f"    the ORIGINAL lane_key `{bundle.lane_key}` (role='author') via\n"
        f"    `kanban_cluster_submit`, carrying your findings verbatim — the\n"
        f"    fix round is a delivery into the author lane session (#833 rule #1).\n"
        f"  * SUBMIT EXACTLY ONE follow-up task per verdict, then FINISH your\n"
        f"    lane (#898): never wait for, cancel, or resubmit a task you\n"
        f"    dispatched — main DEDUPES a second open task on a lane_key\n"
        f"    (`deduped: true` in the answer = the FIRST submit holds, not a\n"
        f"    failure to retry). Your verdict pins to the head sha above; a\n"
        f"    moved head is YOURS to re-check and re-verdict, on this same\n"
        f"    lane — never a reason to dispatch a second reviewer.\n"
    )


def bundle_brief(bundle: BundlePlan) -> str:
    """The grouped delivery brief the lane receives. Carries the #762 sitting
    discipline and the non-dilution proof rules explicitly, because a bundle
    whose brief does not restate them silently loses per-issue guarantees."""
    issues = ", ".join(f"#{i}" for i in bundle.iids)
    return (
        f"## LFP-1 stateful lane sitting — {len(bundle.iids)} issue(s) in one bundle\n"
        f"\n"
        f"Lane key: `{bundle.lane_key}` (one long-lived lane per client×repo —\n"
        f"shared/claude-plugins#762, owner D7-stateful; #833 note 133037 rule #1).\n"
        f"\n"
        f"Issues in this bundle: {issues}\n"
        f"(projects: {', '.join(bundle.project_paths)})\n"
        f"\n"
        f"Sitting discipline (#762, #613 note 105482):\n"
        f"  * ONE clone, ONE app session: launch/attach the app once, authenticate\n"
        f"    once, iterate over ALL issues above against the still-running app.\n"
        f"  * LOCAL-FIRST (LFP-1, !685): batch every fix into the one clone, run the\n"
        f"    exact CI suite locally to green, push ONCE — CI confirms, it does not\n"
        f"    iterate. ONE Draft MR for the whole bundle.\n"
        f"  * AB-1: never cross stacks; this bundle is one repo, cross-area bundling\n"
        f"    inside it is the LFP-1 carve-out and the diff MUST stay reviewable —\n"
        f"    if it exceeds one review pass, split the MR and say so on every\n"
        f"    dropped issue's disposition note.\n"
        f"  * PROOF IS NOT DILUTED BY BUNDLING: each issue needs its own scoped\n"
        f"    proof (per-issue VP-1 instance, TDDD-1 fence for bug-labeled ones)\n"
        f"    and its own disposition note. The MR `Refs` every issue — NEVER\n"
        f"    `Closes` (the VP-1 close gate fires them on merge before live\n"
        f"    verification). Each issue closes individually AFTER its proof note.\n"
        f"  * DAG: do not fix an issue above whose open blocker (is_blocked_by,\n"
        f"    outside this bundle) is unmerged — record the hold as its\n"
        f"    disposition instead.\n"
        f"  * Drain the fix-merged-awaiting-live-verify queue first (S16) so the\n"
        f"    sitting never re-derives a landed fix.\n"
        f"  * Escalate once, in a batch: one AskHumanQuestion with several\n"
        f"    questions beats N separate blocks.\n"
        f"\n"
        f"HAND-OFF (#893 — reviewer self-dispatch; no lead in the loop):\n"
        f"  * Once your ONE Draft MR is pushed with EVERY per-issue disposition\n"
        f"    note posted, do NOT park for the lead: call the\n"
        f"    `kanban_cluster_submit` tool to create your OWN reviewer task —\n"
        f"    `role='reviewer'`, `requires=['review']`,\n"
        f"    `lane_key='{bundle.lane_key}-rev'`, priority = this task's\n"
        f"    priority ({bundle.priority}), and the title = the full\n"
        f"    INDEPENDENT REVIEW brief rendered by\n"
        f"    `reviewer_handoff_brief(bundle, mr_url, head_sha)` — the single\n"
        f"    source of that wording: it carries the MR URL, the head sha, the\n"
        f"    issue list, the rubric (block only on CONFIRMED in-diff\n"
        f"    correctness/acceptance/secret/CLAUDE.md-rule findings;\n"
        f"    `INCOMPLETE-ROSTER` if SocratiCode is short; `NEEDS-HUMAN` never\n"
        f"    overridden), the posting instruction (`bdaya-glab mr note` via\n"
        f"    `npx -y -p @shared/bdaya-gitlab@latest`), and the CLOSE-THE-LOOP\n"
        f"    rule: on PASS at head the REVIEWER lane lands the MR itself with\n"
        f"    `bdaya-glab mr land --project <p> --mr <n> --sha <head>`\n"
        f"    (reviewer != author, fresh context, so RV-1 holds; a\n"
        f"    `needs-human` label is a hard stop — never land past it); on\n"
        f"    NEEDS-CHANGES the reviewer submits a follow-up author task on\n"
        f"    the ORIGINAL lane key `{bundle.lane_key}` with the findings.\n"
        f"  * SUBMIT ONCE, THEN FINISH (#898): you submit exactly ONE\n"
        f"    reviewer task per sitting, after the READY gate (every\n"
        f"    disposition note posted, Draft MR head pushed) — and then you\n"
        f"    FINISH the lane. NEVER wait for the reviewer to run, NEVER\n"
        f"    cancel it, NEVER resubmit it. A queued reviewer is the normal\n"
        f"    state (LFP-1 serializes lanes; main DEDUPES a second submit on\n"
        f"    `{bundle.lane_key}-rev` anyway — a `deduped: true` answer means\n"
        f"    the FIRST submit holds, not that you must try again). A moved\n"
        f"    head is the REVIEWER's to re-pin and re-verdict — never the\n"
        f"    author's reason to dispatch a second reviewer.\n"
        f"  * The author lane NEVER approves, merges, or reviews its own MR,\n"
        f"    and it does not park for the lead — the hand-off above IS the\n"
        f"    end of the author's duties for this sitting.\n"
    )
