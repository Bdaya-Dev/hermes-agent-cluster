"""#893 items 3 + 5: the intake BUNDLE BRIEF teaches the author lane to hand
off to a self-dispatched reviewer, and the reviewer brief closes the loop.

Items 1+2 (executor shared-template hand-off blocks, plugin endpoint
resolution) are already landed on main; what this file pins is that a
GROUPED bundle — the #762 lane shape — carries the same hand-off discipline
in its own brief, and that the reviewer-task wording lives in exactly ONE
helper (`reviewer_handoff_brief`) so brief and helper can never drift.

Invariants:
  * bundle_brief names kanban_cluster_submit + role reviewer + requires
    ['review'] + the '<lane_key>-rev' reviewer lane + this task's priority;
  * the CLOSE-THE-LOOP rule is spelled out: reviewer lands on PASS with
    `bdaya-glab mr land ... --sha` (sha-pinned), submits a follow-up author
    task on the ORIGINAL lane key on NEEDS-CHANGES;
  * `needs-human` is a hard stop, never overridden;
  * the never-self-approve sentence is explicit for the author lane and it
    does NOT park for the lead;
  * reviewer_handoff_brief renders the MR URL and head sha, and the rubric
    tokens (INCOMPLETE-ROSTER / bdaya-glab mr note / npx).
"""
from __future__ import annotations

from hermes_cluster.core.intake_grouping import (
    BundlePlan,
    bundle_brief,
    reviewer_handoff_brief,
)


def _plan() -> BundlePlan:
    return BundlePlan(
        lane_key="invora-flutter#env/dev",
        iids=[101, 202, 303],
        project_paths=["invora/invora-flutter"],
        priority=3,
        issue_ids=["invora/invora-flutter#101", "invora/invora-flutter#202",
                   "invora/invora-flutter#303"],
        reason="band=3 clusters=['area::ui'] batch=3/40 cap",
    )


MR_URL = "https://gitlab.bdaya-dev.com/invora/invora-flutter/-/merge_requests/42"
SHA = "deadbeefcafe0123456789abcdef0123456789ab"


def test_bundle_brief_carries_the_handoff_section():
    text = bundle_brief(_plan())
    assert "HAND-OFF (#893" in text
    # self-dispatch mechanics
    assert "kanban_cluster_submit" in text
    assert "role='reviewer'" in text
    assert "requires=['review']" in text
    assert "invora-flutter#env/dev-rev" in text          # <lane_key>-rev
    assert "priority = this task's" in text and "3" in text
    # the helper is the single source of the reviewer-brief wording
    assert "reviewer_handoff_brief(bundle, mr_url, head_sha)" in text
    # close-the-loop
    assert "bdaya-glab mr land" in text and "--sha" in text
    assert "NEEDS-CHANGES" in text
    assert "ORIGINAL lane key `invora-flutter#env/dev`" in text


def test_bundle_brief_rubric_and_hard_stops():
    text = bundle_brief(_plan())
    assert "INCOMPLETE-ROSTER" in text                    # SocratiCode short
    assert "NEEDS-HUMAN" in text
    assert "needs-human" in text and "hard stop" in text
    assert "bdaya-glab mr note" in text
    assert "@shared/bdaya-gitlab@latest" in text


def test_bundle_brief_never_self_approve_and_no_parking():
    text = bundle_brief(_plan())
    assert "NEVER approves, merges, or reviews its own MR" in text
    assert "does not park for the lead" in text


def test_bundle_brief_ready_gate_before_handoff_ordering():
    """Measured bounce (invora/invora-flutter!479, 2026-09-14): a Draft MR or
    a red required job at head makes the reviewer pipeline refuse at Stage 1
    and bounce a NEEDS-CHANGES follow-up onto the SAME author lane — wasted
    credits. The brief must therefore state the hand-off preconditions in
    ORDER: push ONCE -> CI green at head (terminal traces, fix locally if
    red) -> mark the MR READY -> THEN kanban_cluster_submit the reviewer."""
    text = bundle_brief(_plan())
    # the ordered preconditions, by their tokens
    assert "push ONCE" in text
    assert "mark the MR READY" in text
    assert "bdaya-glab mr update --ready" in text
    assert "markPullRequestReadyForReview" in text
    assert "green" in text
    # the bounce warning is explicit
    assert "Draft is not" in text or "a Draft is not a merge candidate" in text
    assert "wasted credits" in text
    # ORDERING: CI-green and READY both precede the reviewer submit, and the
    # numbered list runs push -> green -> READY -> submit.
    i_push = text.index("1. push ONCE")
    i_green = text.index("2. wait for the required CI jobs at head to be green")
    i_ready = text.index("3. mark the MR READY")
    i_submit = text.index("kanban_cluster_submit", i_ready)
    assert i_push < i_green < i_ready < i_submit
    # and the generic "green before kanban_cluster_submit" pin: the first
    # hand-off-section 'green' sits strictly before the hand-off submit.
    i_green_any = text.index("green", text.index("HAND-OFF"))
    assert i_green_any < i_submit


def test_reviewer_handoff_brief_renders_mr_url_and_sha():
    text = reviewer_handoff_brief(_plan(), MR_URL, SHA)
    assert MR_URL in text
    assert SHA in text
    # reviewer lane key default = <author lane_key>-rev, shown to the reviewer
    assert "invora-flutter#env/dev-rev" in text
    # the reviewer sees the full rubric + close-the-loop too
    assert "INDEPENDENT REVIEW" in text
    assert "INCOMPLETE-ROSTER" in text
    assert "needs-human" in text
    assert "bdaya-glab mr land" in text and SHA in text
    assert "bdaya-glab mr note" in text
    assert "npx -y -p @shared/bdaya-gitlab@latest" in text
    # fix rounds go back to the ORIGINAL author lane key
    assert "invora-flutter#env/dev" in text
    assert "kanban_cluster_submit" in text


def test_reviewer_handoff_brief_explicit_lane_key_override():
    text = reviewer_handoff_brief(_plan(), MR_URL, SHA,
                                  reviewer_lane_key="custom-rev")
    assert "custom-rev" in text


def test_bundle_brief_uses_helper_wording_not_a_fork():
    """The bundle brief POINTS at the helper (single source); the helper
    renders the reviewer-facing text. Drift guard: the close-the-loop tokens
    must appear in BOTH, spelled identically where they name tools."""
    plan = _plan()
    brief = bundle_brief(plan)
    helper = reviewer_handoff_brief(plan, MR_URL, SHA)
    for token in ("INCOMPLETE-ROSTER", "bdaya-glab mr land", "bdaya-glab mr note"):
        assert token in brief and token in helper
