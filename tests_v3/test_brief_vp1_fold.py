"""Owner ruling 2026-09-14 — VP-1 FOLDS INTO THE MR (separate verifier lane retired).

Pins the two brief-renderers in ``hermes_cluster.core.intake_grouping`` to the
ruled shape:

  * ``bundle_brief``: the per-issue proof IS the e2e-layer guard (patrol/playwright
    frontend, black-box e2e/integration/unit backend), RED with the defect; the MR
    still `Refs` — never `Closes`; no separate live-verification pass is promised
    anywhere in the brief.
  * ``reviewer_handoff_brief``: the reviewer — already independent per #893 —
    re-runs the guard RED→GREEN, reads the post-deploy e2e job on env/dev by its
    terminal trace, posts the FOLDED VP-1 note
    (``VP-1: PASS — Surface: merged@<sha> + ci-job@<project>/<job-id> — <mr-url>
    — verifier: @<you>``) and CLOSES the fixed issues itself; a `needs-human`
    label stays a hard stop.

RED PROOF (skill standard): on the pre-fold main, these fail on the DEFECT — the
folded Surface: string, the guard-re-run instruction, and the self-close line are
absent from the rendered briefs — not on ImportError.
"""
from __future__ import annotations

import re

from hermes_cluster.core.intake_grouping import (
    BundlePlan,
    bundle_brief,
    reviewer_handoff_brief,
)


def _bundle(iids=(42, 43)) -> BundlePlan:
    return BundlePlan(
        lane_key="invora-backend#env/dev",
        iids=list(iids),
        project_paths=["invora/invora-backend"],
        priority=3,
        issue_ids=[f"invora/invora-backend#{i}" for i in iids],
        reason="test",
    )


MR_URL = "https://gitlab.test/invora/invora-backend/-/merge_requests/7"
HEAD = "aa3b753c1234567890abcdef1234567890abcdef12"

# Exactly the grammar shared/claude-plugins close-gate.js SURFACE_FOLDED_RE accepts:
# merged@<sha> + ci-job@<group>/<project>/<numeric-id>, project-qualified and numeric.
FOLDED_SURFACE_RE = re.compile(
    r"VP-1: PASS — Surface: merged@<merge-sha> \+ ci-job@<project>/<job-id> — \S+ — verifier: @<your-username>"
)


# ---------------------------------------------------------------- bundle_brief

def test_bundle_brief_names_the_guard_as_the_folded_proof():
    text = bundle_brief(_bundle())
    assert "2026-09-14 owner ruling" in text
    assert "patrol/playwright" in text.lower() or "patrol/playwright" in text
    assert "black-box e2e" in text
    assert "RED with" in text and "GREEN with the fix" in text
    # still Refs-only (the close-keyword ban is unchanged doctrine)
    assert "NEVER `Closes`" in text


def test_bundle_brief_drops_the_separate_live_pass_promise():
    text = bundle_brief(_bundle())
    # the retired wording was "Each issue closes individually AFTER its proof note"
    # (an out-of-band live pass); under the fold the reviewer's close leg says so instead.
    assert "closes individually AFTER its proof note" not in text
    assert "reviewer re-runs that guard RED" in text


# ------------------------------------------------------- reviewer_handoff_brief

def test_reviewer_brief_carries_the_folded_vp1_note_template():
    text = reviewer_handoff_brief(_bundle(), MR_URL, HEAD)
    assert FOLDED_SURFACE_RE.search(text), (
        "the reviewer brief must carry the folded VP-1 shape verbatim-templated"
    )
    assert MR_URL in text  # the note template pins the MR URL as the evidence locator


def test_reviewer_brief_requires_guard_rerun_and_post_deploy_trace():
    text = reviewer_handoff_brief(_bundle(), MR_URL, HEAD)
    assert "RE-RUN the issue's" in text and "TDDD-1 guard" in text
    assert "RED with the defect present" in text
    assert "post-deploy e2e job" in text
    assert "TERMINAL TRACE" in text  # CG-1: trace, not pipeline color
    assert "CLOSE it yourself" in text


def test_reviewer_brief_keeps_not_green_stays_open_and_needs_human_hard_stop():
    text = reviewer_handoff_brief(_bundle(), MR_URL, HEAD)
    assert "fix-merged-awaiting-live-verify" in text
    assert "needs-human" in text.lower()
    # needs-human remains a hard stop on landing (unchanged #893 invariant)
    assert "NEVER land past it" in text
    # and the ruling says relabelling to dodge is banned, not offered
    assert "never relabel to dodge" in text


def test_reviewer_brief_still_names_artifact_sha_and_author_lane():
    # invariants from #893 must survive the fold edit untouched
    text = reviewer_handoff_brief(_bundle(), MR_URL, HEAD)
    assert HEAD in text
    assert MR_URL in text
    assert "invora-backend#env/dev" in text  # follow-up author lane key
    assert "#42, #43" in text
