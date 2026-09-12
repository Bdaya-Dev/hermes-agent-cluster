"""#872 instance 2: a brief's ACTION must name the target its lane names.

Instance 1 (merged as PR#30) was a brief that never mentioned its lane's
target. Instance 2 escaped that guard: lane `infra-github!275-rev-c` received
a brief that mentioned `pr#275` -- so the presence check passed -- while
instructing `gh pr comment 273`. The lane silently overrode its own brief and
posted to the right PR. A lane correcting its brief is luck, not a control.

Both independent reviews of the two competing drafts
(shared/claude-plugins#872, notes 135712 / 135713) recommended the same shape:
#32's broad verb coverage (CLI *and* prose actions) with #31's strict
per-number rule (EVERY action-bound number must equal the lane target; no
set-union loophole). These tests pin that combination -- each REJECT row was
the shape one of the two drafts let through, and each PASS row was a measured
false-rejection from the live brief corpus.

Import discipline (red-proof standard): the new helpers are imported INSIDE
the tests that pin them, never at module top, so a run against a tree without
the fix fails on the guard's assertions -- the defect -- not on ImportError.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.routers.tasks import _lane_target, _brief_names_target


@pytest.fixture
def client():
    return TestClient(
        create_app(cluster_id="test-cluster", node_id="test-node", node_role="main")
    )


def _submit(client, lane_key, title):
    return client.post(
        "/api/v1/tasks",
        json={"title": title, "lane_key": lane_key, "requires": []},
    )


# --- the measured incident ---------------------------------------------------

# Verbatim shape of task_f4c0e3d70e522f93's brief: it names its own target
# (so instance 1's guard passes) while binding the posting action elsewhere.
INCIDENT_BRIEF = (
    "Review PR#275 as an independent verifier. Post your verdict with "
    "`gh pr comment 273` on the issue. NEVER approve or merge."
)


def test_instance_2_measured_incident_is_rejected(client):
    """The 275 lane instructed to `gh pr comment 273` must 422 at submit."""
    r = _submit(client, "infra-github!275-rev-c", INCIDENT_BRIEF)
    assert r.status_code == 422, (
        "instance 2 escaped the guard: the brief mentions 275 but binds the "
        "posting action to 273"
    )
    assert "273" in r.json()["detail"]


# --- REJECT rows: shapes a competing draft let through ------------------------

@pytest.mark.parametrize(
    "brief, why",
    [
        # PR#31 (CLI-only verbs) passed these; prose actions bind the lane too.
        ("Review PR#275, then comment on 273 with your findings.",
         "PR#31's CLI-only verb list missed a prose comment action"),
        ("Review MR 275; merge the MR 273 when green.",
         "PR#31 missed a prose merge action on a foreign number"),
        # PR#32 (set-union rule) passed these; the lane's number appearing
        # somewhere in the action set does not excuse a foreign one.
        ("Review PR#275 and then gh pr comment 273 on the sibling.",
         "PR#32's set-union loophole: 275 bound elsewhere let 273 through"),
        ("Post the verdict to 273 for PR#275.",
         "PR#32 passed it: its target 275 was also an action number"),
    ],
)
def test_foreign_action_bound_number_is_rejected(client, brief, why):
    r = _submit(client, "infra-github!275-rev-c", brief)
    assert r.status_code == 422, f"{why}: {brief!r} passed the guard"


# --- PASS controls: measured false-rejections from the live corpus ------------

@pytest.mark.parametrize(
    "brief",
    [
        # Standing lane rules mention the verbs with no bound number.
        "Review PR#275. NEVER approve or merge. Write your verdict first.",
        # A bare cross-reference is context, not an instruction.
        "Review PR#275. Follows the same defect as Refs #871 and #872.",
        # The verbs with the CORRECT number stay legal.
        "Review PR#275 and post your verdict with `gh pr comment 275`.",
        "Review pr#275 thoroughly; comment on 275 with the evidence.",
        # Corpus false-rejection rows the broad verb list must not repeat:
        "Summarize what merged 2026-09-11 in PR#275.",       # date, not ref
        "PR#275 backport of the change that Closed issue #831.",  # past-tense prose
        "Rebuild PR#275 image from the commit for merge 995d5600.",  # SHA fragment
        # Count adjacency: a number that is not a ref.
        "Review the 120 changed files in PR 275 and comment on 275.",
        # A branch-shaped lane key carries no target to disagree with.
        "Wire MCP into the GKE brain. ONE Draft PR. NEVER approve or merge.",
    ],
)
def test_legal_briefs_still_accepted(client, brief):
    lane = "infra-github!275-rev-c"
    if not _brief_names_target(brief, _lane_target(lane)):
        # the last control is an instance-1-shape brief on a target-less lane
        lane = "claude-plugins#feat/x"
    r = _submit(client, lane, brief)
    assert r.status_code == 200, (
        f"guard false-rejected a legal brief: {r.json().get('detail')!r}"
    )


# --- helper-surface pins (in-function imports keep reds defect-shaped) --------

def _helpers():
    """getattr-based fetch so a tree without the fix fails on an ASSERTION
    (the defect), never on ImportError -- the red-proof standard."""
    import hermes_cluster.routers.tasks as t

    extract = getattr(t, "_brief_action_numbers", None)
    mismatches = getattr(t, "_action_ref_mismatches", None)
    assert extract is not None and mismatches is not None, (
        "instance-2 guard helpers missing: the guard is not on this tree"
    )
    return extract, mismatches


def test_action_numbers_extract_only_bound_refs():
    extract, _ = _helpers()

    # extracts EVERY action-bound number; the strict rule lives in
    # _action_ref_mismatches, not in the extractor.
    assert extract(INCIDENT_BRIEF) == ["275", "273"]
    assert extract(
        "Review PR#275 and then gh pr comment 273 on the sibling."
    ) == ["275", "273"]
    assert extract(
        "NEVER approve or merge. Summarize what merged 2026-09-11."
    ) == []
    assert extract("comment on #273 post-merge") == ["273"]
    assert extract("merge 995d5600 into PR#275") == []
    # the SHA is not bound to a qualifying verb AND `PR#275` is a bare
    # mention; instance 1's presence check covers that side.


def test_mismatch_rule_is_bounded():
    _, mismatches = _helpers()

    # an action on "27" does not satisfy a target of "274"
    assert mismatches("gh pr comment 27 please", "274") == ["27"]
    assert mismatches("gh pr comment 274 please", "274") == []
    # the measured incident: exactly one foreign number flagged
    assert mismatches(INCIDENT_BRIEF, "275") == ["273"]
