"""#902 TDDD-1 fence: the reviewer hand-off brief must CLOSE THE LOOP on a
GitHub PR, not strand it.

Measured failure (2026-09-14, release-train-1): a read-only (#882) reviewer
lane on Bdaya-Dev/bdaya-website-infra PR #308 posted APPROVE at head and
stopped — the brief's PASS branch said `bdaya-glab mr land`, which has no
GitHub verb; with no valid action it even closed its own summary circularly
("the reviewer hand-off submits the reviewer task" — the step it had just
completed). The PR sat Draft + unmerged until the lead hand-dispatched a
landing task.

Required shape (issue #902):
  * GitHub PR + PASS branch: submit a LANDING TASK via kanban_cluster_submit
    (role='author', lane_key '<repo>#land-<n>', requires=['github-write'],
    priority 5) whose brief is headRefOid-verify -> gh pr ready ->
    gh pr merge --merge -> post-merge verify.
  * NEEDS-CHANGES branch unchanged (follow-up author task on the original lane).
  * The brief must NEVER tell a reviewer to "submit the reviewer task".

RED PROOF (skill standard): on the pre-#902 template the GitHub branch is
absent — the brief for a github.com/pull/ URL renders the GitLab `mr land`
verb and no landing-task instruction; these tests fail on that DEFECT (not on
ImportError). The GitLab-path tests pin the PASS-at-head self-land leg to stay
byte-present so the fix cannot regress the other host.
"""
from __future__ import annotations

import re

import pytest

from hermes_cluster.core.intake_grouping import (
    BundlePlan,
    _parse_github_pr_url,
    bundle_brief,
    reviewer_handoff_brief,
)

GH_URL = "https://github.com/Bdaya-Dev/bdaya-website-infra/pull/308"
GL_URL = "https://gitlab.bdaya-dev.com/shared/claude-plugins/-/merge_requests/940"
SHA = "c28e4b90" + "0" * 32


def _bundle() -> BundlePlan:
    return BundlePlan(
        lane_key="infra-github#main",
        iids=[902],
        project_paths=["shared/claude-plugins"],
        priority=0,
        issue_ids=["shared/claude-plugins#902"],
        reason="test",
    )


# ------------------------------------------------------------ url detection


@pytest.mark.parametrize(
    "url, expected",
    [
        (GH_URL, ("Bdaya-Dev/bdaya-website-infra", 308)),
        ("https://github.com/openclaw/openclaw/pull/77/files", ("openclaw/openclaw", 77)),
        (GL_URL, None),
        ("https://gitlab.bdaya-dev.com/shared/claude-plugins/-/merge_requests/940#note_1", None),
        ("", None),
    ],
)
def test_parse_github_pr_url(url, expected):
    assert _parse_github_pr_url(url) == expected


# ------------------------------------------------- GitHub PASS branch shape


def test_github_brief_pass_branch_submits_landing_task():
    text = reviewer_handoff_brief(_bundle(), GH_URL, SHA)
    # THE defect (RED pre-fix): no landing-task instruction at all.
    assert "submit a LANDING task" in text
    assert "kanban_cluster_submit" in text
    assert "github-write" in text
    assert "land-308" in text           # lane_key '<repo>#land-<n>'
    assert "priority=5" in text
    assert "role='author'" in text
    # The landing brief itself: verify-sha -> ready -> merge --merge.
    assert "headRefOid" in text
    assert f"gh pr view 308 --repo Bdaya-Dev/bdaya-website-infra" in text
    assert SHA in text                   # pinned reviewed sha
    assert "gh pr ready 308" in text
    assert "gh pr merge 308" in text
    assert "--merge" in text
    # STOP-when-moved is explicit, not implied.
    assert "STOP" in text


def test_github_brief_never_tells_reviewer_to_submit_reviewer_task():
    text = reviewer_handoff_brief(_bundle(), GH_URL, SHA)
    # The circular close measured on #308: "then the reviewer hand-off
    # submits the reviewer task". The template must name the anti-pattern and
    # negate it, and must NOT instruct it.
    assert re.search(r"submit the reviewer task", text)
    assert "that step is the task you are now" in text  # named AND negated


def test_github_brief_keeps_needs_changes_branch():
    text = reviewer_handoff_brief(_bundle(), GH_URL, SHA)
    assert "On NEEDS-CHANGES: do NOT land." in text
    assert "follow-up AUTHOR task" in text
    assert "infra-github#main" in text


def test_github_brief_title_says_pr_not_mr():
    text = reviewer_handoff_brief(_bundle(), GH_URL, SHA)
    assert text.startswith("[lane:infra-github#main-rev][REVIEW infra-github#main PR #308]")


# ----------------------------------------------- GitLab branch unchanged


def test_gitlab_brief_still_self_lands_byte_present():
    text = reviewer_handoff_brief(_bundle(), GL_URL, SHA)
    assert "YOU land the MR yourself" in text
    assert "bdaya-glab mr land" in text
    assert "On PASS at head (GitHub PR" not in text
    assert text.startswith("[lane:infra-github#main-rev][REVIEW infra-github#main MR !940]")


def test_bundle_brief_summary_names_both_landing_paths():
    text = bundle_brief(_bundle())
    assert "bdaya-glab mr land" in text
    assert "GITHUB PR" in text and "landing task" in text
