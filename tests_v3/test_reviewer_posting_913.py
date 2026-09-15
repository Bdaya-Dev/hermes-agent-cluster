"""shared/claude-plugins#913 — reviewers scheduled where they cannot post.

Measured 2026-09-15T18:45Z: two reviewer tasks carrying only requires=
['review'] were placed on node_windows_pc_worker (advertises review, NOT
github-write, gh token invalid). One produced a full correct PASS and could
not post it -> the verdict lived only in a result file no merge gate reads;
the other failed its roster check outright. The absence of a verdict note is
indistinguishable from a review that never ran — the silent direction is the
bug (#859's note_claim_guard does not fire: this reviewer was HONEST about
not posting).

Three fences, one per the issue's fix, + the controls:

  1. SUBMIT (router): a reviewer task whose brief names a GitHub PR must
     carry github-write in `requires` — 422 otherwise, naming the reason.
     The forge is parsed from the PR URL inside the title (the brief IS the
     title), via the SAME parser #902's landing branch uses — one parser,
     two gates. GitLab-MR reviewers keep plain ['review'] (the bdaya-gitlab
     typed layer is their posting surface).
  2. HAND-OFF TEXT (intake_grouping): the bundle brief instructs
     requires=['review', 'github-write'] for a GitHub PR, and the rendered
     reviewer brief for a PR names the `gh pr comment` posting verb (not
     bdaya-glab mr note) — a lane follows the rendered text; the old text is
     exactly what shipped the lost-verdict placements.
  3. REAP (executor): a reviewer-role hermes reap whose result body CONFESSES
     the verdict could not be posted (the production wording: "could not post
     verdict" / "verdict delivered via this result file only") resolves as
     non_deliverable -> re-queue, never a clean completion. Same #870
     conservatism: a real deliverable that merely DISCUSSES a posting problem
     (or shows posting evidence) must pass.
"""

import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
)


@pytest.fixture
def client(tmp_path):
    app = create_app(cluster_id="t", node_id="main", node_role="main",
                     db_path=str(tmp_path / "g913.db"))
    with TestClient(app) as c:
        yield c


GH_TITLE = ("review PR 70 https://github.com/Bdaya-Dev/hermes-agent-cluster/pull/70 "
            "at head 9b8910e693ff8c1a")
GL_TITLE = ("review MR !949 https://gitlab.bdaya-dev.com/shared/claude-plugins/-/merge_requests/949 "
            "at head e3adec83")


# ---------------------------------------------------------------------------
# Fix 1 — submit refuses a GitHub-PR reviewer that cannot post there
# ---------------------------------------------------------------------------

def test_github_pr_reviewer_without_github_write_is_refused(client):
    r = client.post("/api/v1/tasks", json={
        "title": GH_TITLE, "requires": ["review"],
        "role": "reviewer", "lane_key": "hermes-agent-cluster!70-rev",
    })
    assert r.status_code == 422, (
        "#913: a reviewer task for a GitHub PR must not be schedulable onto a "
        "node without github-write — got " + str(r.status_code) + " " + r.text[:200])
    assert "github-write" in r.text


def test_github_pr_reviewer_with_github_write_is_accepted(client):
    r = client.post("/api/v1/tasks", json={
        "title": GH_TITLE, "requires": ["review", "github-write"],
        "role": "reviewer", "lane_key": "hermes-agent-cluster!70-rev-b",
    })
    assert r.status_code == 200, r.text[:200]


def test_gitlab_mr_reviewer_keeps_plain_review(client):
    """CONTROL: the fence must not re-create #913's twin on GitLab, where the
    typed bdaya-gitlab layer is the posting surface — plain ['review'] stays
    legal for a GitLab MR reviewer."""
    r = client.post("/api/v1/tasks", json={
        "title": GL_TITLE, "requires": ["review"],
        "role": "reviewer", "lane_key": "claude-plugins!949-rev",
    })
    assert r.status_code == 200, r.text[:200]


def test_non_reviewer_task_naming_a_pr_is_untouched(client):
    """CONTROL: an author/landing task that names a GitHub PR is NOT gated by
    this fence — the landing leg already carries github-write per #902, and
    author lanes legitimately mention PRs without posting verdicts."""
    r = client.post("/api/v1/tasks", json={
        "title": "land PR 70 https://github.com/Bdaya-Dev/hermes-agent-cluster/pull/70",
        "requires": [], "role": "author", "lane_key": "hermes-agent-cluster#land-70",
    })
    assert r.status_code == 200, r.text[:200]


def test_reviewer_with_no_forge_url_is_accepted(client):
    """CONTROL: a reviewer brief that names no PR/MR URL cannot be forge-
    classified — stay permissive (the old contract) rather than guess."""
    r = client.post("/api/v1/tasks", json={
        "title": "review the release notes", "requires": ["review"],
        "role": "reviewer", "lane_key": "docs#rev",
    })
    assert r.status_code == 200, r.text[:200]


# ---------------------------------------------------------------------------
# Fix 2 — the rendered hand-off text names the posting capability + verb
# ---------------------------------------------------------------------------

def test_bundle_brief_requires_the_posting_capability_for_github_prs():
    """The single-source bundle brief must instruct requires=['review',
    'github-write'] for GitHub-PR artifacts — an author lane follows the
    rendered text, and the old unconditional ['review'] is what shipped the
    lost-verdict placements."""
    from hermes_cluster.core.intake_grouping import BundlePlan, bundle_brief
    b = BundlePlan(lane_key="infra#main", iids=[319],
                   project_paths=["Bdaya-Dev/bdaya-website-infra"],
                   priority=0, issue_ids=["x#319"], reason="t")
    text = bundle_brief(b)
    flat = text.replace(" ", "")
    assert "['review','github-write']" in flat, (
        "#913: the hand-off text still says requires=['review'] only — a "
        "lane copying it produces a reviewer that cannot post its verdict")


def test_reviewer_handoff_brief_names_the_gh_posting_verb_for_prs():
    """The rendered GitHub-PR reviewer brief must pin the POSTING SURFACE:
    `gh pr comment`, not `bdaya-glab mr note`. The !70 reviewer tried the
    GitLab-shaped instruction against a PR, fell back to an unauthenticated
    gh/MCP path, and stranded a correct PASS."""
    from hermes_cluster.core.intake_grouping import (
        BundlePlan, reviewer_handoff_brief)
    b = BundlePlan(lane_key="cluster#main", iids=[70],
                   project_paths=["Bdaya-Dev/hermes-agent-cluster"],
                   priority=0, issue_ids=["x#70"], reason="t")
    text = reviewer_handoff_brief(
        b, "https://github.com/Bdaya-Dev/hermes-agent-cluster/pull/70", "deadbeef")
    assert "gh pr comment" in text, (
        "#913: the GitHub-PR reviewer brief must name the gh posting verb")


def test_reviewer_handoff_brief_keeps_bdaya_glab_for_mrs():
    """CONTROL: the GitLab-MR brief still instructs `bdaya-glab mr note`."""
    from hermes_cluster.core.intake_grouping import (
        BundlePlan, reviewer_handoff_brief)
    b = BundlePlan(lane_key="cp#main", iids=[949],
                   project_paths=["shared/claude-plugins"],
                   priority=0, issue_ids=["y#949"], reason="t")
    text = reviewer_handoff_brief(
        b, "https://gitlab.bdaya-dev.com/shared/claude-plugins/-/merge_requests/949",
        "deadbeef")
    assert "bdaya-glab mr note" in text


# ---------------------------------------------------------------------------
# Fix 3 — a reviewer that confesses it could not post must not finish clean
# ---------------------------------------------------------------------------

def _executor() -> AgentExecutor:
    return AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60, worker="hermes"),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _reap_role_reviewer(tmp_path, body_text: str):
    executor = _executor()
    results_dir = Path(tmp_path) / "hermes-results"
    results_dir.mkdir(parents=True, exist_ok=True)
    task_id = "task_g913"
    fp = results_dir / f"{task_id}.result.md"
    fp.write_text(body_text, encoding="utf-8")
    mock_proc = MagicMock()
    mock_proc.poll.return_value = 0
    mock_proc.pid = 4242
    mock_proc.stderr = None
    mock_proc.stdout = None
    spawn = ActiveSpawn(
        task_id=task_id, task_title="review PR 70",
        process=mock_proc, lease_id=f"lease_{task_id}",
        started_at=time.time() - 60.0, lane_name=f"hermes-{task_id}",
        mode="hermes", result_path=str(fp),
        lane_key="cluster!70-rev", role="reviewer",
    )
    resolved = []
    executor._reap_hermes_spawn(task_id, spawn, 60.0, resolved)
    return resolved


PASS_STRANDED = (
    "## Verdict: PASS\n"
    "Head sha: 9b8910e693ff8c1a. Eight findings, none blocking.\n"
    "\n"
    "Could not post verdict as PR comment — gh CLI not authenticated on this "
    "node, and GitHub MCP tool lacks write permission (403). Verdict delivered "
    "via this result file only.\n"
)

PASS_POSTED = (
    "## Verdict: PASS\n"
    "Head sha: 9b8910e693ff8c1a.\n"
    "\n"
    "Verdict posted as PR comment https://github.com/Bdaya-Dev/hermes-agent-"
    "cluster/pull/70#issuecomment-123 (gh pr comment, exit 0, read back by id).\n"
)

PASS_DISCUSSING_POSTING = (
    "## Verdict: PASS\n"
    "One note for the record: the review brief warned that a reviewer could "
    "not post verdicts if gh CLI is not authenticated on this node — I "
    "verified gh auth works here and posted the verdict comment (id 123).\n"
)


def test_reap_stranded_reviewer_verdict_is_not_done(tmp_path):
    resolved = _reap_role_reviewer(tmp_path, PASS_STRANDED)
    assert len(resolved) == 1
    outcome, detail = resolved[0][2], resolved[0][3]
    assert outcome != "done", (
        "#913 fix 2: a reviewer whose own result admits the verdict was never "
        "posted must NOT complete clean — got 'done' (the silent-loss shape "
        "indistinguishable from no review)")
    assert "post" in detail.lower() or "verdict" in detail.lower(), detail


def test_reap_posted_reviewer_verdict_stays_done(tmp_path):
    """FALSE-POSITIVE GUARD: a real verdict WITH posting evidence completes."""
    resolved = _reap_role_reviewer(tmp_path, PASS_POSTED)
    assert resolved[0][2] == "done", (
        "#913 false-positive: a genuinely posted verdict rejected: " + resolved[0][3])


def test_reap_reviewer_discussing_posting_stays_done(tmp_path):
    """FALSE-POSITIVE GUARD (the #647-class trap): an honest verdict that
    merely DISCUSSES the posting-problem hazard, with evidence it posted,
    completes."""
    resolved = _reap_role_reviewer(tmp_path, PASS_DISCUSSING_POSTING)
    assert resolved[0][2] == "done", (
        "#913 false-positive: prose mentioning a posting failure rejected a "
        "real posted verdict: " + resolved[0][3])


def test_author_role_stranded_wording_is_untouched(tmp_path):
    """CONTROL: the new rule is reviewer-role scoped — an author lane whose
    result happens to contain the confession wording is judged by the
    existing #870 guards only (an author reporting it could not post
    something is a legitimate deliverable)."""
    executor = _executor()
    results_dir = Path(tmp_path) / "hermes-results"
    results_dir.mkdir(parents=True, exist_ok=True)
    task_id = "task_g913a"
    fp = results_dir / f"{task_id}.result.md"
    fp.write_text("Blocker report: could not post verdict comment upstream — "
                  "the issue tracker rejected the webhook. Escalating per "
                  "the return-value contract. " + ("x" * 200) + "\n",
                  encoding="utf-8")
    mock_proc = MagicMock()
    mock_proc.poll.return_value = 0
    mock_proc.pid = 4242
    mock_proc.stderr = None
    mock_proc.stdout = None
    spawn = ActiveSpawn(
        task_id=task_id, task_title="author report",
        process=mock_proc, lease_id=f"lease_{task_id}",
        started_at=time.time() - 60.0, lane_name=f"hermes-{task_id}",
        mode="hermes", result_path=str(fp),
        lane_key="cp#main", role="author",
    )
    resolved = []
    executor._reap_hermes_spawn(task_id, spawn, 60.0, resolved)
    assert resolved[0][2] == "done", (
        "author-role scoping violated: " + resolved[0][3])
