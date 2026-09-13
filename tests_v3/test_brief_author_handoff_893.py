"""#893: the SHARED lane-brief template teaches the author lane to hand off.

The rule lives in ``AgentExecutor._write_brief`` — the one place every lane
is guaranteed to read — NOT copy-pasted per brief. RV-1 invariants the
template must carry:

  - author lanes are told to submit their OWN reviewer via
    kanban_cluster_submit (role reviewer, requires ['review'], carrying the
    MR URL + head sha through the brief text),
  - the author is told it MUST NOT approve/merge its own work (unchanged
    standing rule + explicit RV-1 line),
  - reviewer briefs must NOT receive the author hand-off block (a reviewer
    told to "submit your own reviewer" invites review loops and pressure
    toward self-review — the one outcome #893 must not enable).
"""
from __future__ import annotations

from pathlib import Path

from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig


def _executor(tmp_path: Path) -> AgentExecutor:
    return AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60,
                                   working_dir=str(tmp_path)),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _brief(tmp_path: Path, role: str) -> str:
    ex = _executor(tmp_path)
    p = ex._write_brief("task_abc", "do the thing", "DETAILS", lane_key="repo#br", role=role)
    return p.read_text(encoding="utf-8")


def test_author_brief_carries_handoff_rule(tmp_path):
    text = _brief(tmp_path, "author")
    assert "Author hand-off" in text
    assert "kanban_cluster_submit" in text
    assert "requires=['review']" in text
    assert "role='reviewer'" in text
    assert "MUST NOT approve or merge" in text


def test_reviewer_brief_does_not_carry_handoff_rule(tmp_path):
    text = _brief(tmp_path, "reviewer")
    assert "Author hand-off" not in text
    assert "kanban_cluster_submit" in text  # the reviewer's OWN landing hand-off
    assert "Reviewer landing hand-off" in text
    assert "MUST NOT merge" in text
    # The never-self-approve standing rule still holds for every lane.
    assert "NEVER approve or merge your own work" in text


def test_author_brief_carries_landing_exception(tmp_path):
    """A landing task (role author) must not spawn a reviewer-for-the-merge loop."""
    text = _brief(tmp_path, "author")
    assert "LANDING task" in text
    assert "does NOT dispatch another reviewer" in text


def test_handoff_rule_appears_once_per_brief(tmp_path):
    text = _brief(tmp_path, "author")
    assert text.count("Author hand-off") == 1
