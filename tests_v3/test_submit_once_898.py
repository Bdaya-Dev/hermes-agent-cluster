"""#898 item 2/3 — plugin surfaces dedup; briefs pin submit-ONCE language.

Part 2 (plugin): when main answers a submit with `deduped: true`, the tool
result must carry the flag AND the existing id — loud enough that a lane
cannot mistake it for a new task. Measured loop: the lane treated every
submit as new, cancelled, resubmitted — four open reviewer tasks.

Part 3 (briefs): `bundle_brief` and `reviewer_handoff_brief` in
hermes_cluster/core/intake_grouping.py must carry the submit-ONCE rule
verbatim tokens: exactly one reviewer task after the READY gate, then
FINISH the lane — never wait for, cancel, or resubmit the reviewer.

These import nothing new at module top (red-proof standard #872): a run
against a tree without the fix fails on the assertions, not ImportError.
"""

import json

import pytest

from hermes_cluster import plugin
from hermes_cluster.core.intake_grouping import (
    BundlePlan,
    bundle_brief,
    reviewer_handoff_brief,
)


# ---------------------------------------------------------------------------
# Part 2 — handle_cluster_submit surfaces the dedup
# ---------------------------------------------------------------------------

def _capture_submit(monkeypatch, responses):
    """Route _api_call through a fake; responses keyed by (method, path)."""
    calls = []

    def fake(method, path, data=None):
        calls.append((method, path, data))
        return responses.get(path, {})

    monkeypatch.setattr(plugin, "_api_call", fake)
    return calls


def test_submit_surfaces_deduped_flag_and_existing_id(monkeypatch):
    calls = _capture_submit(monkeypatch, {
        "/api/v1/tasks": {"id": "task_existing", "status": "ready",
                          "lane_key": "L-rev", "deduped": True},
        "/api/v1/schedule/trigger": {"status": "ok"},
    })
    out = json.loads(plugin.handle_cluster_submit({
        "title": "REVIEW L PR", "role": "reviewer", "lane_key": "L-rev",
    }))
    assert out.get("deduped") is True, "the tool result must keep the dedup flag"
    assert out.get("id") == "task_existing", "and the EXISTING task id"


def test_submit_stamps_source_task_id_from_env(monkeypatch):
    """#898 zombie gate plumbing: inside a cluster spawn the plugin declares
    the session's own task so main can refuse a submit from a cancelled one."""
    monkeypatch.setenv("HERMES_CLUSTER_TASK_ID", "task_owner_1")
    calls = _capture_submit(monkeypatch, {
        "/api/v1/tasks": {"id": "task_new"},
        "/api/v1/schedule/trigger": {"status": "ok"},
    })
    plugin.handle_cluster_submit({"title": "x", "lane_key": "L"})
    submit = [c for c in calls if c[1] == "/api/v1/tasks"][0]
    assert submit[2]["source_task_id"] == "task_owner_1"


def test_submit_without_env_sends_no_source_task_id(monkeypatch):
    monkeypatch.delenv("HERMES_CLUSTER_TASK_ID", raising=False)
    calls = _capture_submit(monkeypatch, {
        "/api/v1/tasks": {"id": "task_new"},
        "/api/v1/schedule/trigger": {"status": "ok"},
    })
    plugin.handle_cluster_submit({"title": "x", "lane_key": "L"})
    submit = [c for c in calls if c[1] == "/api/v1/tasks"][0]
    assert "source_task_id" not in submit[2]


def test_submit_surfaces_409_zombie_refusal_reason(monkeypatch):
    """A refused submit (main answered 409 with a detail body) must reach the
    lane as {error: <reason>}, not a bare transport string. Exercised at the
    _api_call HTTPError leg with a stubbed urlopen."""
    import io

    class _409(Exception):
        pass

    from urllib.error import HTTPError
    err = HTTPError("http://x/api/v1/tasks", 409, "Conflict", {},
                    io.BytesIO(json.dumps(
                        {"detail": "submitting session's task is not active"}
                    ).encode()))

    monkeypatch.setattr(plugin, "_base_url", "http://cluster.test")
    monkeypatch.setattr(plugin, "urlopen", lambda *a, **k: (_ for _ in ()).throw(err))
    out = plugin._api_call("POST", "/api/v1/tasks", {"title": "x"})
    assert "not active" in json.dumps(out), (
        f"409 reason must survive to the lane: {out}")


# ---------------------------------------------------------------------------
# Part 3 — briefs pin the submit-ONCE discipline
# ---------------------------------------------------------------------------

def _bundle():
    return BundlePlan(
        lane_key="sami#claude-plugins",
        iids=[898],
        project_paths=["sami/claude-plugins"],
        priority=3,
        issue_ids=["sami/claude-plugins#898"],
        reason="test",
    )


@pytest.mark.parametrize("render", [
    lambda b: bundle_brief(b),
    lambda b: reviewer_handoff_brief(b, "https://git.example/-/merge_requests/1",
                                    "abc1234"),
])
def test_briefs_carry_submit_once_tokens(render):
    text = render(_bundle())
    lowered = text.lower()
    # ONE reviewer task, then finish — the three tokens of the rule:
    assert "one reviewer task" in lowered or "exactly one" in lowered, (
        "brief must say: submit exactly ONE reviewer task")
    assert "finish" in lowered, "brief must say: then FINISH the lane"
    assert "never" in lowered and ("resubmit" in lowered or "re-submit" in lowered), (
        "brief must forbid resubmitting the reviewer")
    assert "cancel" in lowered, "brief must forbid cancelling the queued reviewer"


def test_bundle_brief_states_head_move_is_the_reviewers():
    """The measured excuse for resubmitting was 'the head moved, my task
    reviews a stale sha'. The brief must pin: a moved head is the
    REVIEWER's to re-pin, not the author's to replace."""
    text = bundle_brief(_bundle()).lower()
    assert "moved head" in text or "head is" in text, (
        "brief must state that a moved head belongs to the reviewer lane")
