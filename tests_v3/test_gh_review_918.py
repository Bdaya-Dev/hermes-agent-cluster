"""#918 STAGE 1 — the reviewer lane records its verdict as a GitHub review
by the `bdaya-lane-agent` App (APPROVE / REQUEST_CHANGES), while still
posting the verdict NOTE, and while still having NO merge path (#882).

Why (owner ruling 2026-09-15, shared/claude-plugins#918): every merge control
today is a client-side hook that fails OPEN. The forge-side gate needs a
SECOND principal: lanes author as `ahmednfwela` and GitHub bars the author
from approving, so the reviewer submits the recorded review as the App whose
capability was proven live on throwaway PR#75 (200, state APPROVED, user
`bdaya-lane-agent[bot]`, type Bot — a recorded review, not the GITHUB_TOKEN
comment fallback).

Required shape (STAGE 1 only — this task must NOT enable branch protection
or auto-merge; that is a SEPARATE task citing the evidence this produces):
  * PASS at head      -> recorded APPROVE         (POST .../pulls/<n>/reviews)
  * NEEDS-CHANGES     -> recorded REQUEST_CHANGES
  * INCOMPLETE-ROSTER / verdict-less / NEEDS-HUMAN -> NO review submitted
  * the verdict NOTE is still posted (audit trail — the landing gate reads it)
  * the reviewer lane is given NO merge verb (#882 stays)
  * failure is loud: if the token cannot be minted or the review submission
    returns 4xx, the result says so — it must never report a successful
    review (#913's silent-failure class, reproduced nowhere new)
  * no secret value is ever printed or returned — byte length / crc only

RED PROOF (skill standard): on the pre-#918 template the brief for a
github.com/pull/ URL carries no review-submission instruction at all — the
brief-shape tests below fail on that DEFECT (AssertionError, not
ImportError). The ``gh_review`` module is imported INSIDE the tests that pin
it (repo standard, tests_v3/test_intake_grouping_762.py).
"""
from __future__ import annotations

import json
import re

import pytest

from hermes_cluster.core.intake_grouping import (
    BundlePlan,
    reviewer_handoff_brief,
)

GH_URL = "https://github.com/Bdaya-Dev/hermes-agent-cluster/pull/77"
SHA = "d1c8e5f0" + "0" * 32


def _bundle() -> BundlePlan:
    return BundlePlan(
        lane_key="hermes-agent-cluster#main",
        iids=[918],
        project_paths=["shared/claude-plugins"],
        priority=0,
        issue_ids=["shared/claude-plugins#918"],
        reason="test",
    )


def _gh_brief() -> str:
    return reviewer_handoff_brief(_bundle(), GH_URL, SHA)


# ------------------------------------------- brief shape (RED = AssertionError)


def test_brief_instructs_app_review_submission():
    """THE #918 defect: a GitHub-PR reviewer brief that stops at the note.
    It must direct the lane to record the review AS THE APP via gh_review."""
    text = _gh_brief()
    assert "gh_review" in text, "brief never tells the reviewer to submit an App review"
    assert "bdaya-lane-agent" in text


def test_brief_pass_maps_to_recorded_approve():
    text = _gh_brief()
    # PASS must produce the recorded APPROVE event via the tool, not a comment.
    assert re.search(r"PASS[^\n]*\n?[^\n]*(APPROVE|approve)", text, re.IGNORECASE)
    assert "--verdict" in text  # the CLI contract the lane actually runs


def test_brief_needs_changes_maps_to_request_changes():
    text = _gh_brief()
    assert "REQUEST_CHANGES" in text


def test_brief_no_review_on_incomplete_or_verdictless():
    """INCOMPLETE-ROSTER / verdict-less completions must produce NEITHER
    APPROVE nor REQUEST_CHANGES — #914's rule that 'completed' is never a
    verdict applies to the forge review too."""
    text = _gh_brief()
    assert "INCOMPLETE-ROSTER" in text
    assert re.search(r"no review|never.{0,40}review|submit.{0,40}NO review",
                     text, re.IGNORECASE)


def test_brief_still_posts_the_verdict_note():
    """Control (must stay true): the note is the human-readable record and
    the landing-gate oracle — the review is ADDED, never replaces it."""
    text = _gh_brief()
    assert "Post your verdict" in text
    assert "The note is the durable oracle" in text


def test_brief_gives_reviewer_no_merge_verb_for_its_own_review():
    """#882: approving is not merging. The #918 review instruction must
    itself state the reviewer gets NO merge verb — the landing task's
    instruction (present since #902) stays the only merge path."""
    text = _gh_brief()
    # the landing-task instruction may *name* gh pr merge as what the
    # LANDING task does; the reviewer must never be told to run it itself.
    assert "you do NOT land it yourself" in text
    assert "NO merge" in text


# ------------------------------------------------ module: verdict -> event map


def test_review_event_for_verdict_mapping():
    from hermes_cluster.core.gh_review import review_event_for_verdict
    assert review_event_for_verdict("PASS") == "APPROVE"
    assert review_event_for_verdict("pass") == "APPROVE"
    assert review_event_for_verdict("NEEDS-CHANGES") == "REQUEST_CHANGES"
    assert review_event_for_verdict("NEEDS-HUMAN") is None
    assert review_event_for_verdict("INCOMPLETE-ROSTER") is None
    assert review_event_for_verdict(None) is None
    assert review_event_for_verdict("") is None


# ------------------------------------------- module: submit paths (fake http)


class FakeHttp:
    """Injectable transport: records calls, scripts responses."""

    def __init__(self, post_response=None, post_error=None, reviews=None):
        self.calls = []
        self.post_response = post_response or {
            "id": 999, "state": "APPROVED", "html_url": "https://r/999"}
        self.post_error = post_error  # (status, body) -> raise
        self.reviews = reviews if reviews is not None else []

    def request(self, method, url, *, token, payload=None):
        self.calls.append({"method": method, "url": url, "payload": payload})
        assert token, "transport must receive a bearer token"
        if method == "POST" and self.post_error is not None:
            from hermes_cluster.core.gh_review import HttpError
            raise HttpError(self.post_error[0], self.post_error[1])
        if method == "POST":
            return self.post_response
        return {"reviews": self.reviews} if "reviews" in url else {}


def _approve_reviews():
    return [{"id": 999, "user": {"login": "bdaya-lane-agent[bot]", "type": "Bot"},
             "state": "APPROVED", "commit_id": SHA}]


def test_submit_pass_records_approve_at_pinned_sha():
    from hermes_cluster.core.gh_review import submit_pr_review
    http = FakeHttp(reviews=_approve_reviews())
    res = submit_pr_review(GH_URL, SHA, "PASS", body="RV-1 PASS",
                           token="ghs_" + "x" * 36, http=http)
    assert res["ok"] is True
    assert res["submitted"] is True
    assert res["event"] == "APPROVE"
    post = [c for c in http.calls if c["method"] == "POST"][-1]
    assert post["url"].endswith("/repos/Bdaya-Dev/hermes-agent-cluster/pulls/77/reviews")
    assert post["payload"]["event"] == "APPROVE"
    assert post["payload"]["commit_id"] == SHA  # sha-pinned, RV-1 discipline
    # verified by read-back, not by the 200 alone (#913 class: say what the
    # forge shows)
    assert res["review_state"] == "APPROVED"


def test_submit_needs_changes_records_request_changes():
    from hermes_cluster.core.gh_review import submit_pr_review
    reviews = [{"user": {"login": "bdaya-lane-agent[bot]", "type": "Bot"},
                "state": "CHANGES_REQUESTED", "commit_id": SHA}]
    http = FakeHttp(
        post_response={"id": 1, "state": "CHANGES_REQUESTED", "html_url": "u"},
        reviews=reviews)
    res = submit_pr_review(GH_URL, SHA, "NEEDS-CHANGES", body="findings",
                           token="***" + "y" * 36, http=http)
    assert res["ok"] and res["submitted"]
    assert res["event"] == "REQUEST_CHANGES"
    assert res["review_state"] == "CHANGES_REQUESTED"


@pytest.mark.parametrize("verdict", ["INCOMPLETE-ROSTER", None, "", "NEEDS-HUMAN"])
def test_submit_no_review_for_non_verdicts(verdict):
    """Produces NEITHER APPROVE nor REQUEST_CHANGES — and says so."""
    from hermes_cluster.core.gh_review import submit_pr_review
    http = FakeHttp()
    res = submit_pr_review(GH_URL, SHA, verdict, body="x",
                           token="***" + "z" * 36, http=http)
    assert res["ok"] is True
    assert res["submitted"] is False
    assert not [c for c in http.calls if c["method"] == "POST"], \
        "a non-verdict must never reach the reviews endpoint"


def test_submit_http_4xx_is_loud_failure():
    """#913 in a new place, forbidden: a rejected submission must report
    failure, never a successful review."""
    from hermes_cluster.core.gh_review import submit_pr_review
    http = FakeHttp(post_error=(422, {"message": "Validation Failed"}))
    res = submit_pr_review(GH_URL, SHA, "PASS", body="x",
                           token="***" + "w" * 36, http=http)
    assert res["ok"] is False
    assert res["submitted"] is False
    assert "422" in res["reason"]


def test_mint_failure_is_loud_and_never_success():
    """Token cannot be minted -> ok=False, no review claimed, reason says so."""
    from hermes_cluster.core import gh_review

    def boom(**kwargs):
        raise RuntimeError("resolver exit 1: key not provisioned")

    res = gh_review.submit_pr_review(GH_URL, SHA, "PASS", body="x",
                                     token_minter=boom)
    assert res["ok"] is False
    assert res["submitted"] is False
    assert "token" in res["reason"].lower() or "mint" in res["reason"].lower()


def test_no_secret_value_in_result_or_cli_output():
    """The installation token is short-lived but still a credential: results
    carry at most its byte length; output carries neither it nor the key."""
    from hermes_cluster.core.gh_review import submit_pr_review
    secret = "gh.s…f456"
    http = FakeHttp(reviews=_approve_reviews())
    res = submit_pr_review(GH_URL, SHA, "PASS", body="ok", token=secret, http=http)
    dumped = json.dumps(res)
    assert secret not in dumped
    assert "token_len" in res and res["token_len"] == len(secret)


def test_cli_reports_verdict_line_exit_zero_only_on_success(monkeypatch, capsys):
    """CLI: PASS -> submitted+verified line, exit 0; mint failure -> exit !=
    0 with a loud reason (the lane must SAY SO in its result)."""
    from hermes_cluster.core import gh_review

    class _H(FakeHttp):
        pass

    monkeypatch.setattr(
        gh_review, "_default_token",
        lambda: ("ghs_" + "a" * 36, "cache"))
    monkeypatch.setattr(
        gh_review, "_default_http", lambda: _H(reviews=_approve_reviews()))
    rc = gh_review.main(["--mr-url", GH_URL, "--sha", SHA,
                         "--verdict", "PASS", "--body", "RV-1 PASS"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "REVIEW APPROVE" in out
    assert "ghs_" not in out

    def boom(**kwargs):
        raise RuntimeError("no key")
    monkeypatch.setattr(gh_review, "_default_token", boom)
    rc2 = gh_review.main(["--mr-url", GH_URL, "--sha", SHA,
                          "--verdict", "PASS", "--body", "x"])
    assert rc2 != 0


def test_module_has_no_merge_path():
    """#882 + stage-1 scope: this module must not be able to merge, set
    auto-merge, or touch branch protection — grep its own source."""
    import inspect
    from hermes_cluster.core import gh_review
    src = inspect.getsource(gh_review)
    # strip docstrings/comments crudely: check no *code* calls the verbs
    code = re.sub(r'"""[\s\S]*?"""|#[^\n]*', "", src)
    assert "gh pr merge" not in code
    assert re.search(r"/merge\b", code) is None
    assert "auto_merge" not in code
    assert "branch_protection" not in code
    # the reviews endpoint is the only write verb:
    write_urls = re.findall(r'f"[^"]*(/repos/[^"]+)"', code)
    for u in write_urls:
        assert u.endswith("/reviews") or "/pulls/" in u
