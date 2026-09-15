"""#919 (P0) — a reviewer task can reach `completed` with NO VERDICT.

status=completed on a reviewer lane is not evidence a review happened. The
lead measured it on hosted main (2026-09-15, every role=reviewer +
status=completed row): 58 completed, 41 carry a verdict token, 17 carry NONE
(~29%), including two at 0 bytes (shared/devops/frappe-byo-storage!6,
shared/claude-plugins!941). Independently corroborated from the other side:
the bayader-backend#env/dev lane measured MR !180's dispatched reviewer as
TRANSCRIPT-PROMOTED with 0 notes on the MR — the loop stalled silently.

Establishing the split (re-measured 2026-09-15T21:1xZ against each MR's own
notes, .a224_split.py in-lane): of the GitLab-artifact verdict-less rows, 15
DO have a sha-pinned verdict note on the MR — the review happened and only the
task-result report evaporated (REPORTING shape: a re-dispatch must not burn a
second full review on these); 7 have no verdict note anywhere — the artifact
is genuinely unreviewed while the board says 'completed' (GATE shape). The
status word lies in BOTH classes, so the fix refuses the lie at its source.

Fix shape (stop the lie at the terminal transition, fail-closed, recovery
preserved — the brief's option 1, which also feeds option 2's consumers):

  * POST /api/v1/tasks/{id}/complete — the server-side seam every completion
    crosses (the executor's _report_completion, plugin.py, and any direct
    caller) — refuses the completed flip for role=reviewer unless the
    delivered result carries a verdict token
    (PASS | NEEDS-CHANGES | NEEDS-HUMAN | INCOMPLETE-ROSTER | NOT-READY),
    judged with #914's landing_gate vocabulary (single source of truth,
    extended with NOT-READY the sweep's stage-1 bounce verdict) and honoring
    the #871 rule: a TRANSCRIPT-PROMOTED body is never verdict-grade.
  * The refusal is the #870 re-queue machinery main already owns: lease
    revoked, attempts bumped, task back to `ready` for another delivery,
    fail_reason naming the violated gate — consumed as `failed` with the
    typed reason at the cap. A later round on the SAME persistent lane key
    superseding an earlier empty one (the measured !941 recovery: 0 bytes
    then a 4964-byte PASS) must keep working: terminal predecessors never
    dedupe (#898) and re-queue is non-terminal.
  * Landing rows exempt: role=reviewer is stamped on landing-shaped tasks by
    sweeps and hand-offs (measured live: land-kb-102-v3, invora-devops!793-
    land-pc completed verdict-less — a merge result, not a review). Gating
    them would hold healthy merges — a false positive, which is worse than
    the bug (#870 doctrine). landing_artifact() is the same recogniser #914
    uses at promotion.

RED on origin/main: complete_task flips ANY role to completed on the status
word alone — the verdict-less reviewer reaps 200 'completed' and the board
keeps lying. The recovery-guard and normal-completion controls are GREEN
before and after (regression locks).
"""

from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import TaskStatus
from hermes_cluster.routers import tasks as tasks_mod

# import NEW symbols INSIDE the tests that pin them (repo standard).

PASS_RESULT = ("# Independent review verdict\n\n"
               "**Reviewer verdict: PASS**\n"
               "**SHA:** deadbeef1234\n")
NOT_READY_RESULT = ("Reviewer verdict: NOT-READY — the MR is Draft; "
                    "the dispatch IS the summon, nothing to review yet.")
NO_VERDICT_RESULT = ("## Review Task Blocker Report\n\n"
                     "Unable to perform the review due to project path "
                     "resolution failure. 404 on every candidate.")


def _client():
    app = create_app(cluster_id="test-cluster", node_id="test-node",
                     node_role="main")
    return TestClient(app)


def _submit(c, title, **kw):
    body = {"title": title}
    body.update(kw)
    r = c.post("/api/v1/tasks", json=body)
    assert r.status_code in (200, 201), r.text
    return r.json()


# ---------------------------------------------------------------------------
# RED: the lie refused at its source.
# ---------------------------------------------------------------------------

def test_verdictless_reviewer_does_not_reach_completed():
    """The defect (#919): a reviewer whose delivered result carries no
    verdict word must NOT land in a clean `completed`."""
    c = _client()
    t = _submit(c, "INDEPENDENT REVIEW of repo!1", role="reviewer",
                lane_key="repo!1", requires=["review"])
    r = c.post(f"/api/v1/tasks/{t['id']}/complete",
               json={"result": NO_VERDICT_RESULT})
    assert r.status_code in (200, 409), r.text  # refuses the flip, not the call
    row = c.get(f"/api/v1/tasks/{t['id']}").json()
    assert row["status"] != TaskStatus.completed.value, (
        f"#919: a verdict-less reviewer reached completed — status=completed "
        f"was reported as evidence a review happened. row={row['status']} "
        f"fail_reason={row.get('fail_reason')!r}")


def test_zero_byte_reviewer_handled_identically():
    """A 0-byte completion is the same class: refused identically to a
    non-empty verdict-less body (the frappe-byo-storage!6 / !941 shape)."""
    c = _client()
    a = _submit(c, "INDEPENDENT REVIEW of repo!2", role="reviewer",
                lane_key="repo!2", requires=["review"])
    b = _submit(c, "INDEPENDENT REVIEW of repo!3", role="reviewer",
                lane_key="repo!3", requires=["review"])
    ra = c.post(f"/api/v1/tasks/{a['id']}/complete", json={"result": ""})
    rb = c.post(f"/api/v1/tasks/{b['id']}/complete", json={"result": NO_VERDICT_RESULT})
    sa = c.get(f"/api/v1/tasks/{a['id']}").json()["status"]
    sb = c.get(f"/api/v1/tasks/{b['id']}").json()["status"]
    assert ra.status_code == rb.status_code
    assert sa == sb, f"0-byte ({sa}) and verdict-less ({sb}) must land the same"
    assert sa != TaskStatus.completed.value, (
        f"#919: both are verdict-less reviewer shapes; got {sa}")


def test_promoted_transcript_with_verdict_words_is_no_verdict():
    """#871 made MECHANICAL at the flip: a TRANSCRIPT-PROMOTED body that
    mentions PASS inside the captured transcript is NOT verdict-grade."""
    c = _client()
    t = _submit(c, "INDEPENDENT REVIEW of repo!4", role="reviewer",
                lane_key="repo!4", requires=["review"])
    body = ("<!-- TRANSCRIPT-PROMOTED: the executor copied this from the "
            "child's stdout; the lane did not write a deliverable. NOT a "
            "verdict-grade result (shared/claude-plugins#871). -->\n"
            "# Review of PR #4\n\nVerdict: PASS at deadbeef1234\n")
    r = c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": body})
    assert r.status_code in (200, 409), r.text
    row = c.get(f"/api/v1/tasks/{t['id']}").json()
    assert row["status"] != TaskStatus.completed.value, (
        "#871/#919: a promoted transcript is never a verdict — do not "
        f"accept it at the terminal transition (status={row['status']})")


def test_refusal_requeues_and_consumes_under_the_cap():
    """The refusal is recovery, not a dead end: main's existing #870
    machinery bumps attempts, drops the task out of completed, and the
    cap (task_retry_limit) consumes it as `failed` with the typed reason."""
    c = _client()
    t = _submit(c, "INDEPENDENT REVIEW of repo!5", role="reviewer",
                lane_key="repo!5", requires=["review"])
    cap = int(getattr(c.app.state, "cluster_state", None)
              and getattr(_state_of(c), "task_retry_limit", 3) or 3) \
        if hasattr(c.app.state, "cluster_state") else 3
    from hermes_cluster.routers.tasks import _state as st
    cap = int(getattr(st, "task_retry_limit", 3))
    for i in range(cap + 1):
        r = c.post(f"/api/v1/tasks/{t['id']}/complete",
                   json={"result": NO_VERDICT_RESULT})
        assert r.status_code in (200, 409), (i, r.text)
        row = c.get(f"/api/v1/tasks/{t['id']}").json()
        assert row["status"] != TaskStatus.completed.value
    assert row["status"] == TaskStatus.failed.value, (
        f"at the cap the task must be consumed `failed`, got {row['status']}")
    assert "no verdict" in (row.get("fail_reason") or "").lower(), (
        "the typed reason must name the violated gate (#919), got "
        f"{row.get('fail_reason')!r}")


def _state_of(c):
    from hermes_cluster.routers.tasks import _state
    return _state


def test_reviewer_gate_symbols_pinned():
    """Direct unit of the new pure module (repo standard: NEW symbols
    imported INSIDE the test that pins them)."""
    from hermes_cluster.core.reviewer_gate import (
        reviewer_verdict, reviewer_completion_veto)

    assert reviewer_verdict("") is None
    assert reviewer_verdict("just prose, no verdict") is None
    assert reviewer_verdict("Reviewer verdict: NEEDS-CHANGES") == "NEEDS-CHANGES"
    assert reviewer_verdict("Verdict: NOT-READY") == "NOT-READY"
    assert reviewer_verdict("# Review Result: PASS\n") == "PASS"

    class L:  # minimal task stand-in
        def __init__(self, role, lane):
            self.role, self.lane_key, self.title = role, lane, ""

    assert reviewer_completion_veto(L("author", "x#main"), "whatever") is None
    assert reviewer_completion_veto(L("reviewer", "repo!3"),
                                    "no verdict here") is not None
    assert reviewer_completion_veto(L("reviewer", "repo!3"),
                                    "Reviewer verdict: PASS") is None
    # landing rows never gated regardless of role
    assert reviewer_completion_veto(L("reviewer", "repo#land-3"),
                                    "merged") is None
    # #871: a promoted transcript is never verdict-grade
    assert reviewer_verdict("<!-- TRANSCRIPT-PROMOTED marker -->\n"
                            "Verdict: PASS quoted by the transcript\n") is None


# ---------------------------------------------------------------------------
# Grammar probes from the live-board hazard audit (.a224_audit2.py): the
# gate must admit every honest verdict shape the board carries and refuse
# only genuine absence — a false positive vetoes good lanes (#870 doctrine).
# ---------------------------------------------------------------------------

def test_board_grammar_variants_are_admitted():
    """The shapes measured among the 372 completed reviewers: header/line
    tokens, em-dash APPROVE, table rows, bare tokens, underscore variants."""
    bodies = [
        "# Review Result: PASS\n**MR:** x!5\n",
        "# RV-1 Verdict: PR #308 — APPROVE\n",
        "### !923 — PASS\n- **SHA:** 6cff7fd0952194f4a7fffc803ee87c49be342559\n",
        "**Verdict:** ✅ PASS — Non-dilution confirmed.\n",
        "INCOMPLETE_ROSTER\n",
        "| note 137151 | Scoped stays-open | ✅ PASS — \"stays-open\" |\n",
    ]
    for i, body in enumerate(bodies):
        c = _client()
        t = _submit(c, f"INDEPENDENT REVIEW of repo!{30+i}", role="reviewer",
                    lane_key=f"repo!{30+i}", requires=["review"])
        r = c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": body})
        assert r.status_code == 200, (i, r.text)
        assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
            TaskStatus.completed.value, f"body {i} wrongly vetoed"


def test_mid_prose_tokens_do_not_admit_a_completion():
    """The false-positive DIRECTION of the same audit: a body that merely
    quotes vocabulary words inside sentences is still no verdict."""
    bodies = [
        "The build passes the test suite every run and we celebrated.",
        "We discussed the PASS-the-parcel game at standup today.",
        "This PR is about the approve button; nothing else. Fine.",
        "I looked at the diff and filed notes on lines 12 and 30.",
    ]
    for i, body in enumerate(bodies):
        c = _client()
        t = _submit(c, f"INDEPENDENT REVIEW of repo!{40+i}", role="reviewer",
                    lane_key=f"repo!{40+i}", requires=["review"])
        c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": body})
        assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] != \
            TaskStatus.completed.value, f"body {i} should NOT be a verdict"


def test_control_lane_word_land_exempt_without_repo_hash_shape():
    """Measured live: landing lanes that are neither '<repo>#land-<n>' nor
    LAND-leading titles (invora-devops!793-land-pc, land-kb-102-v3) carry
    role=reviewer and deliver merge results. The word-boundary exemption
    must cover them — gating a merge report for a verdict word is the
    stranded-gate failure mode this fleet has died of."""
    c = _client()
    t = _submit(c, "merge the passed MR", role="reviewer",
                lane_key="land-kb-102-v3", requires=["github-write"])
    r = c.post(f"/api/v1/tasks/{t['id']}/complete",
               json={"result": "# Land Result: shared/knowledge-base!102\nstate: merged\n"})
    assert r.status_code == 200, r.text
    assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
        TaskStatus.completed.value


# ---------------------------------------------------------------------------
# Controls — GREEN before and after. The recovery path and honest completions
# must survive the fix untouched.
# ---------------------------------------------------------------------------

def test_control_pass_reviewer_completes_clean():
    c = _client()
    t = _submit(c, "INDEPENDENT REVIEW of repo!6", role="reviewer",
                lane_key="repo!6", requires=["review"])
    r = c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": PASS_RESULT})
    assert r.status_code == 200, r.text
    assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
        TaskStatus.completed.value


def test_control_needs_human_and_incomplete_roster_are_verdicts():
    """Refusing the ABSENCE of a verdict must not refuse an honest negative
    verdict — those are exactly the shapes the gate must let stand."""
    for i, body in enumerate([
        "Reviewer verdict: NEEDS-HUMAN — roster missing artifact-registry-read.",
        "Reviewer verdict: INCOMPLETE-ROSTER — github-write absent on node.",
        NOT_READY_RESULT,
    ]):
        c = _client()
        t = _submit(c, f"INDEPENDENT REVIEW of repo!{20+i}", role="reviewer",
                    lane_key=f"repo!{20+i}", requires=["review"])
        r = c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": body})
        assert r.status_code == 200, (i, r.text)
        assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
            TaskStatus.completed.value, i


def test_control_author_completion_untouched():
    c = _client()
    t = _submit(c, "author work on repo", role="author", lane_key="repo#main")
    r = c.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": "shipped"})
    assert r.status_code == 200, r.text
    assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
        TaskStatus.completed.value


def test_control_landing_shaped_reviewer_completes_without_verdict():
    """The false-positive guard (measured live: land-kb-102-v3 and
    invora-devops!793-land-pc carry role=reviewer but deliver MERGE results,
    not verdicts). A landing row must never be gated for a verdict word."""
    c = _client()
    t = _submit(c, "LAND repo!7 at sha deadbeef1234 — verdict PASS round 2",
                role="reviewer", lane_key="repo#land-7",
                requires=["github-write"])
    r = c.post(f"/api/v1/tasks/{t['id']}/complete",
               json={"result": "# LAND repo!7 — merged. merge_commit deadbeef1"})
    assert r.status_code == 200, r.text
    assert c.get(f"/api/v1/tasks/{t['id']}").json()["status"] == \
        TaskStatus.completed.value


def test_control_no_body_complete_behaves_by_role():
    """Callers that POST no body keep working for authors (the documented
    pre-#874 shape); a reviewer still cannot slip a verdict-less completed
    through an empty body."""
    c = _client()
    a = _submit(c, "author task", role="author", lane_key="repo#main")
    r = c.post(f"/api/v1/tasks/{a['id']}/complete")
    assert r.status_code == 200 and c.get(f"/api/v1/tasks/{a['id']}").json()["status"] == \
        TaskStatus.completed.value
    b = _submit(c, "INDEPENDENT REVIEW of repo!9", role="reviewer",
                lane_key="repo!9", requires=["review"])
    r2 = c.post(f"/api/v1/tasks/{b['id']}/complete")
    assert c.get(f"/api/v1/tasks/{b['id']}").json()["status"] != \
        TaskStatus.completed.value, (
        "#919: an empty result is verdict-less by definition; got "
        f"{r2.status_code} {r2.text[:120]}")


# ---------------------------------------------------------------------------
# REGRESSION GUARD — the !941 recovery path must be GREEN before AND after.
# ---------------------------------------------------------------------------

def test_recovery_later_round_on_same_lane_supersedes_empty_completion():
    """GREEN before AND after — a regression guard on behaviour the fix
    must not break. shared/claude-plugins!941 appears TWICE on the board:
    once at 0 bytes, once at 4964 bytes with a PASS. Whatever the first
    attempt's empty completion lands as (base: silently `completed`; the
    fix: re-queued or consumed at the cap), the lane '<repo>!<n>' must
    still receive a later round that delivers a real verdict and completes
    clean. On base this holds because the terminal row never dedupes; the
    fix must preserve it through BOTH of its outcomes (a re-queued row is
    re-deliverable in place; a consumed row is replaceable)."""
    c = _client()
    first = _submit(c, "INDEPENDENT REVIEW of repo!8", role="reviewer",
                    lane_key="repo!8", requires=["review"])
    c.post(f"/api/v1/tasks/{first['id']}/complete", json={"result": ""})

    # The next sitting's hand-off on the same lane key: creatable either as
    # a fresh row (terminal predecessor) or as a #898 dedupe returning the
    # re-queued row (same id, still dispatchable) — recovery is blocked only
    # if the lane yields no deliverable row at all.
    second = c.post("/api/v1/tasks", json={
        "title": "INDEPENDENT REVIEW of repo!8 round 2", "role": "reviewer",
        "lane_key": "repo!8", "requires": ["review"]})
    assert second.status_code in (200, 201), second.text
    s = second.json()
    assert not s.get("deduped") or s["status"] in ("pending", "ready"), (
        f"recovery blocked: lane repo!8 cannot be re-reviewed: {s}")

    # ...and the recovery round delivers a real verdict and completes clean.
    rr = c.post(f"/api/v1/tasks/{s['id']}/complete", json={"result": PASS_RESULT})
    assert rr.status_code == 200, rr.text
    assert c.get(f"/api/v1/tasks/{s['id']}").json()["status"] == \
        TaskStatus.completed.value
