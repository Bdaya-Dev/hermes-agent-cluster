"""#913 (P0 safety) — a landing task auto-spawned on a REJECTED review: no
supersede check, no author!=lander check.

The protocol (#893/#902): an author lane produces a diff, hands to a
fresh-context reviewer, and the reviewer SUBMITS a LANDING task with
depends_on=[<reviewer task id>] so it waits for the verdict (#905 made the
gate REAL: the landing holds pending until the dependency completes). But
COMPLETION IS NOT VERDICT-AWARE: a reviewer that delivers a REJECTION
closes its task exactly like one that PASSes, _trigger_downstream promotes
the dependent landing to ready, and the fleet merges work the independent
gate refused. That is the auto-spawn. Neither promised check exists on
main:

  (1) SUPERSEDE — main reads the LATEST completed reviewer verdict on the
      artifact and requires it to be PASS *at the head the landing names*;
      a later rejection round supersedes an earlier PASS; a verdict-less
      completed reviewer (#871's TRANSCRIPT-PROMOTED class) is NO verdict —
      'completed' is not a pass.
  (2) AUTHOR != LANDER — the lane that REVIEWED (or authored) the artifact
      may not be its lander lane.

MEASURED live (hosted board, signed GET /api/v1/tasks, times exact):
shared/knowledge-base#land-104 (task_477eba7bf2746d0e) was created
2026-09-14T13:30:08 titled 'Land MR !104 at PASS head sha 19823d37...'
while the ONLY completed reviewer verdict at that instant was
NEEDS-CHANGES (task_cd7a2df25846ea91, completed 13:15:12) — the round-2
PASS completed 13:30:59, 51 seconds AFTER the lander was promoted;
depends_on was empty (#905 never applied) and !104's GitLab author is the
bot identity bdaya-agent — a LEAD-AUTHORED MR with no author lane, the
exact title of this defect. The MR merged 13:43:10. Same shape, different
instant: morshdy-flutter#land-2 (task_025562263081b23d) created
15:20:33.876 citing "Verdict: PASS" while its reviewer
(task_c7af3466eb55abf3) completed at 15:21:49 — the lander preceded the
verdict. And hermes-gateway-image PR #6 shows the ORDER the gate must
respect: round-1 NEEDS-CHANGES on lane '!6', round-2 PASS on '!6-rev2',
landing on the rev2 PASS — the latest completed round, whatever its lane
twin, is the truth.

Fix shape: at the two promotion boundaries (submit-time for a
deps-already-met landing, _trigger_downstream for the auto-spawn), a
landing task ("<repo>#land-<n>" lane key) is HELD pending unless the board's
latest reviewer verdict on <repo>!<n> is PASS at the sha the landing names
and the lander lane is neither the reviewer lane nor an authoring lane of
the artifact. A held landing gets fail_reason 'landing held: ...' and stays
pending — an operator with real authority (owner ruling) can advance it
with POST /tasks/{id}/advance.

RED on origin/main@8114242: _trigger_downstream promotes on completion
alone. Each test drives that exact path. Controls that must hold on main
too: a PASS-at-sha landing auto-spawns today (and must keep spawning), and
lane-less dependency tasks are untouched.
"""

import time

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import TaskStatus
from hermes_cluster.routers import tasks as tasks_mod


@pytest.fixture()
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def _submit(client, title, **kw):
    body = {"title": title}
    body.update(kw)
    return client.post("/api/v1/tasks", json=body)


PASS_RESULT = ("# Independent review verdict\n\n"
               "**Verdict**: PASS\n"
               "**Head sha**: deadbeef1234\n"
               "**Reviewer lane**: repo#main-rev\n")
REJECT_RESULT = ("# Independent review verdict\n\n"
                 "**Verdict**: NEEDS-CHANGES\n"
                 "**Head sha**: deadbeef1234\n"
                 "Finding: the gate reads verdicts, not brief claims.\n")
NO_VERDICT_RESULT = ("No files found containing 'landing task' patterns. "
                     "The search came up empty.")


def _review(client, lane="repo!2", result=PASS_RESULT, complete=True):
    r = _submit(client, f"INDEPENDENT REVIEW of {lane}", role="reviewer",
                lane_key=lane, requires=["review"])
    assert r.status_code in (200, 201), r.text
    tid = r.json()["id"]
    if complete:
        cr = client.post(f"/api/v1/tasks/{tid}/complete",
                         json={"result": result})
        assert cr.status_code == 200, cr.text
    return tid


def _land(client, reviewer_id, lane="repo#land-2",
          title="LAND repo!2 — gh pr view MUST equal deadbeef1234 → merge"):
    """The #902 reviewer-submitted landing: gated on the reviewer task. The
    auto-spawn is _trigger_downstream firing when that task completes — so
    submit it AFTER the reviewer completed, exactly like the live order."""
    r = _submit(client, title, role="author", lane_key=lane,
                requires=["github-write"], depends_on=[reviewer_id])
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _task(client, tid):
    r = client.get(f"/api/v1/tasks/{tid}")
    assert r.status_code == 200
    return r.json()


# ---------------------------------------------------------------------------
# 1. The auto-spawn: landing promoted on reviewer completion must read the
#    verdict, not the status word.
# ---------------------------------------------------------------------------

def test_rejected_review_holds_the_landing(client):
    """Specimen shape (bayader-flutter!224): reviewer completes with a
    NEEDS-CHANGES; today completion promotes the dependent landing to ready
    and it merges. Must stay pending, with the reason recorded."""
    rid = _review(client, lane="repo!2", result=REJECT_RESULT)
    lid = _land(client, rid)
    t = _task(client, lid)
    assert t["status"] == TaskStatus.pending.value, (
        "landing auto-spawned on a REJECTED review — #913: completion was "
        f"read as a pass; status={t['status']}")
    reason = (t.get("fail_reason") or "").lower()
    assert "needs-changes" in reason or "reject" in reason, (
        "a held landing must say WHY: " + repr(t.get("fail_reason")))


def test_verdictless_reviewer_result_holds_the_landing(client):
    """The #871 class on the gate side: a reviewer that 'completed' with a
    transcript-promoped non-verdict body is NO verdict — it never becomes
    a pass by the status word."""
    rid = _review(client, lane="repo!2", result=NO_VERDICT_RESULT)
    lid = _land(client, rid)
    assert _task(client, lid)["status"] == TaskStatus.pending.value


def test_pass_at_named_sha_spawn_proceeds(client):
    """Control (green on main too): the healthy path — a real PASS verdict
    at the sha the landing names spawns the landing exactly as #902/905
    designed."""
    rid = _review(client, lane="repo!2", result=PASS_RESULT)
    lid = _land(client, rid)
    assert _task(client, lid)["status"] == TaskStatus.ready.value, (
        "the verdict gate must not break the PASS path")


def test_stale_sha_landing_held(client):
    """Specimen B (bayader-flutter!225): PASS pinned to head A; the author
    later pushed head B; a landing naming B cannot ride A's verdict."""
    rid = _review(client, lane="repo!2", result=PASS_RESULT)  # @deadbeef1234
    lid = _land(client, rid,
                title="LAND repo!2 — MUST equal cafe00019999 → merge")
    t = _task(client, lid)
    assert t["status"] == TaskStatus.pending.value, (
        "a PASS at one sha does not certify a landing at another (#913)")
    assert "sha" in (t.get("fail_reason") or "").lower()


def test_later_rejection_supersedes_earlier_pass(client):
    """Round 1 PASS, round 2 NEEDS-CHANGES (the same artifact): the LATEST
    completed verdict governs. (A later PASS would un-hold — round order is
    by completion, not submission.)"""
    rid1 = _review(client, lane="repo!2", result=PASS_RESULT)
    rid2 = _review(client, lane="repo!2", result=REJECT_RESULT)
    lid = _land(client, rid1)
    assert _task(client, lid)["status"] == TaskStatus.pending.value


def test_newest_pass_supersedes_older_rejection(client):
    """Round 1 rejected, author fixed, round 2 PASS at the moved head: the
    landing for the NEW pass+sha spawns (bounded: a rejection cannot strand
    the artifact forever once genuinely cleared)."""
    r1 = ("# Independent review verdict\n\n**Verdict**: NEEDS-CHANGES\n"
          "**Head sha**: aaa1111\n")
    rid1 = _review(client, lane="repo!2", result=r1)
    rid2 = _review(client, lane="repo!2",
                   result=("# Independent review verdict\n\n"
                           "**Verdict**: PASS\n**Head sha**: bbb2222\n"))
    lid = _land(client, rid2,
                title="LAND repo!2 — MUST equal bbb2222 → merge")
    assert _task(client, lid)["status"] == TaskStatus.ready.value


def test_re_review_round_on_a_twin_lane_is_read(client):
    """The measured PR#6 flow (hermes-gateway-image): round 1 rejected on
    lane 'repo!6', round 2 PASS on the re-review twin 'repo!6-rev2' at the
    same head, lander cites the rev2 PASS — it must proceed. A gate that
    only reads the plain lane would hold this landing forever, and holding
    the healthy path is how gates die."""
    rid1 = _review(client, lane="repo!6", result=(
        "# Review Complete\n**Verdict:** NEEDS-CHANGES\n"
        "**Head SHA reviewed:** d60285d5\n"), complete=True)
    time.sleep(0.01)
    rid2 = _review(client, lane="repo!6-rev2", result=(
        "# Task result: RE-REVIEW (v2)\n## Verdict: PASS\n"
        "**Head SHA reviewed:** `d60285d5`\n"))
    lid = _land(client, rid2, lane="repo#land-6",
                title="LAND repo!6 on its RV-1 PASS at head d60285d5")
    assert _task(client, lid)["status"] == TaskStatus.ready.value, (
        "the rev2 PASS must govern — the latest completed reviewer round, "
        "whatever its lane twin is")


# ---------------------------------------------------------------------------
# 2. author != lander (and reviewer != lander) on the same artifact.
# ---------------------------------------------------------------------------

def test_landing_on_the_reviewer_lane_is_refused(client):
    """A landing submitted on the artifact's REVIEWER lane (role author but
    lane repo!2 — the reviewer's own lane) is a reviewer landing its own
    verdict: 409 at the boundary."""
    rid = _review(client, lane="repo!2", result=PASS_RESULT)
    r = _submit(client, "LAND repo!2 MUST equal deadbeef1234", role="author",
                lane_key="repo!2", requires=["github-write"],
                depends_on=[rid])
    assert r.status_code == 409, (
        "main must refuse a landing task submitted on the reviewer lane of "
        "the same artifact (#913)")


def test_landing_on_the_authoring_lane_is_refused(client):
    """The lane that carries the artifact in `issues` (its authoring sitting)
    may not be its lander lane: RV-1 mechanically, not by convention.
    Authorship membership is written by the intake bundle path (store-
    level), so the fixture seeds it the same way _create_bundle_task does."""
    rid = _review(client, lane="repo!2", result=PASS_RESULT)
    st = tasks_mod._state
    st.create_task("task_author1", "bundle delivering repo!2", ["tooling"],
                   lane_key="repo#env/dev", issues=["repo!2"])
    r = _submit(client, "LAND repo!2 MUST equal deadbeef1234", role="author",
                lane_key="repo#env/dev", requires=["github-write"],
                depends_on=[rid])
    assert r.status_code == 409, (
        "the authoring lane of an artifact cannot be its landing lane (#913)")


# ---------------------------------------------------------------------------
# controls: the gate must not widen
# ---------------------------------------------------------------------------

def test_no_recorded_verdict_holds_the_landing(client):
    """A landing whose artifact the board has never seen reviewed is HELD:
    absence of a verdict is absence of a PASS (the morshdy-flutter#land-2
    specimen — the lander was born 76 seconds BEFORE the PASS it cited
    existed). Dependency completion — or the absence of any dependency —
    must never be the authority."""
    other = _submit(client, "prep", requires=["tooling"])
    oid = other.json()["id"]
    client.post(f"/api/v1/tasks/{oid}/complete", json={"result": "ok"})
    r = _submit(client, "LAND repo!9 MUST equal deadbeef1234",
                role="author", lane_key="repo#land-9",
                requires=["github-write"], depends_on=[oid])
    assert r.status_code in (200, 201)
    assert _task(client, r.json()["id"])["status"] == TaskStatus.pending.value, (
        "no recorded PASS at the named sha -> held (fail-closed on absence)")


def test_reviewer_number_match_is_bounded(client):
    """A PASS on repo!22 must not satisfy a landing for repo!2 — the number
    match is bounded (the #872 bounded-comparison discipline). Without the
    bound, one lucky PASS unlocks every sibling landing."""
    _review(client, lane="repo!22", result=(
        "# Independent review verdict\n**Verdict**: PASS\n"
        "**Head sha**: deadbeef1234\n"))
    other = _submit(client, "prep", requires=["tooling"])
    oid = other.json()["id"]
    client.post(f"/api/v1/tasks/{oid}/complete", json={"result": "ok"})
    r = _submit(client, "LAND repo!2 MUST equal deadbeef1234",
                role="author", lane_key="repo#land-2",
                requires=["github-write"], depends_on=[oid])
    assert _task(client, r.json()["id"])["status"] == TaskStatus.pending.value, (
        "a verdict on !22 does not gate the landing for !2")


def test_non_landing_tasks_never_gated(client):
    """Control: an author delivery on a branch-shaped lane with no land-key
    and no 'LAND' title is untouched by the verdict gate — including the
    follow-up author task a NEEDS-CHANGES hand-back spawns."""
    rid = _review(client, lane="repo!2", result=REJECT_RESULT)
    r = _submit(client, "fix the findings on repo!2", role="author",
                lane_key="repo#env/dev", requires=["tooling"],
                depends_on=[rid])
    assert r.status_code in (200, 201), r.text
    # reviewer is already terminal so deps are met -> promoted ready at once
    assert _task(client, r.json()["id"])["status"] == TaskStatus.ready.value
