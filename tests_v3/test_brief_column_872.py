"""#872 (deeper half): the brief gets its OWN column — `tasks.description`.

The merged guards (PR#30/#35) validate the brief *inside* `title` because the
schema had nowhere else to put it. That conflation is the root defect this
issue's "Deeper:" paragraph names: while `title` IS the brief, the only
dispatch shape stays "paste the brief into the column named title", and a
7.5 KB markdown blob rides in a field every other consumer reads as a one-line
goal (dashboard, `--goal`, dedup, `[#123] Issue title` intake tasks).

These tests pin the authoring-path fix:

- `description` exists end-to-end: request -> Task -> all three stores ->
  API read-back -> the executor's own `_write_brief` contract.
- The two #872 rules validate the BRIEF (description when present, legacy
  title otherwise) and still sweep the goal line for action-bound numbers --
  the goal rides to the lane as `--goal` too, so a foreign number there is the
  same wrong-job signal.
- A multi-line `title` on a lane-bearing task is LOUD (422 naming the fix),
  because that is exactly the wrong-column shape the incident produced.
- No `description` => title-as-brief, byte-identical behavior with the merged
  guards (legacy senders untouched).
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


def _submit(client, **kw):
    body = {"requires": []}
    body.update(kw)
    return client.post("/api/v1/tasks", json=body)


INCIDENT_SHAPE = (
    "=== WRITE YOUR VERDICT FIRST. ===\n"
    "Two previous reviewer lanes on this PR produced nothing.\n"
    "Review PR#274 as an independent verifier. Post with `gh`. NEVER approve."
)

GOAL_LINE = "REVIEW-ONLY lane for infra-github!274-rev-b: review PR#274 at head 85a9b38"


# --- the column exists -------------------------------------------------------

def test_submit_accepts_description_and_it_round_trips(client):
    """A brief in its own column; the title stays a one-line goal."""
    r = _submit(
        client, title=GOAL_LINE, description=INCIDENT_SHAPE,
        lane_key="infra-github!274-rev-b",
    )
    assert r.status_code == 200, r.text
    task_id = r.json()["id"]
    got = client.get(f"/api/v1/tasks/{task_id}").json()
    assert got["description"] == INCIDENT_SHAPE, (
        "the brief was not persisted on the task row"
    )
    assert got["title"] == GOAL_LINE


def test_description_defaults_empty_for_legacy_senders(client):
    """No description in the body => '' -- old callers keep working."""
    r = _submit(client, title="do the thing")
    assert r.status_code == 200, r.text
    assert r.json()["description"] == ""


def test_store_create_task_accepts_description():
    """A store handed 'description' does not TypeError (all three shapes).

    TypeError is converted to AssertionError per the red-proof standard: a
    tree without the fix must fail on the DEFECT (no brief column), not on an
    import/signature accident.
    """
    from hermes_cluster.state import ClusterState

    try:
        from hermes_cluster.state.cluster_store import ClusterStore
    except ImportError as e:
        raise AssertionError(f"sqlite store missing: {e}")

    tid = "task_col_1"
    try:
        ClusterState().create_task(
            tid, "goal", [], 3, lane_key="x!1-rev", role="author",
            description=INCIDENT_SHAPE,
        )
        st = ClusterStore(":memory:")
        t = st.create_task(
            tid, "goal", [], 3, lane_key="x!1-rev", role="author",
            description=INCIDENT_SHAPE,
        )
    except TypeError as e:
        raise AssertionError(f"store.create_task takes no 'description': {e}")
    assert t.description == INCIDENT_SHAPE, "SQLite store dropped the brief"


# --- the #872 rules move to the BRIEF ---------------------------------------

def test_guards_validate_description_when_present(client):
    """A brief in `description` that never names its lane target is rejected.

    Before this fix there was no column to look at; the conflation forced the
    brief into `title` and the guard into `title`-land.
    """
    r = _submit(
        client, title="review the assigned PR",
        description="Fix the widget factory. Post with gh. NEVER approve.",
        lane_key="infra-github!274-rev-c",
    )
    assert r.status_code == 422, (
        "the conflation this fix removes: a description brief naming no "
        "target sailed through because only title was checked"
    )
    assert "274" in r.json()["detail"]


def test_action_numbers_swept_in_description_brief(client):
    """Instance 2 shape carried in the NEW column is still a 422: the brief
    names its target 275 (presence passes) while binding the action to 273."""
    r = _submit(
        client, title="review PR#275 with a fresh eye",
        description="Review PR#275 as verifier. Post your verdict with "
                    "`gh pr comment 273` on the issue.",
        lane_key="infra-github!275-rev-c",
    )
    assert r.status_code == 422, "the action bound to a foreign number passed"
    assert "273" in r.json()["detail"]


def test_goal_line_is_swept_too(client):
    """`--goal` rides to the lane beside the brief: a foreign action number in
    a one-line title must 422 even when the description is compliant."""
    r = _submit(
        client, title="Review PR#275, then comment on 273",
        description=INCIDENT_SHAPE + "\nThis brief names 275: review PR#275 fully.",
        lane_key="infra-github!275-rev-c",
    )
    assert r.status_code == 422, (
        "the goal line reaches the lane as --goal; a wrong number there is "
        "the same wrong-job signal"
    )
    assert "273" in r.json()["detail"]


def test_absent_description_keeps_title_as_brief_byte_identical(client):
    """Legacy senders: no description => title-as-brief, same verdicts."""
    bad = _submit(client, title="Fix the widget factory. Post with gh.",
                  lane_key="infra-github!274-rev-b")
    assert bad.status_code == 422
    good = _submit(
        client,
        title="REVIEW-ONLY lane - review PR#274 at head 85a9b382f. Post with gh.",
        lane_key="infra-github!274-rev-b")
    assert good.status_code == 200, good.text


# --- the wrong-column shape is loud ------------------------------------------

def test_multiline_title_on_lane_task_is_rejected(client):
    """The incident's exact fingerprint: a 3-line markdown blob in `title` on
    a lane-bearing task. It means the brief was pasted into the goal column."""
    r = _submit(client, title=INCIDENT_SHAPE, lane_key="infra-github!274-rev-b")
    assert r.status_code == 422, (
        "multi-line title on a lane task is the wrong-column shape #872 was "
        "filed for; it must be named, not silently accepted"
    )
    detail = r.json()["detail"]
    assert "description" in detail.lower(), detail


def test_multiline_title_laneless_task_stays_legal(client):
    """Blast-radius control: no lane_key => no 422 (intake and per-task
    sessions keep their current shape)."""
    r = _submit(client, title=INCIDENT_SHAPE)
    assert r.status_code == 200, r.text


def test_branch_shaped_lane_multiline_title_still_rejected(client):
    """Branch lanes carry briefs too; the multi-line shape is checked on any
    non-empty lane_key, not only number-targeted ones."""
    r = _submit(client, title=INCIDENT_SHAPE,
                lane_key="claude-plugins#feat/869-seat-by-paste")
    assert r.status_code == 422


def test_single_line_title_with_description_ok(client):
    """The dispatch shape this fix creates: one-line goal + real brief."""
    r = _submit(client, title="AUTHOR LANE - wire MCP into the GKE brain",
                description="Repo Bdaya-Dev/bdaya-website-infra, branch "
                            "feat/mcp-872. Open ONE Draft PR. NEVER approve "
                            "or merge your own work.",
                lane_key="claude-plugins#feat/869-seat-by-paste")
    assert r.status_code == 200, r.text


# --- intake writes the issue BODY into description ---------------------------

def test_intake_task_carries_issue_body_in_description():
    """The GitLab intake path — the issue's own 'better shape' — moves the
    issue BODY into description while the title stays `[#iid] Issue title`."""
    from hermes_cluster.routers import intake
    from hermes_cluster.state import ClusterState

    body = "## Summary\n\nThe real brief lives here now.\n"
    st = ClusterState()
    try:
        t = intake._create_task_from_issue(
            dedup_key="999001", display_iid=999001, title="Some issue",
            description=body, state=st,
        )
    except TypeError as e:
        raise AssertionError(f"intake takes no 'description': {e}")
    task, is_new = t if isinstance(t, tuple) else (t, True)
    assert task.description == body, (
        "intake still has nowhere to put the issue body; the issue IS the "
        "brief and the task should point at it"
    )
    assert task.title == "[#999001] Some issue"
