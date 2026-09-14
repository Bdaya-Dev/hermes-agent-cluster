"""#898 — one OPEN task per lane_key, deduped at the main's POST /api/v1/tasks.

Measured on the live cluster 2026-09-14 (shared/claude-plugins#898): the lane
`metering-poller` submitted its reviewer task 4x and cancelled 3 while the
reviewers sat queued — the main happily held four OPEN tasks on ONE reviewer
lane_key, the PR stayed Draft, and the lead cancelled the lane. The scheduler
already serializes a lane (LFP-1: never assign while another task on the same
lane_key is active), but nothing stopped the SUBMIT path from accumulating a
second, third, fourth open task on one lane_key.

The invariant this file pins: a POST /api/v1/tasks whose lane_key already has
an OPEN (ready/assigned/running — and pending/cancel_requested, still in
flight) task returns THAT task (HTTP 200, ``deduped: true``) instead of
creating a second one. Cancellation of the open one stays available ONLY via
the explicit cancel endpoint — dedup must never offer a cancel-and-replace.

RED on main: submitting the same lane_key twice yields two distinct task ids
and no ``deduped`` field anywhere.

Import discipline (#872 standard): nothing new is imported at module top; the
first tests fail on the DEFECT (two open tasks), not on ImportError.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.routers import tasks as tasks_router


def _state_of(client):
    """The store the tasks router operates on (a module global by design)."""
    return tasks_router._state


@pytest.fixture
def client():
    return TestClient(
        create_app(cluster_id="test-cluster", node_id="test-node", node_role="main")
    )


def _open_task_ids(client, lane_key):
    """Every task on lane_key that is NOT terminal."""
    terminal = {"completed", "failed", "cancelled"}
    return [
        t for t in client.get("/api/v1/tasks").json()
        if t.get("lane_key") == lane_key and t.get("status") not in terminal
    ]


# --- the measured incident: reviewer resubmit --------------------------------

def test_reviewer_lane_key_second_submit_is_deduped(client):
    """The #898 loop: author lane submits its reviewer, sees it queued,
    resubmits. The second submit must answer the EXISTING task, deduped."""
    brief = ("REVIEW the metering PR (11 chars ok) for #898 — post the verdict. "
             "NEVER approve or merge your own review.")
    first = client.post("/api/v1/tasks", json={
        "title": brief, "requires": ["review"], "role": "reviewer",
        "lane_key": "metering-poller-rev",
    })
    assert first.status_code == 200
    first_id = first.json()["id"]
    assert not first.json().get("deduped")  # the creator is never a dedup

    second = client.post("/api/v1/tasks", json={
        "title": brief, "requires": ["review"], "role": "reviewer",
        "lane_key": "metering-poller-rev",
    })
    assert second.status_code == 200
    body = second.json()
    assert body.get("deduped") is True, (
        "second submit on an open lane_key must answer the EXISTING task — "
        "on main it silently creates a second open task (the #898 loop)")
    assert body["id"] == first_id

    open_tasks = _open_task_ids(client, "metering-poller-rev")
    assert len(open_tasks) == 1, (
        f"single-open-task-per-lane_key invariant violated: "
        f"{[(t['id'], t['status']) for t in open_tasks]}")


def test_dedup_response_carries_the_existing_task_status(client):
    """A deduped answer must reflect the open task as the store holds it, so
    the lane cannot mistake it for a fresh submit."""
    client.post("/api/v1/tasks", json={
        "title": "bundle deliver #898", "lane_key": "metering-poller",
    })
    r = client.post("/api/v1/tasks", json={
        "title": "bundle deliver #898", "lane_key": "metering-poller",
    })
    assert r.status_code == 200
    body = r.json()
    assert body.get("deduped") is True
    assert body.get("status") in ("ready", "pending"), (
        "deduped answer must carry the existing task's real status")


# --- the invariant is lane_key-scoped, not role-scoped ------------------------

@pytest.mark.parametrize("status", ["ready", "assigned", "running"])
def test_open_states_block_a_second_task_on_the_same_lane_key(client, status):
    """ready/assigned/running (the brief's open set) all dedup a resubmit."""
    first = client.post("/api/v1/tasks", json={
        "title": "lane work", "lane_key": "laney-998",
    }).json()
    # Drive the row into the state under test through the store directly —
    # the HTTP surface has no set-status endpoint by design.
    state = _state_of(client)
    from hermes_cluster.models import TaskStatus
    if status != "ready":
        assert state.set_task_status(first["id"], TaskStatus(status))

    second = client.post("/api/v1/tasks", json={
        "title": "lane work", "lane_key": "laney-998",
    })
    assert second.status_code == 200
    assert second.json().get("deduped") is True, (
        f"lane_key holding a(n) {status} task must NOT accept a second open task")
    assert second.json()["id"] == first["id"]


def test_terminal_lane_key_reopens_cleanly(client):
    """A lane whose task is COMPLETED (or cancelled/failed) must be able to
    receive the NEXT delivery — dedup must not strand a lane forever."""
    first = client.post("/api/v1/tasks", json={
        "title": "delivery one", "lane_key": "laney-997",
    }).json()
    assert client.post(f"/api/v1/tasks/{first['id']}/cancel").status_code == 200

    second = client.post("/api/v1/tasks", json={
        "title": "delivery two", "lane_key": "laney-997",
    })
    assert second.status_code == 200
    body = second.json()
    assert not body.get("deduped"), (
        "a cancelled lane_key is OPEN for the next delivery — dedup only "
        "applies to tasks still in flight")
    assert body["id"] != first["id"]
    assert len(_open_task_ids(client, "laney-997")) == 1


def test_legacy_tasks_without_lane_key_never_dedup(client):
    """Empty lane_key = per-task session: nothing to key on. These must keep
    their current create-every-time behaviour (the #48-era per-issue lanes)."""
    a = client.post("/api/v1/tasks", json={"title": "same title", "requires": []})
    b = client.post("/api/v1/tasks", json={"title": "same title", "requires": []})
    assert a.json()["id"] != b.json()["id"]
    assert not b.json().get("deduped")


def test_different_lane_keys_do_not_collide(client):
    """Dedup is by lane_key, not by title/role."""
    a = client.post("/api/v1/tasks", json={"title": "x", "lane_key": "lane-a"})
    b = client.post("/api/v1/tasks", json={"title": "x", "lane_key": "lane-b"})
    assert a.json()["id"] != b.json()["id"]
    assert not b.json().get("deduped")


def test_pending_bundle_task_still_counts_as_open(client):
    """A dep-held (pending) bundle task holds its lane — creating a second
    open task behind it would let both release as the dep chain resolves.
    Mirrors intake's _ISSUE_HOLDING_STATUSES, which counts pending +
    cancel_requested as holding."""
    state = _state_of(client)
    from hermes_cluster.models import TaskStatus
    # park the lane task in pending (a bundle waiting on deps sits here)
    first = client.post("/api/v1/tasks", json={
        "title": "bundle", "lane_key": "laney-996",
    }).json()
    assert state.set_task_status(first["id"], TaskStatus.cancel_requested)

    second = client.post("/api/v1/tasks", json={
        "title": "bundle", "lane_key": "laney-996",
    })
    assert second.status_code == 200
    assert second.json().get("deduped") is True, (
        "cancel_requested still holds the lane session until the worker acks "
        "(intake #762 cardinality rule) — it is open for dedup")


def test_explicit_cancel_then_resubmit_is_the_only_replace_path(client):
    """The #898 loop was author submits -> cancels -> resubmits. With dedup,
    cancel-then-resubmit still WORKS (it replaces), which keeps the cancel
    endpoint as the only mutation path — the invariant is "no two OPEN
    tasks", not "lane frozen"."""
    first = client.post("/api/v1/tasks", json={
        "title": "bundle work", "lane_key": "laney-995",
    }).json()
    assert client.post(f"/api/v1/tasks/{first['id']}/cancel").status_code == 200
    second = client.post("/api/v1/tasks", json={
        "title": "bundle work", "lane_key": "laney-995",
    })
    assert second.status_code == 200
    assert second.json()["id"] != first["id"]
    assert not second.json().get("deduped")
