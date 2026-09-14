"""Priority band semantics on submit (shared/claude-plugins #866).

The request model uses ``priority: int = 0`` as its not-supplied sentinel,
while the scheduler sorts ``ORDER BY priority`` ASCENDING — so 0 is also a
perfectly meaningful "more urgent than 1". A caller sending 0 meaning
*most urgent* was silently rewritten to 3 (two bands in the wrong
direction); the fix makes the request field ``Optional[int] = None`` and
defaults only on ``None``, freeing 0 for the ordering to honour.

Acceptance criteria covered here:
  1. Submit ``priority: 0`` -> the STORED value is 0 (RED against the old
     ``req.priority if req.priority > 0 else 3`` line, which stored 3).
  2. Submit nothing -> the documented default (3).
  3. Ordering end-to-end: a top-band task schedules ahead of a second-band
     one when only one node slot exists.
  4. Out-of-band values are rejected loudly (422), not silently coerced.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app


@pytest.fixture
def client():
    return TestClient(create_app(cluster_id="test", node_id="test", node_role="main"))


def _submit(client, title, **extra):
    resp = client.post("/api/v1/tasks", json={"title": title, **extra})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _read_back(client, task_id):
    resp = client.get("/api/v1/tasks")
    assert resp.status_code == 200
    for t in resp.json():
        if t["id"] == task_id:
            return t
    raise AssertionError(f"task {task_id} missing from list")


# ---------------------------------------------------------------------------
# 1. The stored value — proof of the footgun (#866 measured incident)
# ---------------------------------------------------------------------------

def test_submit_priority_zero_stores_zero(client):
    """A caller sending 0 meaning 'most urgent' keeps 0 end to end.

    RED against the pre-fix line ``priority = req.priority if req.priority
    > 0 else 3``: the response carried 3, silently demoting the task two
    bands below the band the sort says 0 means.
    """
    task = _submit(client, "critical-path-review", priority=0)
    assert task["priority"] == 0, (
        "priority 0 was rewritten — the exact #866 footgun (sent 0, stored 3)"
    )
    assert _read_back(client, task["id"])["priority"] == 0


def test_submit_priority_one_stores_one(client):
    task = _submit(client, "band-1", priority=1)
    assert task["priority"] == 1


# ---------------------------------------------------------------------------
# 2. Omitted priority still yields the documented default
# ---------------------------------------------------------------------------

def test_omitted_priority_defaults_to_three(client):
    task = _submit(client, "no-priority-said")
    assert task["priority"] == 3
    assert _read_back(client, task["id"])["priority"] == 3


def test_explicit_null_defaults_to_three(client):
    """Explicit ``null`` is the other spelling of 'not supplied'."""
    task = _submit(client, "explicit-null", priority=None)
    assert task["priority"] == 3


# ---------------------------------------------------------------------------
# 3. Ordering honours the band end-to-end: top schedules first
# ---------------------------------------------------------------------------

def test_zero_band_schedules_before_first_band(client):
    """Two ready tasks, exactly one free node slot (max_concurrent=1):
    the priority-0 task must take the slot; the priority-1 task waits.

    Under the old coercion the 0-task became 3 and the 1-task won — the
    measured incident (a 90-minute-starved critical review behind a less
    urgent one) in miniature.
    """
    urgent = _submit(client, "top-band", priority=0)
    less = _submit(client, "second-band", priority=1)

    join = client.post("/api/v1/nodes/join", json={
        "node_name": "only-slot",
        "capabilities": ["coding"],
        "max_concurrent": 1,
    })
    assert join.status_code == 200, join.text
    node_id = join.json()["node_id"]

    trig = client.post("/api/v1/schedule/trigger")
    assert trig.status_code == 200, trig.text

    assigned = {t["id"]: t for t in client.get("/api/v1/tasks").json()}
    assert assigned[urgent["id"]]["assigned_to"] == node_id, (
        "the priority-0 task must take the only slot (ascending sort: 0 < 1); "
        "if it lost, the band was coerced away"
    )
    assert assigned[less["id"]]["assigned_to"] is None
    assert assigned[less["id"]]["status"] == "ready"


# ---------------------------------------------------------------------------
# 4. Out-of-band values get a loud 422, not a silent substitution
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [4294967296, -1, 6])
def test_out_of_band_priority_rejected(client, bad):
    resp = client.post("/api/v1/tasks", json={"title": "bad-band", "priority": bad})
    assert resp.status_code == 422, (
        f"priority {bad} must be rejected loudly, not coerced (got {resp.status_code})"
    )
