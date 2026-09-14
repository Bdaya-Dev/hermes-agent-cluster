"""#858 defect 1, part 3: `error: None` at the API boundary, fixed.

The operator symptom: a busy-lane task landed in `failed` with
`error: None` and a zero-byte result — a failure with NO reason attached.
Two structural causes are pinned here:

  A. `POST /tasks/{id}/fail` accepted `req=None` and silently recorded
     `reason="failed"` — an executor report that lost its body (or a
     hand-call without one) produced an unexplained failure. It must now
     carry a reason: no body / blank / missing `reason` field => 422.

  B. Every executor terminal path must report a non-blank reason that
     carries the child's actual stderr (the hermes refusal text, the
     crash output) — never None, never empty.

RED on main (68f7f6e): A — `fail_task(task_id, req=None)` returns 200
with `reason='failed'`; B — `_report_failure(task_id, "")` sends a blank
reason without complaint.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core.agent_executor import (
    AgentExecutor,
    AgentExecutorConfig,
)
from hermes_cluster.models import TaskStatus


@pytest.fixture()
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def _submit(client, title="lane task"):
    resp = client.post("/api/v1/tasks", json={"title": title})
    assert resp.status_code in (200, 201)
    return resp.json()["id"]


def _task(client, task_id):
    for t in client.get("/api/v1/tasks").json():
        if t["id"] == task_id:
            return t
    raise AssertionError(f"task {task_id} not listed")


# ---------------------------------------------------------------------------
# A. /fail must carry a reason — the API boundary that let `error: None`
#    failures through with no explanation.
# ---------------------------------------------------------------------------

def test_fail_without_body_is_rejected(client):
    """`req=None` used to succeed as reason='failed' — a failed task whose
    error field carries nothing an operator can act on. The endpoint now
    requires the reason in the body."""
    task_id = _submit(client)
    resp = client.post(f"/api/v1/tasks/{task_id}/fail")  # NO body
    assert resp.status_code == 422, (
        f"a reasonless /fail must be rejected, got {resp.status_code} "
        "(main accepted it as reason='failed')")
    # and the task must not be failed at all:
    assert _task(client, task_id)["status"] != TaskStatus.failed.value


def test_fail_with_blank_reason_is_rejected(client):
    task_id = _submit(client)
    resp = client.post(f"/api/v1/tasks/{task_id}/fail",
                       json={"reason": "   "})
    assert resp.status_code == 422
    assert _task(client, task_id)["status"] != TaskStatus.failed.value


def test_fail_with_real_reason_still_works(client):
    task_id = _submit(client)
    resp = client.post(f"/api/v1/tasks/{task_id}/fail",
                       json={"reason": "hermes exited rc=1: boom"})
    assert resp.status_code == 200
    body = _task(client, task_id)
    assert body["status"] == TaskStatus.failed.value
    assert body["fail_reason"] == "hermes exited rc=1: boom"


# ---------------------------------------------------------------------------
# B. The executor's own report path refuses a blank reason (defense in
#    depth: even a caller bug inside agent_executor.py cannot produce a
#    reasonless failure report on the wire).
# ---------------------------------------------------------------------------

def _executor(**overrides):
    cfg = AgentExecutorConfig(enabled=True, poll_interval=60, **overrides)
    return AgentExecutor(
        config=cfg, node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999", peer_token="tok",
    )


def test_report_failure_never_sends_blank_reason():
    executor = _executor()
    seen = []

    def spy(endpoint, method, path, data, token, node_id, **kw):
        seen.append((method, path, data))
        return {"status": "failed"}

    with patch("hermes_cluster.core.agent_executor._signed_request",
               side_effect=spy):
        ok = executor._report_failure("t_x", "")   # blank reason, no fallback
    assert ok is False, "a blank failure reason must not reach the wire"
    assert seen == [], f"nothing should be POSTed, saw {seen}"

    # With a fallback, the fallback text goes on the wire instead:
    with patch("hermes_cluster.core.agent_executor._signed_request",
               side_effect=spy):
        ok = executor._report_failure("t_x", "", fallback="lane died")
    assert ok is True
    assert seen and seen[-1][2]["reason"] == "lane died"


def test_report_failure_carries_stderr_detail():
    """The incident class: the reason must carry hermes' refusal text, not
    just 'exit rc=1'."""
    executor = _executor()
    seen = []

    def spy(endpoint, method, path, data, token, node_id, **kw):
        seen.append((method, path, data))
        return {}

    with patch("hermes_cluster.core.agent_executor._signed_request",
               side_effect=spy):
        executor._report_failure(
            "t_y", "hermes exited rc=1: SESSION_NOT_OWNED already has a "
                   "live owner (cli, pid 72390)")
    reason = seen[-1][2]["reason"]
    assert "SESSION_NOT_OWNED" in reason and "pid 72390" in reason
