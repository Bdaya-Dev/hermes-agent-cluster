"""Tests for two-phase task cancellation.

Covers:
- Cancel unclaimed (pending/ready) → cancelled immediately
- Cancel running → cancel_requested + lease revoked
- Worker ack (/complete) on cancel_requested → cancelled
- Cancel terminal → 409
- Scheduler skips cancel_requested
- Sync round-trips the new states
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import TaskStatus, SyncEventType, EventType, SyncMessage, TaskSync
from hermes_cluster.state import ClusterState


@pytest.fixture
def app():
    """Create a test app with fresh state."""
    return create_app(
        cluster_id="test-cluster",
        node_id="test-node",
        node_role="main",
    )


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


def _create_task(client, title="test-task"):
    """Helper: create a task and return its ID."""
    resp = client.post("/api/v1/tasks", json={"title": title})
    assert resp.status_code == 200
    return resp.json()["id"]


def _join_node(client, name="worker-1"):
    """Helper: join a node and return its ID."""
    resp = client.post("/api/v1/nodes/join", json={
        "node_name": name,
        "capabilities": ["coding"],
    })
    assert resp.status_code == 200
    return resp.json()["node_id"]


# ---------------------------------------------------------------------------
# 1. Cancel unclaimed → cancelled immediately
# ---------------------------------------------------------------------------

class TestCancelUnclaimed:
    def test_cancel_pending(self, client):
        """Cancelling a pending task transitions directly to cancelled."""
        task_id = _create_task(client)
        resp = client.post(f"/api/v1/tasks/{task_id}/cancel", json={"reason": "no longer needed"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cancelled"
        assert data["phase"] == "immediate"

        # Verify task state
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"

    def test_cancel_ready(self, client):
        """Cancelling a ready task transitions directly to cancelled."""
        task_id = _create_task(client)
        # Task should be ready after creation (no deps → promoted)
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] in ("pending", "ready")

        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"


# ---------------------------------------------------------------------------
# 2. Cancel running → cancel_requested + lease revoked
# ---------------------------------------------------------------------------

class TestCancelRunning:
    def test_cancel_running_revokes_lease(self, client):
        """Cancelling a running task revokes the lease and sets cancel_requested."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim the task (pending → running)
        resp = client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"

        # Verify lease exists
        leases = client.get("/api/v1/leases").json()
        active_leases = [l for l in leases if l["task_id"] == task_id and l["status"] == "active"]
        assert len(active_leases) == 1

        # Cancel
        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cancel_requested"
        assert data["phase"] == "pending_ack"

        # Verify lease was revoked
        leases = client.get("/api/v1/leases").json()
        active_leases = [l for l in leases if l["task_id"] == task_id and l["status"] == "active"]
        assert len(active_leases) == 0

        # Verify task status
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancel_requested"


# ---------------------------------------------------------------------------
# 3. Worker ack → cancelled
# ---------------------------------------------------------------------------

class TestWorkerAck:
    def test_complete_on_cancel_requested(self, client):
        """Worker calling /complete on a cancel_requested task closes it to cancelled."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim and cancel
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        # Worker completes (ack)
        resp = client.post(f"/api/v1/tasks/{task_id}/complete")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"

        # Verify final state
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"

    def test_fail_on_cancel_requested(self, client):
        """Worker calling /fail on a cancel_requested task closes it to cancelled."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim and cancel
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        # Worker fails (ack)
        resp = client.post(f"/api/v1/tasks/{task_id}/fail", json={"reason": "couldn't finish"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"


# ---------------------------------------------------------------------------
# 4. Cancel terminal → 409
# ---------------------------------------------------------------------------

class TestCancelTerminal:
    def test_cancel_completed_409(self, client):
        """Cancelling a completed task returns 409."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Complete the task
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        client.post(f"/api/v1/tasks/{task_id}/complete")

        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 409
        assert "not cancelable" in resp.json()["detail"]

    def test_cancel_failed_409(self, client):
        """Cancelling a failed task returns 409."""
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/fail", json={"reason": "broken"})

        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 409

    def test_cancel_already_cancelled_409(self, client):
        """Cancelling an already cancelled task returns 409."""
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 409

    def test_cancel_not_found_404(self, client):
        """Cancelling a non-existent task returns 404."""
        resp = client.post("/api/v1/tasks/nonexistent/cancel")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Scheduler skips cancel_requested
# ---------------------------------------------------------------------------

class TestSchedulerSkip:
    def test_scheduler_skips_cancel_requested(self, client):
        """Scheduler must not assign a cancel_requested task."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Cancel the task
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        # Trigger scheduling
        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200

        # Verify task was NOT scheduled
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"
        assert task["assigned_to"] is None

    def test_scheduler_does_not_reassign_cancel_requested(self, client):
        """Scheduler must not re-assign a cancel_requested task (was running)."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim then cancel
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        # Trigger scheduling
        client.post("/api/v1/schedule/trigger")

        # Verify task status unchanged
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancel_requested"


# ---------------------------------------------------------------------------
# 6. Sync round-trips new states
# ---------------------------------------------------------------------------

class TestSyncRoundTrip:
    def test_sync_cancel_requested(self):
        """Sync LWW carries cancel_requested state."""
        state = ClusterState()
        # Create task locally
        state.create_task("t1", "test", [], 3)

        # Receive sync with cancel_requested
        msg = SyncMessage(
            version=1,
            sender_node="remote-node",
            event_type=SyncEventType.task_cancel_requested,
            timestamp=1000,
            task_state=TaskSync(
                task_id="t1",
                title="test",
                status="cancel_requested",
                version=2,
            ),
        )
        applied = state.handle_sync_message(msg)
        assert applied is True

        task = state.get_task("t1")
        assert task.status == TaskStatus.cancel_requested

    def test_sync_cancelled(self):
        """Sync LWW carries cancelled state."""
        state = ClusterState()
        state.create_task("t1", "test", [], 3)

        msg = SyncMessage(
            version=1,
            sender_node="remote-node",
            event_type=SyncEventType.task_cancelled,
            timestamp=1000,
            task_state=TaskSync(
                task_id="t1",
                title="test",
                status="cancelled",
                version=2,
            ),
        )
        applied = state.handle_sync_message(msg)
        assert applied is True

        task = state.get_task("t1")
        assert task.status == TaskStatus.cancelled

    def test_sync_version_ordering(self):
        """Sync rejects older versions (LWW)."""
        state = ClusterState()
        state.create_task("t1", "test", [], 3)

        # Apply version 5
        msg1 = SyncMessage(
            version=5,
            sender_node="remote",
            event_type=SyncEventType.task_cancel_requested,
            timestamp=1000,
            task_state=TaskSync(task_id="t1", title="test", status="cancel_requested", version=5),
        )
        assert state.handle_sync_message(msg1) is True

        # Reject version 3 (older)
        msg2 = SyncMessage(
            version=3,
            sender_node="remote",
            event_type=SyncEventType.task_cancelled,
            timestamp=500,
            task_state=TaskSync(task_id="t1", title="test", status="cancelled", version=3),
        )
        assert state.handle_sync_message(msg2) is False

        # Still cancel_requested
        task = state.get_task("t1")
        assert task.status == TaskStatus.cancel_requested


# ---------------------------------------------------------------------------
# 7. Enum values
# ---------------------------------------------------------------------------

class TestEnums:
    def test_task_status_has_cancel_states(self):
        """TaskStatus enum includes cancel_requested and cancelled."""
        assert TaskStatus.cancel_requested.value == "cancel_requested"
        assert TaskStatus.cancelled.value == "cancelled"

    def test_sync_event_type_has_cancel_events(self):
        """SyncEventType includes cancel events."""
        assert SyncEventType.task_cancel_requested.value == "task_cancel_requested"
        assert SyncEventType.task_cancelled.value == "task_cancelled"

    def test_event_type_has_cancel_events(self):
        """EventType includes cancel events."""
        assert EventType.task_cancel_requested.value == "task_cancel_requested"
        assert EventType.task_cancelled.value == "task_cancelled"


# ---------------------------------------------------------------------------
# 7. Regression tests for review fixes (B1, B2, S1-S4)
# ---------------------------------------------------------------------------

class TestReviewFixes:
    """Tests added to close round-1 review findings."""

    def test_b1_advance_rejects_cancelled(self, client):
        """B1: /advance must reject a cancelled task (no revival)."""
        _join_node(client)
        task_id = _create_task(client)
        # Cancel immediately (no lease)
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        # Try to advance
        resp = client.post(f"/api/v1/tasks/{task_id}/advance")
        assert resp.status_code == 409
        # Verify still cancelled
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"

    def test_b1_advance_rejects_cancel_requested(self, client):
        """B1: /advance must reject a cancel_requested task."""
        node_id = _join_node(client)
        task_id = _create_task(client)
        # Claim then cancel (has lease → cancel_requested)
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        # Try to advance
        resp = client.post(f"/api/v1/tasks/{task_id}/advance")
        assert resp.status_code == 409
        # Verify still cancel_requested
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancel_requested"

    def test_b2_complete_rejects_cancelled(self, client):
        """B2: /complete must reject an already-cancelled task (no overwrite)."""
        _join_node(client)
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        resp = client.post(f"/api/v1/tasks/{task_id}/complete")
        assert resp.status_code == 409
        # Verify still cancelled
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"

    def test_b2_fail_rejects_cancelled(self, client):
        """B2: /fail must reject an already-cancelled task (no overwrite)."""
        _join_node(client)
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        resp = client.post(f"/api/v1/tasks/{task_id}/fail", json={"reason": "test"})
        assert resp.status_code == 409
        # Verify still cancelled
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "cancelled"

    def test_s2_cancel_branches_on_lease_not_status(self, client):
        """S2: cancel must branch on lease existence, not status."""
        node_id = _join_node(client)
        task_id = _create_task(client)
        # Claim (creates lease)
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})
        # Cancel should go to cancel_requested (has lease)
        resp = client.post(f"/api/v1/tasks/{task_id}/cancel")
        data = resp.json()
        assert data["phase"] == "pending_ack"
        assert data["status"] == "cancel_requested"
        # Now cancel again (no lease after revoke) should 409 (already terminal)
        resp2 = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp2.status_code == 409

    def test_s3_cluster_status_includes_cancel_states(self, client):
        """S3: /api/v1/cluster/status must surface cancel_requested/cancelled counts."""
        _join_node(client)
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        resp = client.get("/api/v1/cluster/status")
        data = resp.json()
        assert "cancelled" in data["tasks_by_status"]
        assert data["tasks_by_status"]["cancelled"] >= 1

    def test_s3_status_endpoint_includes_cancel_states(self, client):
        """S3: /api/v1/status must surface cancel_requested/cancelled in summary."""
        _join_node(client)
        task_id = _create_task(client)
        client.post(f"/api/v1/tasks/{task_id}/cancel")
        resp = client.get("/api/v1/status")
        data = resp.json()
        assert "cancelled" in data["summary"]
        assert data["summary"]["cancelled"] >= 1

    def test_s4_fail_cascades_cancel_to_dependents(self, client):
        """S4: /fail must cascade-cancel pending/ready dependents."""
        _join_node(client)
        parent_id = _create_task(client)
        child_id = _create_task(client)
        # Set dependency
        client.post(f"/api/v1/tasks/{child_id}/dependencies", json={"depends_on": [parent_id]})
        # Fail parent
        client.post(f"/api/v1/tasks/{parent_id}/fail", json={"reason": "test"})
        # Child should be cancelled
        tasks = client.get("/api/v1/tasks").json()
        child = next(t for t in tasks if t["id"] == child_id)
        assert child["status"] == "cancelled"

    def test_mutation_terminality_guard(self, client):
        """Mutation: removing terminality guard in set_task_status must break the sync seam.

        S5 fix: this test now exercises the sync path (handle_sync_message → set_task_status),
        NOT the router-level guard in /advance. If the set_task_status terminality guard is
        removed, a sync message can revive a cancelled task — this test catches it.
        """
        state = ClusterState()
        state.create_task("t1", "test", [], 3)
        # Cancel the task (terminal)
        state.set_task_status("t1", TaskStatus.cancelled)
        assert state.get_task("t1").status == TaskStatus.cancelled

        # Try to sync it back to running via handle_sync_message (the N1 seam)
        msg = SyncMessage(
            version=1,
            sender_node="remote-lagging-node",
            event_type=SyncEventType.task_assigned,
            timestamp=1000,
            task_state=TaskSync(
                task_id="t1",
                title="test",
                status="running",
                assigned_to="node_remote",
                version=99,
            ),
        )
        state.handle_sync_message(msg)
        # Must STAY cancelled — the set_task_status guard blocks the revival
        task = state.get_task("t1")
        assert task.status == TaskStatus.cancelled, \
            "terminality guard in set_task_status must reject sync-driven revival"


# ---------------------------------------------------------------------------
# 8. Round-2 regression tests (N1, N2, S4)
# ---------------------------------------------------------------------------

class TestRound2Fixes:
    """Regression tests for round-2 review findings."""

    # N1: sync must not revive a terminal task
    def test_n1_sync_cannot_revive_cancelled_task(self):
        """N1: handle_sync_message must not overwrite a cancelled task's status."""
        state = ClusterState()
        state.create_task("t1", "doomed", [], 3)
        # Cancel the task locally (terminal)
        state.set_task_status("t1", TaskStatus.cancelled)
        assert state.get_task("t1").status == TaskStatus.cancelled

        # Remote sends a sync with status=running, high version
        msg = SyncMessage(
            version=500,
            sender_node="remote-lagging-node",
            event_type=SyncEventType.task_assigned,
            timestamp=1000,
            task_state=TaskSync(
                task_id="t1",
                title="doomed",
                status="running",
                assigned_to="node_remote",
                version=99,
            ),
        )
        applied = state.handle_sync_message(msg)
        assert applied is True  # global counter advanced

        # Task must STILL be cancelled — terminality guard held
        task = state.get_task("t1")
        assert task.status == TaskStatus.cancelled

    def test_n1_sync_cannot_revive_completed_task(self):
        """N1: sync cannot revive a completed task either."""
        state = ClusterState()
        state.create_task("t1", "done", [], 3)
        state.set_task_status("t1", TaskStatus.completed)

        msg = SyncMessage(
            version=10,
            sender_node="remote",
            event_type=SyncEventType.task_assigned,
            timestamp=1000,
            task_state=TaskSync(task_id="t1", title="done", status="ready", version=99),
        )
        state.handle_sync_message(msg)
        assert state.get_task("t1").status == TaskStatus.completed

    def test_n1_sync_stale_per_task_version_rejected(self):
        """N1: sync with stale per-task version must not overwrite, even with fresh global version."""
        state = ClusterState()
        state.create_task("t1", "test", [], 3)
        # Bump local task version to 5
        for _ in range(4):
            state.set_task_status("t1", TaskStatus.cancel_requested)
            # cancel_requested → cancel_requested is rejected (terminal), so bump via other means
        # Actually let's just create at version 1 and sync to version 3, then try version 2
        state2 = ClusterState()
        state2.create_task("t1", "test", [], 3)
        # Sync to version 3 (running)
        msg1 = SyncMessage(
            version=1, sender_node="r", event_type=SyncEventType.task_assigned,
            timestamp=1, task_state=TaskSync(task_id="t1", title="test", status="running", version=3),
        )
        state2.handle_sync_message(msg1)
        assert state2.get_task("t1").version == 3

        # Now try version 2 (stale) with a higher global version
        msg2 = SyncMessage(
            version=2, sender_node="r", event_type=SyncEventType.task_cancelled,
            timestamp=2, task_state=TaskSync(task_id="t1", title="test", status="cancelled", version=2),
        )
        state2.handle_sync_message(msg2)
        # Task should still be running (version 3 > 2, so stale sync rejected)
        assert state2.get_task("t1").status == TaskStatus.running

    # N2: recovery must not report a terminal task as rescheduled
    def test_n2_recovery_does_not_false_reschedule_failed_task(self, client):
        """N2: a failed task with an active lease must NOT be reported as rescheduled."""
        from hermes_cluster.recovery.revoker import Revoker
        from hermes_cluster.recovery.rescheduler import Rescheduler

        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim the task (creates lease)
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})

        # Fail the task — N2 fix: this now revokes the lease
        resp = client.post(f"/api/v1/tasks/{task_id}/fail", json={"reason": "broken"})
        assert resp.status_code == 200

        # Verify lease was revoked by /fail
        leases = client.get("/api/v1/leases").json()
        active_for_task = [l for l in leases if l["task_id"] == task_id and l["status"] == "active"]
        assert len(active_for_task) == 0, "/fail must revoke the lease"

        # Verify task is failed
        tasks = client.get("/api/v1/tasks").json()
        task = next(t for t in tasks if t["id"] == task_id)
        assert task["status"] == "failed"

    def test_n2_recovery_rescheduler_skips_terminal_tasks(self):
        """N2: Rescheduler.reschedule_orphaned must skip terminal tasks (unassign_task returns False)."""
        from hermes_cluster.state import ClusterState
        from hermes_cluster.recovery.rescheduler import Rescheduler
        from hermes_cluster.models import Node, NodeStatus

        state = ClusterState()
        # Register a node
        node = Node(id="node_w1", name="w1", capabilities=["coding"], status=NodeStatus.online)
        state.register_node(node)

        # Create and fail a task
        state.create_task("t1", "test", [], 3)
        state.set_task_status("t1", TaskStatus.running)
        with state._tasks_lock:
            state._tasks["t1"].assigned_to = "node_w1"
        state.set_task_status("t1", TaskStatus.failed, fail_reason="broken")

        rescheduler = Rescheduler(state)
        # Try to reschedule the failed task
        count = rescheduler.reschedule_orphaned(["t1"])
        assert count == 0, "failed task must not be rescheduled"

        # Verify task is still failed, not ready
        task = state.get_task("t1")
        assert task.status == TaskStatus.failed

        # Verify no spurious recovery events
        events = state.get_recovery_events()
        reschedule_events = [e for e in events if e.action == "reschedule"]
        assert len(reschedule_events) == 0, "no reschedule event for a terminal task"

    # S4: transitive cascade
    def test_s4_cancel_cascades_to_dependents(self, client):
        """S4: /cancel must cascade-cancel pending/ready dependents."""
        _join_node(client)
        parent_id = _create_task(client)
        child_id = _create_task(client)
        # Set dependency
        client.post(f"/api/v1/tasks/{child_id}/dependencies", json={"depends_on": [parent_id]})

        # Cancel parent
        resp = client.post(f"/api/v1/tasks/{parent_id}/cancel")
        assert resp.status_code == 200

        # Child must be cancelled
        tasks = client.get("/api/v1/tasks").json()
        child = next(t for t in tasks if t["id"] == child_id)
        assert child["status"] == "cancelled"

    def test_s4_fail_cascades_transitively_depth2(self, client):
        """S4: A→B→C, /fail A must cascade to both B and C."""
        _join_node(client)
        a_id = _create_task(client, title="A")
        b_id = _create_task(client, title="B")
        c_id = _create_task(client, title="C")
        # A → B → C
        client.post(f"/api/v1/tasks/{b_id}/dependencies", json={"depends_on": [a_id]})
        client.post(f"/api/v1/tasks/{c_id}/dependencies", json={"depends_on": [b_id]})

        # Fail A
        client.post(f"/api/v1/tasks/{a_id}/fail", json={"reason": "broken"})

        # Both B and C must be cancelled
        tasks = client.get("/api/v1/tasks").json()
        b = next(t for t in tasks if t["id"] == b_id)
        c = next(t for t in tasks if t["id"] == c_id)
        assert b["status"] == "cancelled", f"B should be cancelled, got {b['status']}"
        assert c["status"] == "cancelled", f"C should be cancelled, got {c['status']}"

    def test_s4_cascaded_dependent_not_schedulable(self, client):
        """S4: a cascaded dependent must NOT be picked up by /schedule/trigger."""
        _join_node(client)
        parent_id = _create_task(client)
        child_id = _create_task(client)
        client.post(f"/api/v1/tasks/{child_id}/dependencies", json={"depends_on": [parent_id]})

        # Cancel parent → child is cascade-cancelled
        client.post(f"/api/v1/tasks/{parent_id}/cancel")

        # Trigger scheduler — must NOT assign the cancelled child
        resp = client.post("/api/v1/schedule/trigger")
        data = resp.json()
        assigned_ids = [a["task_id"] for a in data.get("assignments", [])]
        assert child_id not in assigned_ids, "cascaded dependent must not be scheduled"

        # Verify child is still cancelled
        tasks = client.get("/api/v1/tasks").json()
        child = next(t for t in tasks if t["id"] == child_id)
        assert child["status"] == "cancelled"

    def test_s4_fail_cascades_to_running_dependent(self, client):
        """S4: /fail must cascade to running dependents too (cancel_requested via lease revoke)."""
        node_id = _join_node(client)
        parent_id = _create_task(client)
        child_id = _create_task(client)
        client.post(f"/api/v1/tasks/{child_id}/dependencies", json={"depends_on": [parent_id]})

        # Claim the child (makes it running with a lease)
        resp = client.post(f"/api/v1/tasks/{child_id}/claim", json={"node_id": node_id})
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"

        # Fail parent — child should be cascade to cancel_requested (has lease)
        client.post(f"/api/v1/tasks/{parent_id}/fail", json={"reason": "broken"})

        tasks = client.get("/api/v1/tasks").json()
        child = next(t for t in tasks if t["id"] == child_id)
        assert child["status"] == "cancel_requested", \
            f"running dependent with lease should be cancel_requested, got {child['status']}"


# ---------------------------------------------------------------------------
# 9. Round-3 regression test (R3-1)
# ---------------------------------------------------------------------------

class TestRound3Fixes:
    """Regression test for round-3 review finding R3-1."""

    def test_r3_1_unleased_running_dependent_cancelled_immediately(self, client):
        """R3-1: cascade must branch on lease existence, not status.

        A running task with NO live lease — the #804 revive shape, or any
        row whose lease was revoked out from under it — when its parent
        fails/cancels, must be cancelled immediately, NOT set to
        cancel_requested (which would be an un-ackable zombie).

        #916 update: the scheduler leases on assign NOW, so "running with
        no lease" is no longer the natural post-schedule state; the live
        lease is explicitly revoked to construct the shape this branch
        guards (the same shape /cluster/status reports as
        unleased_running)."""
        node_id = _join_node(client)
        parent_id = _create_task(client)
        child_id = _create_task(client)
        client.post(f"/api/v1/tasks/{child_id}/dependencies", json={"depends_on": [parent_id]})

        # Scheduler assigns child to running (and leases it, #916).
        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200

        # Verify child is running (scheduler-assigned)
        tasks = client.get("/api/v1/tasks").json()
        child = next(t for t in tasks if t["id"] == child_id)
        assert child["status"] == "running", \
            f"child should be running (scheduler-assigned), got {child['status']}"

        # Construct the guarded shape: strip the child's live lease so it
        # is running-without-ownership (the legacy / revive / revoked state).
        for l in client.get("/api/v1/leases").json():
            if l["task_id"] == child_id and l["status"] == "active":
                assert client.delete(f"/api/v1/leases/{l['id']}").status_code == 200
        leases = client.get("/api/v1/leases").json()
        active_leases_for_child = [l for l in leases if l["task_id"] == child_id and l["status"] == "active"]
        assert len(active_leases_for_child) == 0, \
            "setup: child must be running with no live lease"

        # Fail parent — child should cascade to cancelled immediately (not cancel_requested)
        client.post(f"/api/v1/tasks/{parent_id}/fail", json={"reason": "broken"})

        tasks = client.get("/api/v1/tasks").json()
        child_after = next(t for t in tasks if t["id"] == child_id)
        assert child_after["status"] == "cancelled", \
            f"unleased running dependent must be cancelled immediately, got {child_after['status']}"

    def test_s7_residual_release_returns_409_on_terminal_task(self, client):
        """S7 residual: /release must return 409 when task is terminal, not 200."""
        node_id = _join_node(client)
        task_id = _create_task(client)

        # Claim the task (creates lease, sets to running)
        client.post(f"/api/v1/tasks/{task_id}/claim", json={"node_id": node_id})

        # Cancel the task (revokes lease, sets to cancel_requested)
        client.post(f"/api/v1/tasks/{task_id}/cancel")

        # Try to release — should 409 (task is cancel_requested, terminal)
        resp = client.post(f"/api/v1/tasks/{task_id}/release", json={"node_id": node_id})
        assert resp.status_code == 409, \
            f"/release on terminal task must return 409, got {resp.status_code}"
        assert "terminal" in resp.json()["detail"].lower()
