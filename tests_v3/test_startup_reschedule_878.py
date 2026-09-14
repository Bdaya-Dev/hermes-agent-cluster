"""#878: a main restart must not silently strand the queue.

Scheduling is driven purely by task-lifecycle events -- created, capabilities
updated, completed -- each calling ``trigger_pending_tasks()`` exactly once. A
task still `ready` when the process dies has spent its only trigger, so the
next process loads it from the store and never asks the scheduler about it
again. Node re-registration does not rescue it: the trigger in
``routers/nodes.py`` lives in ``update_capabilities``, not in join/heartbeat.

Measured on the hosted main 2026-09-12: a ``requires: ["tooling"]`` task sat
`ready` for 3+ minutes after a pod restart with three workers online at
``load 0.0`` and zero rows in ``scheduling_decisions``; one manual
``POST /api/v1/schedule/trigger`` dispatched it in under 15s.

The failure is silent -- no error, no fail_reason, ``failed_schedules`` stays
0 -- and in Kubernetes a restart happens on any drain, image bump or eviction.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app


def _client(db_path, node_role="main"):
    return TestClient(
        create_app(
            cluster_id="test-cluster",
            node_id="test-node-" + node_role,
            node_role=node_role,
            db_path=str(db_path),
        )
    )


@pytest.fixture
def db(tmp_path):
    return tmp_path / "cluster.db"


def _register_worker(c, name="w1", caps=("tooling",)):
    return c.post(
        "/api/v1/nodes/join", json={"node_name": name, "capabilities": list(caps)}
    ).json()["node_id"]


def test_a_ready_task_is_rescheduled_when_the_main_restarts(db):
    """The regression: queue must not go inert across a restart."""
    # First process: a worker is available and a task is queued for it.
    with _client(db) as first:
        node_id = _register_worker(first)
        tid = first.post(
            "/api/v1/tasks", json={"title": "survives a restart", "requires": ["tooling"]}
        ).json()["id"]
        assert first.get(f"/api/v1/tasks/{tid}").json()["status"] in ("ready", "assigned")

    # Second process over the SAME store -- this is the restart. No manual
    # /schedule/trigger is issued anywhere in this test; that is the point.
    with _client(db) as second:
        _register_worker(second)
        task = second.get(f"/api/v1/tasks/{tid}").json()
        assert task["status"] != "ready" or task.get("assigned_to"), (
            "task is still unassigned `ready` after a restart -- #878: the "
            "startup reschedule did not run, so the queue is inert until a "
            "human notices and POSTs /api/v1/schedule/trigger"
        )


def test_startup_reschedule_does_not_run_on_a_worker(db):
    """Workers do not schedule; only the main may promote work.

    Asserts the NEGATIVE, not merely that the worker boots: a worker that
    booted fine while also scheduling would satisfy a health check and still
    be wrong.
    """
    with _client(db) as main:
        _register_worker(main)
        tid = main.post(
            "/api/v1/tasks", json={"title": "t", "requires": ["tooling"]}
        ).json()["id"]

    with _client(db, node_role="worker") as worker:
        assert worker.get("/health").json()["role"] == "worker"
        task = worker.get(f"/api/v1/tasks/{tid}").json()
        assert task["status"] == "ready" and not task.get("assigned_to"), (
            "a worker scheduled work: the node_role guard did not hold"
        )


def test_startup_reschedule_never_prevents_boot(db, monkeypatch):
    """A scheduling hiccup must not stop the server starting.

    An unscheduled queue is recoverable; a main that refuses to boot is not.

    The patch target matters and a previous version of this test got it wrong:
    it patched ``ClusterState``, but ``create_app`` builds a ``ClusterStore``
    when ``db_path`` is set (app.py:144), and ``ClusterStore`` does NOT inherit
    from ``ClusterState`` (cluster_store.py:248 — no base class). The patch was
    inert, the real method ran and succeeded, and the test passed WITHOUT ever
    exercising the ``except`` path it exists to cover.

    So this version patches the class actually in use AND asserts the fault was
    really injected, which is what stops the same false positive returning.
    """
    from hermes_cluster.state.cluster_store import ClusterStore

    with _client(db) as first:
        _register_worker(first)
        first.post("/api/v1/tasks", json={"title": "t", "requires": ["tooling"]})

    called = {"n": 0}

    def boom(self, *a, **k):
        called["n"] += 1
        raise RuntimeError("scheduler exploded")

    monkeypatch.setattr(ClusterStore, "trigger_pending_tasks", boom)

    with _client(db) as c:
        assert c.get("/health").status_code == 200, "boot must survive a scheduling fault"

    assert called["n"] > 0, (
        "the fault was never injected — the startup hook did not call "
        "trigger_pending_tasks on ClusterStore, so the except path is untested "
        "and this test is a false positive"
    )
