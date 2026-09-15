"""The worker connector had NO stop path — `_loop` is `while True:` with no
exit, so a started connector thread ends only when the PROCESS ends.

This is a production gap first (a worker cannot shut its connector down; a
reconfigure means a process restart, and #899's duplicate-executor incident
is exactly two connectors alive at once), and a test-suite defect second.

The test-suite half is what surfaced it, on PR#70's CI run 34999417891:

    tests_v3/test_node_health_selfreport_879.py::test_join_409_is_loud_and_retried
    AssertionError: 409 must retry the join, got [
      ('/api/v1/nodes/join',      {... 'instance_token': 'tok' ...}),
      ('/api/v1/nodes/heartbeat', {'node_id': 'node_pc_worker', ...}),  x3
    ]
    assert 1 >= 2

`registered_id` is LOCAL to `_loop`, so `node_pc_worker` cannot come from the
failing test — it is the id from `test_main_outage_tolerance_897`'s script,
posted by that test's connector thread, still running. Its helper
`_run_connector_briefly` asserts the opposite in its own docstring —
"threads are daemons, nothing leaks past assert" — but a daemon thread is
killed at PROCESS exit, not at test exit. Every later test in the session
shares module state with it, so the leaked thread:

  1. appended its heartbeats into the LATER test's `sent` list, and
  2. consumed that test's one-shot fake `time.sleep`, so the test's own loop
     took SystemExit after ONE join instead of two.

Both monkeypatched globals, both racing. That is why it is intermittent and
why `main` is usually green: the leak is unconditional, only the interleaving
is timing-dependent.
"""

from __future__ import annotations

import threading
import time

import pytest


def _connector_threads():
    return [t for t in threading.enumerate()
            if t.name.startswith("worker-connector-")]


@pytest.fixture(autouse=True)
def _no_leak_around_this_module():
    """Fail loudly rather than leak into a neighbour if the fix regresses."""
    yield
    from hermes_cluster.core import worker_connector as wc
    wc.stop_worker_connector(timeout=2.0)


def _post_ok(endpoint, path, data, token, node_id, **kw):
    if path.endswith("/join"):
        return {"node_id": "node_" + data["node_name"], "status": "registered"}
    return {"status": "ok"}


def test_a_started_connector_contaminates_a_LATER_tests_captures(monkeypatch):
    """THE DEFECT ITSELF, reproduced without naming any new symbol.

    Phase 1 plays `test_main_outage_tolerance_897`: start the real connector
    thread, let it register, walk away. Phase 2 plays every later test in the
    session: install a FRESH capture over the same module global and record
    what arrives. Nothing in phase 2 posts anything.

    RED at origin/main: phase 2's list fills with phase 1's heartbeats,
    carrying phase 1's node id. GREEN once phase 1's thread can be stopped.
    """
    from hermes_cluster.core import worker_connector as wc

    # ---- phase 1: the neighbour test, which leaves a thread running -------
    monkeypatch.setattr(wc, "_signed_post", _post_ok)
    wc._connector_started = False
    wc.start_worker_connector(
        node_id="neighbour", cluster_endpoint="http://main.invalid:8787",
        capabilities=["tooling"], peer_token="t", heartbeat_interval=0.01)
    time.sleep(0.15)                      # let it join + beat a few times

    try:
        wc.stop_worker_connector(timeout=5.0)   # what the fix makes possible
    except AttributeError:
        pass                                    # origin/main: no stop path

    # ---- phase 2: a later, unrelated test installs its own capture -------
    mine = []

    def _capture(endpoint, path, data, token, node_id, **kw):
        mine.append((path, dict(data)))
        return {"status": "ok"}

    monkeypatch.setattr(wc, "_signed_post", _capture)
    time.sleep(0.25)                      # a later test doing its own work

    assert mine == [], (
        "a connector thread from an EARLIER test posted into this test's "
        f"capture: {mine[:4]} — this is the cross-contamination that failed "
        "test_join_409_is_loud_and_retried on PR#70's CI run")


def test_stop_worker_connector_actually_ends_the_thread(monkeypatch):
    """RED at origin/main: `stop_worker_connector` does not exist, and the
    thread it should stop runs until the process dies."""
    from hermes_cluster.core import worker_connector as wc

    monkeypatch.setattr(wc, "_signed_post", _post_ok)
    wc._connector_started = False
    wc.start_worker_connector(
        node_id="leaktest", cluster_endpoint="http://main.invalid:8787",
        capabilities=["tooling"], peer_token="t", heartbeat_interval=0.02,
    )
    assert _connector_threads(), "connector thread never started"

    stopped = wc.stop_worker_connector(timeout=5.0)
    assert stopped is True, "stop_worker_connector must report the join"
    assert not _connector_threads(), (
        "the connector thread outlived stop_worker_connector — this is the "
        "leak that cross-contaminates every later test in the session")


def test_stop_is_idempotent_and_safe_when_never_started():
    """A worker that never started one, and a second stop, must both be
    no-ops — a shutdown path that raises is a shutdown path nobody calls."""
    from hermes_cluster.core import worker_connector as wc

    wc.stop_worker_connector(timeout=1.0)
    assert wc.stop_worker_connector(timeout=1.0) in (True, False)
    assert not _connector_threads()


def test_stop_then_start_again_works(monkeypatch):
    """Stop must also clear `_connector_started`, or the next start is a
    silent no-op that logs 'already running' and beats nothing — a worker
    that reconfigures itself would go permanently dark."""
    from hermes_cluster.core import worker_connector as wc

    monkeypatch.setattr(wc, "_signed_post", _post_ok)
    wc._connector_started = False
    wc.start_worker_connector(
        node_id="cycle1", cluster_endpoint="http://main.invalid:8787",
        capabilities=["tooling"], peer_token="t", heartbeat_interval=0.02)
    wc.stop_worker_connector(timeout=5.0)

    wc.start_worker_connector(
        node_id="cycle2", cluster_endpoint="http://main.invalid:8787",
        capabilities=["tooling"], peer_token="t", heartbeat_interval=0.02)
    names = [t.name for t in _connector_threads()]
    assert names == ["worker-connector-cycle2"], (
        f"restart after stop must yield exactly one live connector, got {names}")


def test_stop_completes_within_about_one_heartbeat_interval(monkeypatch):
    """The shutdown BOUND, pinned so it cannot silently regress.

    The loop waits with one `time.sleep(heartbeat_interval)` per cycle and
    checks the stop event either side of it, so a stop is picked up within at
    most one interval. Two more interruptible designs were tried and both
    break the existing connector tests, which drive this loop by patching
    `time.sleep`:

      * `Event.wait(interval)` is interruptible but ignores the patched
        sleep entirely, so the injected SystemExit never fires and the loop
        never terminates;
      * slicing the sleep into 0.1s chunks calls the patched sleep several
        times per cycle, so a one-shot fake sleep lands mid-cycle and the
        loop runs fewer iterations than the test scripts expect -- that is
        what broke test_join_409_is_loud_and_retried when it was tried here.

    So the bound is one interval, not instant. Workers beat in seconds, so
    this is prompt in practice; a deployment that raises the interval into
    the minutes is choosing a slower shutdown with it.
    """
    from hermes_cluster.core import worker_connector as wc

    interval = 0.5
    monkeypatch.setattr(wc, "_signed_post", _post_ok)
    wc._connector_started = False
    wc.start_worker_connector(
        node_id="boundcheck", cluster_endpoint="http://main.invalid:8787",
        capabilities=["tooling"], peer_token="t", heartbeat_interval=interval)
    time.sleep(0.05)

    t0 = time.monotonic()
    assert wc.stop_worker_connector(timeout=5.0) is True
    elapsed = time.monotonic() - t0

    assert not _connector_threads()
    assert elapsed < interval * 3, (
        f"stop took {elapsed:.2f}s on a {interval}s beat -- the loop is no "
        "longer checking the stop event either side of its sleep")


def test_worker_app_shutdown_stops_the_connector():
    """The app-level half, and the leak that actually fired in the suite.

    `create_app(node_role="worker", ...)` starts the connector on startup.
    The shutdown handler stops the metering poller, the heartbeat sender,
    the watchdog, the lease manager, the recovery manager and the agent
    executor -- the connector was the ONE background loop it did not stop,
    because there was nothing to call.

    RED at origin/main: `tests_v3/test_agent_executor.py::test_executor_running`
    builds exactly this app inside a TestClient block, and the connector
    thread ("worker-connector-test-worker") was still alive hundreds of
    tests later, when it failed the stop tests in this module.
    """
    from fastapi.testclient import TestClient
    from hermes_cluster.app import create_app

    app = create_app(
        cluster_id="test-cluster",
        node_id="shutdown-probe",
        node_role="worker",
        cluster_endpoint="http://127.0.0.1:9999",   # unreachable on purpose
    )
    with TestClient(app):
        pass                                        # startup, then shutdown

    assert not _connector_threads(), (
        "the worker app's shutdown left its connector thread running")
