"""#899 — a worker restart produced two executors for one node id.

Measured 2026-09-14, windows_desktop_worker: the lead killed the child of a
`:loop` keep-alive wrapper to apply a config change; the wrapper relaunched it
5 s later AND the scheduled task started a second wrapper — two executors,
same node id, both spawning every assigned lane (4 executor processes, 81
lane processes at peak, every lane running twice, half the tasks marked
failed by the loser of the lane-lock race).

Fences here pin the four fixes the issue's direction names:
  1. worker-role serve holds a SINGLE-INSTANCE LOCK per node id: a second
     instance exits non-zero with a clear message (the :loop wrapper then
     just backs off);
  2. main joins carry a per-process instance token, and a re-join
     force-REPLACES the previous instance (documented choice, the issue's
     "pick one");
  3. a heartbeat from a NON-current instance is refused (`replaced`) and
     does not refresh the node's heartbeat clock — the stale twin learns it
     is stale;
  4. the worker connector exits LOUD when main says `replaced` (its process
     is a zombie by that point; dying is the recovery the lock + wrapper
     then converge on).

Reds are import-clean against main (no ImportError) and pin new surfaces
with presence asserts, per the fork's getattr-pin rule.
"""
from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core import worker_connector as wc


def _lock_mod():
    """Presence-pinned import: at base the module is absent, so the red is
    an assertion naming the missing surface, never an ImportError."""
    spec = importlib.util.find_spec("hermes_cluster.instance_lock")
    assert spec is not None, "hermes_cluster.instance_lock module missing"
    return importlib.import_module("hermes_cluster.instance_lock")


# ---------------------------------------------------------------------------
# leg 1 — the lock itself
# ---------------------------------------------------------------------------

def test_lock_acquire_release_roundtrip(tmp_path):
    il = _lock_mod()
    h1 = il.acquire("windows_desktop_worker", data_dir=str(tmp_path))
    assert h1 is not None, "first acquire of a free lock must succeed"
    assert h1.path and Path(h1.path).exists()
    h2 = il.acquire("windows_desktop_worker", data_dir=str(tmp_path))
    assert h2 is None, "second acquire while the holder pid is live must fail"
    h1.release()
    h3 = il.acquire("windows_desktop_worker", data_dir=str(tmp_path))
    assert h3 is not None, "acquire after release must succeed"
    h3.release()


def test_lock_steals_a_stale_holder(tmp_path):
    """A crashed instance leaves its lock behind; the pid-liveness probe
    makes the next acquire steal it (the :loop wrapper's relaunch must not
    be locked out by a dead holder)."""
    il = _lock_mod()
    dead = 2_000_000  # above any real pid on fleet members (pid_max ~131072)
    lock_file = tmp_path / "hermes-worker-windows_desktop_worker.lock"
    lock_file.write_text(json.dumps({"pid": dead, "node_id": "windows_desktop_worker"}))
    h = il.acquire("windows_desktop_worker", data_dir=str(tmp_path))
    assert h is not None, "a lock held by a DEAD pid must be stealable"
    h.release()


def test_pid_alive_probe_agrees_with_os():
    il = _lock_mod()
    assert il.pid_alive(os.getpid()) is True
    assert il.pid_alive(2_000_000) is False


def test_serve_worker_role_holds_the_lock(tmp_path):
    """serve's gate function exits non-zero with a clear message when another
    live instance owns the node id; it returns cleanly when the slot is free."""
    il = _lock_mod()
    holder = il.acquire("win_worker_test", data_dir=str(tmp_path))
    assert holder is not None
    with pytest.raises(SystemExit) as ei:
        il.guard_singleton_or_exit("win_worker_test", data_dir=str(tmp_path))
    code = ei.value.code
    assert code not in (0, None), "second instance must exit NON-ZERO"
    holder.release()
    # clean path: returns normally (does NOT exit)
    il.guard_singleton_or_exit("win_worker_test", data_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# legs 2+3 — main: instance token on join, replaced-instance heartbeats
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(create_app(cluster_id="t", node_id="t-main", node_role="main"))


def _join(client, name, instance):
    body = {"node_name": name, "capabilities": ["tooling"]}
    if instance:
        body["instance_id"] = instance
    return client.post("/api/v1/nodes/join", json=body)


def test_join_records_instance_token(client):
    r = _join(client, "win899", "inst-A")
    assert r.status_code == 200
    nid = r.json()["node_id"]
    nodes = {n["id"]: n for n in client.get("/api/v1/nodes").json()}
    assert nodes[nid].get("instance_id") == "inst-A", (
        "the main must remember which PROCESS instance owns a node id (#899)")


def test_rejoin_force_replaces_the_instance(client):
    _join(client, "win899", "inst-A")
    r = _join(client, "win899", "inst-B")  # the restart's second wrapper joins
    assert r.status_code == 200, "replace (not refuse) is the documented choice"
    nodes = {n["id"]: n for n in client.get("/api/v1/nodes").json()}
    assert nodes["node_win899"]["instance_id"] == "inst-B", (
        "a re-join must force-replace the owning instance (documented choice)")


def test_heartbeat_from_a_replaced_instance_is_refused(client):
    _join(client, "win899", "inst-A")
    _join(client, "win899", "inst-B")
    r = client.post("/api/v1/nodes/heartbeat", json={
        "node_id": "node_win899", "instance_id": "inst-A"})
    body = r.json()
    assert body.get("status") == "replaced", (
        "the stale twin's heartbeat must be REFUSED loudly (#899: nothing on "
        "the main knew two processes shared one node id)")
    assert r.status_code == 200, "still 200 — the answer is in the status field"
    # and the CURRENT instance keeps working
    r2 = client.post("/api/v1/nodes/heartbeat", json={
        "node_id": "node_win899", "instance_id": "inst-B"})
    assert r2.json().get("status") == "ok"


def test_heartbeat_without_instance_field_still_works(client):
    """By-design control: older workers (no instance_id anywhere) keep the
    exact pre-#899 behavior — join + heartbeat both succeed."""
    nid = _join(client, "old899", "").json()["node_id"]
    r = client.post("/api/v1/nodes/heartbeat", json={"node_id": nid})
    assert r.json().get("status") == "ok"
    # a NEW-style heartbeat on an OLD-style join (no instance stored) is ok
    r2 = client.post("/api/v1/nodes/heartbeat", json={
        "node_id": nid, "instance_id": "whatever"})
    assert r2.json().get("status") == "ok", (
        "an empty stored instance never refuses — the rule bites only once "
        "the main has a token to compare against")


# ---------------------------------------------------------------------------
# leg 4 — the connector: stamps its instance, and DIES when replaced
# ---------------------------------------------------------------------------

def test_connector_stamps_instance_id_on_join_and_heartbeat(monkeypatch):
    calls = []

    def fake_post(endpoint, path, data, token, node_id, timeout=10):
        calls.append((path, dict(data)))
        if path.endswith("/join"):
            return {"node_id": "node_w", "status": "registered"}
        return {"status": "ok"}
    monkeypatch.setattr(wc, "_signed_post", fake_post)

    ticks = {"n": 0}

    def fake_sleep(_s):
        ticks["n"] += 1
        if ticks["n"] >= 2:
            raise RuntimeError("stop-loop")
    monkeypatch.setattr(wc.time, "sleep", fake_sleep)
    monkeypatch.setattr(wc, "_connector_started", False)

    # The connector loop runs in a daemon thread; run it synchronously so
    # the stop-loop exception (and any future os._exit decision) is testable
    # from the calling thread.
    class _SyncThread:
        def __init__(self, target=None, **kwargs):
            self._target = target

        def start(self):
            self._target()
    monkeypatch.setattr(wc.threading, "Thread", _SyncThread)

    with pytest.raises(RuntimeError):
        wc.start_worker_connector("w", "http://x:1", [], heartbeat_interval=1)
    join = [d for p, d in calls if p.endswith("/join")]
    assert join and join[0].get("instance_id"), (
        "the connector must stamp its per-process instance token on join")
    hb = [d for p, d in calls if p.endswith("/heartbeat")]
    assert hb and hb[0].get("instance_id"), (
        "heartbeats carry the same token so the main can spot the stale twin")
    assert join[0]["instance_id"] == hb[0]["instance_id"]


def test_connector_replaced_reaction_exists():
    """The replaced-instance reaction lives in the connector and is
    testable: a pure decision + a non-zero distinct exit code."""
    decide = getattr(wc, "_replaced_exit_code", None)
    assert decide is not None, "worker_connector._replaced_exit_code missing"
    assert decide({"status": "replaced"}) == wc.INSTANCE_REPLACED_EXIT, (
        "a replaced instance must exit non-zero with a DISTINCT code")
    assert decide({"status": "ok"}) is None
    assert decide({}) is None        # older main: no status field -> never fatal
    assert decide(None) is None      # transport failure: retry, never fatal
    assert wc.INSTANCE_REPLACED_EXIT not in (0, 1), (
        "distinct from a clean exit AND from the generic crash code so the "
        "wrapper/log can tell 'superseded' apart")
