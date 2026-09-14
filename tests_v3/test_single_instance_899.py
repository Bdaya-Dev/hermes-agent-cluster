"""#899 — one executor per node id: worker single-instance lock in serve.

Incident (windows_desktop, measured 2026-09-14): a killed worker child was
relaunched by run-worker-desktop.cmd's ``:loop`` AND a Start-ScheduledTask
started a second wrapper -> two executors for one node id, every lane
spawned twice, 10 tasks failed/cancelled, 81 orphan processes.

This file pins the worker-side reflex: ``serve.main()`` in worker role
takes a per-node-id single-instance lock (an OS-held loopback bind keyed
by the node id + a pid/token lock file under the worker data dir) BEFORE
anything starts, and exits non-zero with a clear, machine-detectable
message (ALREADY RUNNING) when another live instance holds it, so the
``:loop`` wrapper backs off instead of racing.

RED on main: serve has no lock at all — the second instance starts
(rc=0, prints 'Starting hermes-cluster') instead of refusing.
"""

import contextlib
import io
import json
import os
import socket
import sys
from unittest.mock import patch

import pytest

from hermes_cluster.serve import main as serve_main

# #899: import-clean against main — the new module surfaces as an
# assertion ("lock module missing"), not an ImportError.
from typing import Any
si: Any = None
try:
    from hermes_cluster.core import single_instance as _si_mod
    si = _si_mod
except ImportError:  # pragma: no cover - the RED shape on main
    pass

LOCK_REFUSAL = "ALREADY RUNNING"  # the token the .cmd wrappers back off on


def _have_si():
    assert si is not None, (
        "#899: hermes_cluster.core.single_instance does not exist — "
        "serve has no per-node-id single-instance lock")


def _run_worker_serve(tmp_path, node_id="w1", token=None, role="worker"):
    """Invoke serve.main() with uvicorn stubbed; return (rc, stdout+stderr).

    The single-instance lock uses REAL binds on the deterministic derived
    port — mutual exclusion must be the actual OS arbiter, never a mock.
    """
    argv = [
        "serve", "--node-role", role, "--node-id", node_id,
        "--db-path", str(tmp_path / f"{node_id}.db"),
    ]
    if token:
        argv += ["--instance-token", token]
    out, err = io.StringIO(), io.StringIO()
    rc = 0
    with patch.object(sys, "argv", argv), \
         patch("uvicorn.run", side_effect=lambda *a, **k: None):
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                serve_main()
        except SystemExit as exc:
            rc = exc.code if isinstance(exc.code, int) else 1
    return rc, out.getvalue() + err.getvalue()


class TestServeWorkerLock:
    def test_second_worker_instance_refused_nonzero(self, tmp_path):
        """Two live serve instances for one node id: the second exits
        non-zero with the clear refusal naming the holder pid."""
        _have_si()
        first = si.acquire("w1", data_dir=tmp_path)  # real bind held
        try:
            rc, printed = _run_worker_serve(tmp_path, node_id="w1")
            assert rc != 0, (
                "second instance for a live node id must exit non-zero, "
                f"got rc={rc}; printed: {printed[:200]!r}")
            assert LOCK_REFUSAL in printed
            # The message names the live holder's pid for the operator.
            assert str(os.getpid()) in printed
        finally:
            first.release()

    def test_after_release_the_node_id_can_start_again(self, tmp_path):
        """Crash/restart path: once the holder releases (or dies), the
        same node id starts cleanly."""
        _have_si()
        first = si.acquire("w8", data_dir=tmp_path)
        first.release()
        rc, printed = _run_worker_serve(tmp_path, node_id="w8")
        assert rc == 0, f"restart after release must start, got: {printed[:300]!r}"

    def test_different_node_ids_both_start(self, tmp_path):
        """The lock is PER node id: w2 starts while w1 holds its own."""
        _have_si()
        first = si.acquire("w1", data_dir=tmp_path)
        try:
            rc, printed = _run_worker_serve(tmp_path, node_id="w2")
            assert rc == 0, f"distinct node id must start, got: {printed[:300]!r}"
        finally:
            first.release()

    def test_main_role_never_takes_the_worker_lock(self, tmp_path):
        """Role guard: a `main` serve with the same id still starts — the
        lock is the worker-role reflex the brief specifies (the main's own
        --port uvicorn bind already refuses a second main on the same
        socket, so a second mechanism there is moot)."""
        _have_si()
        first = si.acquire("node_main", data_dir=tmp_path)
        try:
            rc, printed = _run_worker_serve(tmp_path, node_id="node_main",
                                            role="main")
            assert rc == 0, (
                f"main role must not consult the worker lock: {printed[:300]!r}")
        finally:
            first.release()

    def test_stale_lock_file_does_not_wedge_restart(self, tmp_path):
        """Crash leaves a lock FILE naming a dead pid but the port is free
        (the OS released it with the process): the restart takes over."""
        _have_si()
        lock_dir = tmp_path / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        node_id = "wstale"
        (lock_dir / f"{node_id}.lock").write_text(json.dumps(
            {"node_id": node_id, "pid": 999999, "token": "old",
             "port": si.derive_port(node_id)}), encoding="utf-8")
        rc, printed = _run_worker_serve(tmp_path, node_id=node_id)
        assert rc == 0, f"stale lock must allow takeover, got: {printed[:300]!r}"
        data = json.loads((lock_dir / f"{node_id}.lock").read_text())
        assert data["pid"] == os.getpid()  # file rewritten to the live holder

    def test_port_squatter_with_dead_holder_pid_fails_loud(self, tmp_path):
        """The unambiguous-collision case: the derived port is held by a
        foreign live socket while our lock file names a DEAD pid — exit
        non-zero with a loud error (never start a possibly-duplicate)."""
        _have_si()
        node_id = "wsquat"
        port = si.derive_port(node_id)
        squatter = socket.socket()
        try:
            squatter.bind(("127.0.0.1", port))
        except OSError:
            pytest.skip(f"port {port} already in use on this machine")
        lock_dir = tmp_path / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        (lock_dir / f"{node_id}.lock").write_text(json.dumps(
            {"node_id": node_id, "pid": 999999, "token": "old", "port": port}),
            encoding="utf-8")
        try:
            rc, printed = _run_worker_serve(tmp_path, node_id=node_id)
            assert rc != 0
            assert "taken by an unrelated process" in printed
        finally:
            squatter.close()


# ---------------------------------------------------------------------------
# Lock module unit pins (the primitives serve uses)
# ---------------------------------------------------------------------------

class TestLockModule:
    def test_acquire_refuses_live_holder(self, tmp_path):
        a = si.acquire("m1", data_dir=tmp_path)
        with pytest.raises(si.LockHeldByLiveInstance) as ei:
            si.acquire("m1", data_dir=tmp_path)
        assert ei.value.holder_pid == os.getpid()
        a.release()

    def test_release_allows_reacquire(self, tmp_path):
        a = si.acquire("m2", data_dir=tmp_path)
        a.release()
        b = si.acquire("m2", data_dir=tmp_path)
        b.release()

    def test_port_is_deterministic_per_node_id(self):
        assert si.derive_port("windows_desktop") == si.derive_port("windows_desktop")
        assert si.derive_port("a") != si.derive_port("b")

    def test_lock_file_records_pid_and_token(self, tmp_path):
        a = si.acquire("m3", data_dir=tmp_path, token="tk1")
        data = json.loads((tmp_path / "locks" / "m3.lock").read_text())
        assert data["pid"] == os.getpid()
        assert data["token"] == "tk1"
        a.release()
        assert not (tmp_path / "locks" / "m3.lock").exists()

    def test_release_does_not_delete_a_takeovers_file(self, tmp_path):
        """Token compare: a release after a takeover must not unlink the
        NEW holder's lock file."""
        a = si.acquire("m4", data_dir=tmp_path, token="tkA")
        path = tmp_path / "locks" / "m4.lock"
        # simulate a takeover rewriting the file (a's socket already closed
        # so the port is free again)
        a._sock.close()
        path.write_text(json.dumps({"node_id": "m4", "pid": os.getpid(),
                                    "token": "tkB", "port": si.derive_port("m4")}),
                        encoding="utf-8")
        a.release()
        assert path.exists(), "release() must keep a foreign-token lock file"
        # clean up so the node id is reusable
        path.unlink()

    def test_pid_alive_self_dead_and_garbage(self):
        assert si.pid_alive(os.getpid()) is True
        assert si.pid_alive(999999) is False   # far above any real pid on CI
        assert si.pid_alive(0) is False
        assert si.pid_alive(-1) is False
        assert si.pid_alive(None) is False  # defensive: records predate pid persist

    def test_bind_collision_without_descriptor_fails_loud(self, tmp_path):
        """No lock file at all + port held by a foreign live socket =>
        RuntimeError ('unrelated process'), NOT a LockHeld — the refusal
        must never name a phantom holder pid."""
        node_id = "mnodesc"
        port = si.derive_port(node_id)
        squatter = socket.socket()
        try:
            squatter.bind(("127.0.0.1", port))
        except OSError:
            pytest.skip(f"port {port} already in use on this machine")
        try:
            with pytest.raises(RuntimeError) as ei:
                si.acquire(node_id, data_dir=tmp_path)
            assert "unrelated process" in str(ei.value)
            assert not isinstance(ei.value, si.LockHeldByLiveInstance)
        finally:
            squatter.close()

    def test_live_sibling_pid_makes_refusal_name_the_holder(self, tmp_path):
        """The refusal must carry the holder pid a wrapper can inspect —
        built from a lock file whose pid is THIS process (live)."""
        node_id = "mlive"
        port = si.derive_port(node_id)
        holder = socket.socket()
        try:
            holder.bind(("127.0.0.1", port))
        except OSError:
            pytest.skip(f"port {port} already in use on this machine")
        lock_dir = tmp_path / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        (lock_dir / f"{node_id}.lock").write_text(json.dumps(
            {"node_id": node_id, "pid": os.getpid(), "token": "tkH",
             "port": port}), encoding="utf-8")
        try:
            with pytest.raises(si.LockHeldByLiveInstance) as ei:
                si.acquire(node_id, data_dir=tmp_path)
            assert ei.value.holder_pid == os.getpid()
            assert "ALREADY RUNNING" not in str(ei.value)  # module message is raw
        finally:
            holder.close()
