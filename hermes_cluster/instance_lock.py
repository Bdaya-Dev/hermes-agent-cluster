"""Single-instance lock for worker-role serve (#899).

Measured 2026-09-14 (windows_desktop_worker): a keep-alive ``:loop`` wrapper
and a scheduled task produced TWO ``hermes_cluster.serve --node-role worker``
processes under one node id; both polled the main and both spawned every
assigned lane — 81 lane processes at peak, half the tasks marked failed by
the loser of the lane-lock race, orphaned winners running untracked.

The lock: one file per node id under the worker's data dir holding the
holder pid. A second instance sees a LIVE holder pid and exits non-zero with
a clear message (the ``:loop`` wrapper then just backs off, as it does for
any crash — no code in the wrapper needs to know about this). A lock whose
pid is DEAD is stale (crashed/kill -9 without cleanup) and gets stolen.

Platform notes: pid liveness uses ``os.kill(pid, 0)`` on POSIX and
``OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)`` on Windows — the shapes
the executor's own ``_ResumedProcess`` probe already trusts on this fleet.
"""
from __future__ import annotations

import ctypes
import errno
import json
import os
import sys
from pathlib import Path
from typing import Optional

LOCK_DIR_ENV = "HERMES_CLUSTER_LOCK_DIR"
SINGLETON_EXIT_CODE = 3  # distinct from a generic crash (1): the wrapper log reads


def pid_alive(pid: int) -> bool:
    """True when a process with this pid exists right now."""
    if pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return False
            kernel32.CloseHandle(handle)
            return True
        except Exception:
            return True  # probe failed -> assume alive (never steal on doubt)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, not ours
    except OSError as exc:
        # Windows ESRCH-shape (WinError 87) is handled above; POSIX EINVAL
        # means an invalid pid. Treat unknown probes as alive (never steal).
        if exc.errno == errno.EINVAL:
            return False
        return True


def _default_data_dir() -> str:
    env = os.environ.get(LOCK_DIR_ENV, "")
    if env:
        return env
    home = os.environ.get("HERMES_HOME", "")
    if home:
        return str(Path(home) / "run")
    return str(Path.home() / ".config" / "bdaya" / "hermes-cluster-run")


def _lock_path(node_id: str, data_dir: str) -> Path:
    return Path(data_dir) / f"hermes-worker-{node_id}.lock"


class LockHandle:
    """Holds the lock; ``release()`` removes it. The pid inside is ours."""

    def __init__(self, path: Path, node_id: str):
        self.path = str(path)
        self.node_id = node_id
        self._pid = os.getpid()

    def release(self) -> None:
        try:
            p = Path(self.path)
            if p.exists():
                raw = p.read_text(encoding="utf-8", errors="replace")
                if json.loads(raw).get("pid") == self._pid:
                    p.unlink()
        except Exception:
            pass


def acquire(node_id: str, data_dir: Optional[str] = None) -> Optional[LockHandle]:
    """Take the single-instance lock for ``node_id``; None when a LIVE
    process already holds it. Stale locks (dead pid, corrupt file) are
    stolen. Raises nothing: any unexpected FS error degrades to None so a
    broken lock NEVER becomes a reason two instances can start — the
    opposite failure direction is what cost 81 processes."""
    dd = data_dir or _default_data_dir()
    try:
        Path(dd).mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    path = _lock_path(node_id, dd)
    for _attempt in (0, 1):  # one steal-retry
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            holder_pid = None
            try:
                holder_pid = json.loads(
                    path.read_text(encoding="utf-8", errors="replace")).get("pid")
            except Exception:
                pass  # corrupt/empty file: treated as stale below
            if holder_pid and pid_alive(int(holder_pid)):
                return None  # live twin holds it
            try:
                path.unlink()
            except OSError:
                return None
            continue  # stole a stale lock: retry the exclusive create
        except OSError:
            return None
        try:
            os.write(fd, json.dumps(
                {"pid": os.getpid(), "node_id": node_id}).encode())
        finally:
            os.close(fd)
        return LockHandle(path, node_id)
    return None


# Keep the process's handle alive for the process lifetime (GC would not
# unlink it — release() is explicit — but the module ref documents intent
# and serves() installs an atexit).
_HELD: Optional[LockHandle] = None


def guard_singleton_or_exit(node_id: str, data_dir: Optional[str] = None) -> LockHandle:
    """serve's gate: acquire the lock or SystemExit(3) with a message naming
    the live twin's pid. Called from serve for role=worker BEFORE uvicorn
    binds, so the second wrapper never opens its API port at all."""
    global _HELD
    h = acquire(node_id, data_dir=data_dir)
    if h is None:
        holder = "?"
        try:
            dd = data_dir or _default_data_dir()
            holder = str(json.loads(_lock_path(node_id, dd).read_text(
                encoding="utf-8", errors="replace")).get("pid"))
        except Exception:
            pass
        print(
            f"hermes-cluster serve: node id {node_id!r} already has a LIVE "
            f"instance (pid {holder}) holding the single-instance lock (#899). "
            "This is the two-executors-on-one-node-id case: refuse to start. "
            "If you are restarting the worker, stop the old instance first "
            "(see docs/restart-worker.md) — do NOT kill just the child of the "
            "keep-alive wrapper.",
            file=sys.stderr,
        )
        raise SystemExit(SINGLETON_EXIT_CODE)
    _HELD = h
    return h
