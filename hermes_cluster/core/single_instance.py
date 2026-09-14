"""Per-node-id single-instance lock for the worker serve process (#899).

The 2026-09-14 windows_desktop incident: the operator stopped the worker's
python process and started the scheduled task; the wrapper's ``:loop``
relaunched the killed child *and* the scheduled task started a second
wrapper — two executors for one node id, both polling and spawning every
assigned task (10 tasks failed/cancelled, 81 orphan processes).

This module gives ``hermes_cluster.serve`` (worker role) the missing
reflex: acquire an exclusive lock keyed by the node id *before* any server
or background thread starts, and exit non-zero with a clear message when
another live instance holds it. The lock file carries the holder's pid;
liveness is re-checked with the same portable OS-level probe
``_ResumedProcess`` uses (os.kill(pid, 0) + the documented Windows
WinError-87 branch), so a crashed/stale holder never wedges a restart.

Portability notes:
  - flock-style advisory locking via O_EXCL create+read is deliberately
    avoided: on Windows an open file handle locks the file against
    deletion, not against a second opener. Instead the *authoritative*
    mutual-exclusion is an OS-level bound socket on a fixed loopback port
    derived from a hash of the node id — bind() is atomic and cross-
    platform (EADDRINUSE / WSAEADDRINUSE both surface as OSError
    EADDRINUSE), and the fd is held for the process lifetime (closed by
    the OS on exit, so a crash frees it with no stale-lock sweep needed).
  - The lock *file* (pid + instance token + port, under the worker data
    dir) is the human/agent-visible companion: it lets a refused
    launcher name the live holder, and lets the Windows ``.cmd`` wrappers
    back off with a "lock held" line without parsing serve's stderr.

If the node's port is taken by a process that is NOT our own (port reuse
after a crash), the file's recorded pid decides: dead pid => stale lock,
take over after freeing... we cannot free another process's port, so a
mismatch re-raises with the holder pid in the message (operator resolves
by killing the stale holder or changing the node id — loud, never silent).
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Fixed loopback band: (1024 << 16) | port would be non-routable anyway —
# we bind 127.0.0.1 only, so the derived port just has to be free. 40000
# keeps us clear of the ephemeral range on both platforms (Windows default
# starts at 49152; Linux at 32768+ — we use 10000-39999 to dodge both
# the Linux ephemeral band and the well-known range).
_PORT_BAND_LO = 10000
_PORT_BAND_HI = 40000


class LockHeldByLiveInstance(RuntimeError):
    """Another live serve instance for this node id holds the lock."""

    def __init__(self, node_id: str, pid: int, lock_path: Path,
                 instance_token: str = ""):
        self.node_id = node_id
        self.holder_pid = pid
        self.lock_path = lock_path
        self.holder_instance_token = instance_token
        super().__init__(
            f"another live hermes_cluster.serve instance holds node id "
            f"'{node_id}' (pid {pid}, lock {lock_path}). Refusing to start "
            f"a second executor for the same node id (#899: two executors "
            f"spawn every lane twice). Stop the running instance first — "
            f"see README 'Restarting a worker' — or let the wrapper back "
            f"off and retry."
        )


def pid_alive(pid: "Optional[int]") -> bool:
    """Portable OS-level liveness probe (same semantics as _ResumedProcess).

    True when a process with this pid exists and we may signal it.
    PermissionError counts as alive (exists, owned by someone else);
    Windows raises OSError WinError 87 / EINVAL for a dead pid (measured
    Windows 11 + CPython 3.13) — treated as NOT alive there, matching
    ProcessLookupError on POSIX. A pid <= 0 is never alive (records with
    pid 0 predate the pid persist and cannot be validated).
    """
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if os.name == "nt" and (
            getattr(exc, "winerror", None) == 87
            or exc.errno == errno.EINVAL
        ):
            return False
        # Other OSErrors (ESRCH variants) — treat as not alive; a false
        # 'not alive' only costs a lock re-validation, and bind() below
        # still arbitrates the real mutual exclusion.
        return False
    except Exception:
        return True


def instance_token() -> str:
    """Fresh per-process random instance token (hex)."""
    import secrets
    return secrets.token_hex(8)


def derive_port(node_id: str) -> int:
    """Deterministic loopback lock port for a node id (stable across
    restarts and platforms — sha256, not Python's per-process-seeded
    hash())."""
    digest = hashlib.sha256(f"hermes-cluster-899:{node_id}".encode()).digest()
    span = _PORT_BAND_HI - _PORT_BAND_LO
    return _PORT_BAND_LO + int.from_bytes(digest[:4], "big") % span


def lock_file_path(node_id: str, data_dir: Optional[Path]) -> Path:
    """The human/agent-visible lock file: <data_dir>/locks/<node-id>.lock."""
    base = Path(data_dir) if data_dir else Path.cwd()
    return base / "locks" / f"{node_id}.lock"


class SingleInstanceLock:
    """Held lock handle. Call release() on clean shutdown (the OS releases
    the bound port on process exit anyway; release() also removes the
    file for tidiness)."""

    def __init__(self, node_id: str, sock: socket.socket, path: Path,
                 token: str):
        self.node_id = node_id
        self._sock = sock
        self.path = path
        self.token = token
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            # Only remove OUR file (compare token) — a takeover race must
            # not delete the new holder's lock.
            if self.path.is_file():
                data = _read_lock(self.path)
                if data and data.get("token") == self.token:
                    self.path.unlink()
        except OSError:
            pass

    def __enter__(self) -> "SingleInstanceLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def _read_lock(path: Path) -> Optional[dict]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return json.loads(raw)
    except (OSError, ValueError):
        return None


def acquire(node_id: str,
            data_dir: Optional[Path | str] = None,
            token: Optional[str] = None,
            ) -> SingleInstanceLock:
    """Take the per-node-id single-instance lock or raise.

    Order of operations is deliberate: bind FIRST (the authoritative,
    atomic arbiter), then write the file (descriptive). A crash between
    the two leaves a port held with no file — a second instance then
    fails at bind, which is the correct refusal.
    """
    if not node_id:
        raise ValueError("node_id is required for the single-instance lock")
    token = token or instance_token()
    port = derive_port(node_id)
    path = lock_file_path(node_id, Path(data_dir) if data_dir else None)

    # 1) Authoritative exclusion: bind a loopback socket on the derived
    #    port. Held open for the process lifetime.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # SO_REUSEADDR is NOT set: on some platforms it lets a second
        # bind steal the port, which is exactly the bug we are fixing.
        sock.bind(("127.0.0.1", port))
    except OSError as exc:
        sock.close()
        if exc.errno not in (errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", errno.EADDRINUSE)):
            # Unexpected bind failure (e.g. permission): fail LOUD — a
            # silent pass here is how two executors got born once.
            raise RuntimeError(
                f"single-instance lock: could not bind 127.0.0.1:{port} "
                f"for node '{node_id}': {exc}"
            ) from exc
        # Port held. Is it our own stale lock file or a genuinely live
        # sibling? Read the file and liveness-probe its pid.
        data = _read_lock(path) or {}
        holder_pid = int(data.get("pid") or 0)
        holder_token = str(data.get("token") or "")
        if holder_pid and not pid_alive(holder_pid):
            # Stale descriptor AND stale pid: the port is held by some
            # unrelated process that reused it after our holder died.
            # Rebind cannot work while a stranger holds the port — raise
            # loudly (port collisions in the derived band are ~1/30000
            # per node; operators resolve by restarting or renaming).
            raise RuntimeError(
                f"single-instance lock: node '{node_id}' lock port "
                f"127.0.0.1:{port} is taken by an unrelated process "
                f"(lock file pid {holder_pid or '?'} is dead). Free the "
                f"port or change the node id — refusing to start a "
                f"worker that could not guarantee single-instance."
            )
        raise LockHeldByLiveInstance(node_id, holder_pid, path,
                                     holder_token) from exc

    # 2) Descriptive lock file: who holds the node id, for wrappers and
    #    humans. Best-effort — the socket is the arbiter, so a
    #    non-writable data dir warns instead of blocking a real worker.
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
        tmp.write_text(json.dumps({
            "node_id": node_id,
            "pid": os.getpid(),
            "token": token,
            "port": port,
            "acquired_at": time.time(),
        }), encoding="utf-8")
        # rename over: on Windows os.replace is atomic-over-existing.
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning(
            "single-instance lock file could not be written (%s: %s) — "
            "the port lock still enforces single instance", path, exc)

    logger.info(
        "single-instance lock acquired: node=%s pid=%d port=%d file=%s",
        node_id, os.getpid(), port, path)
    return SingleInstanceLock(node_id, sock, path, token)
