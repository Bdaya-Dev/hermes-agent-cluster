"""Worker connector — outbound signed join+heartbeat from worker to main.

When a node runs with role=worker and cluster.endpoint is set, this module
starts a background thread that:
  1. POSTs a signed /api/v1/nodes/join to the main (retried until successful)
  2. Periodically POSTs signed /api/v1/nodes/heartbeat

The heartbeat interval MUST be significantly shorter than the main's
watchdog degraded_after threshold (default 15s) to avoid flapping between
online/degraded. The default 10s interval gives 5s margin below the 15s
degraded threshold and 20s margin below the 30s offline threshold.

Without this, the worker's heartbeat sender only updates the LOCAL
in-memory store and the main node never sees the worker.

The peer token is resolved in priority order:
  1. PEER_TOKEN environment variable (matches app.py's resolution)
  2. ~/.config/bdaya/hermes-peer-token file (Bdaya fleet convention)
  3. The `peer_token` argument (from cluster.token in config)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

from .disk_preflight import disk_free_gb

logger = logging.getLogger(__name__)


def _resolve_peer_token(explicit: str = "") -> str:
    """Resolve the peer token for signing outbound requests.

    Resolution order matches app.py:92 (env first) to ensure connector
    and plugin signer present the same token to main's per-node map.
    """
    import os
    env_token = os.environ.get("PEER_TOKEN", "")
    if env_token:
        return env_token
    # Bdaya fleet convention: shared peer token at ~/.config/bdaya/hermes-peer-token
    token_path = Path.home() / ".config" / "bdaya" / "hermes-peer-token"
    if token_path.is_file():
        return token_path.read_text().strip()
    if explicit:
        return explicit
    return ""


def _sign_request(
    token: str, node_id: str, method: str, path: str, body: bytes
) -> Dict[str, str]:
    """Sign a request with HMAC-SHA256, returning auth headers."""
    if not token:
        return {}
    ts = int(time.time())
    body_hash = hashlib.sha256(body).hexdigest()
    message = f"{node_id}:{ts}:{method}:{path}:{body_hash}"
    signature = hmac.new(
        token.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    return {
        "X-Peer-Node": node_id,
        "X-Peer-Timestamp": str(ts),
        "X-Peer-Signature": signature,
    }


def _signed_post(
    endpoint: str, path: str, data: dict, token: str, node_id: str, timeout: int = 10
) -> Optional[dict]:
    """POST a signed JSON request to the main node."""
    url = f"{endpoint}{path}"
    body = json.dumps(data).encode()
    req = Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    headers = _sign_request(token, node_id, "POST", path, body)
    for key, value in headers.items():
        req.add_header(key, value)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        logger.warning("signed POST %s failed: %s", path, e)
        return None
    except Exception as e:
        logger.warning("signed POST %s error: %s", path, e)
        return None


_connector_started = False
_connector_lock = threading.Lock()


def run_probe(name: str, spec: dict) -> bool:
    """Execute one capability probe command; True iff it exits 0 (#867)."""
    cmd = spec.get("command") or []
    if not cmd:
        return False
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=int(spec.get("timeout_s", 120)))
        ok = proc.returncode == 0
        if not ok:
            logger.warning("capability probe %s failed (rc=%d): %s",
                           name, proc.returncode, (proc.stdout or "").strip()[-160:])
        return ok
    except Exception as e:
        logger.warning("capability probe %s errored: %s", name, e)
        return False


def declared_capabilities(static_caps: List[str], probes: Dict[str, dict],
                          states: Dict[str, bool]) -> List[str]:
    """Static caps + only the probed caps currently passing. A capability
    named by BOTH config and a probe is treated as probe-gated (#867): the
    node declares it only while its probe succeeds."""
    gated = set(probes.keys())
    return sorted({c for c in static_caps if c not in gated}
                  | {c for c, ok in states.items() if ok})


def start_worker_connector(
    node_id: str,
    cluster_endpoint: str,
    capabilities: List[str],
    peer_token: str = "",
    heartbeat_interval: float = 10.0,
    max_concurrent: int = 0,
    capability_probes: Optional[Dict[str, dict]] = None,
    disk_probe_path: str = "",
    instance_token: str = "",
) -> None:
    """Start the outbound worker connector thread.

    Idempotent: calling twice is a no-op.

    Args:
        node_id: this worker's node ID
        cluster_endpoint: main node URL (e.g. http://127.0.0.1:8787)
        capabilities: list of capability strings (from config)
        peer_token: shared secret for signing (resolved from env/file if empty)
        heartbeat_interval: seconds between heartbeat POSTs. MUST be < main's
            watchdog degraded_after (default 15s). Default 10s gives safe margin.
        max_concurrent: maximum simultaneously-assigned tasks this worker can
            run; declared at /join so the main scheduler honours the ceiling
            when assigning tasks (#833). 0 = unlimited.
        capability_probes: optional {capability: {"command": [...],
            "interval_s": int}} — a capability is DECLARED ONLY while its probe
            command exits 0 (#867). Proves per-node reachability mechanically
            (e.g. the bdaya-lane-agent App key is provisioned here and can
            mint), so a task `requires: [github-write]` can never be dispatched
            to a node whose credential is missing — the #867 silent mid-lane
            failure becomes a loud dispatch-time miss. Re-run on
            interval_s (default 300); a capability lost mid-run is PATCHed off.
        disk_probe_path: #892 — volume/path whose free space is reported as
            ``disk_free_gb`` in EVERY join and heartbeat payload (the main
            degrades the node while the report is below node.min_free_disk_gb).
            Empty resolves to HERMES_HOME, then the lanes default dir, then cwd;
            an unreadable volume reports nothing (field omitted — the main then
            keeps the pre-#892 semantics for this worker).
        instance_token: #899 — this process's single-instance identity,
            carried in the /join payload so the main can REFUSE a second
            executor for the same node id while the first instance is still
            heartbeating (409, retried loudly like any failed join — a 409
            means a duplicate exists somewhere and an operator must stop
            one; the survivor keeps heartbeating regardless). Empty (older
            launch path) omits the field — pre-#899 tokenless join
            semantics are preserved.
    """
    global _connector_started
    with _connector_lock:
        if _connector_started:
            logger.warning("worker connector already running")
            return
        _connector_started = True

    # #892: resolve the disk-probe target ONCE per path value (per-call reads
    # are cheap; keep a callable for tests / explicit wiring).
    def _disk_probe_target() -> str:
        if disk_probe_path:
            return disk_probe_path
        import os
        home = os.environ.get("HERMES_HOME", "")
        if home:
            return home
        return str(Path.cwd())

    def _disk_report() -> Optional[float]:
        return disk_free_gb(_disk_probe_target())

    token = _resolve_peer_token(peer_token)
    if not token:
        logger.warning(
            "worker connector: no peer token available — outbound requests "
            "will be unsigned and likely rejected by main's auth middleware"
        )

    # --- #867 capability probes ------------------------------------------------
    probes = capability_probes or {}

    def _probe_states() -> Dict[str, bool]:
        return {name: run_probe(name, spec) for name, spec in probes.items()}

    def _declared_caps(states: Dict[str, bool]) -> List[str]:
        return declared_capabilities(capabilities, probes, states)

    # strip trailing slash from endpoint
    cluster_endpoint = cluster_endpoint.rstrip("/")

    def _loop():
        logger.info(
            "worker connector started: node=%s endpoint=%s interval=%.1fs caps=%s",
            node_id, cluster_endpoint, heartbeat_interval, capabilities,
        )

        # Join + heartbeat loop. Join is retried on every cycle until the
        # main accepts it (returns node_id). This handles boot-order races
        # (worker starts before main).
        probe_states = _probe_states() if probes else {}
        declared = _declared_caps(probe_states)
        last_probe_at = time.time()
        probe_interval = min([int(s.get("interval_s", 300)) for s in probes.values()] or [300])
        #
        # #897: post-registration main restarts ARE now handled. The main
        # answers {"status":"unknown_node"} for a heartbeat whose node it does
        # not know; the cycle below drops registered_id so the next iteration
        # re-JOINs (main's join is idempotent — it re-registers and refreshes
        # the heartbeat). A transport failure (None) deliberately does NOT
        # touch registered_id: during a real #897 outage the worker keeps
        # beating toward the endpoint instead of churning joins.
        registered_id = None

        while True:
            # Re-probe on its own cadence (#867); push a capability change.
            if probes and time.time() - last_probe_at >= probe_interval:
                last_probe_at = time.time()
                new_states = _probe_states()
                new_declared = _declared_caps(new_states)
                if new_declared != declared and registered_id is not None:
                    path = f"/api/v1/nodes/{registered_id}/capabilities"
                    body = json.dumps({"capabilities": new_declared}).encode()
                    req = Request(f"{cluster_endpoint}{path}", data=body, method="PATCH")
                    req.add_header("Content-Type", "application/json")
                    for k, v in _sign_request(token, node_id, "PATCH", path, body).items():
                        req.add_header(k, v)
                    try:
                        with urlopen(req, timeout=10) as resp:
                            json.loads(resp.read().decode())
                        logger.info("capabilities updated: %s", new_declared)
                        declared = new_declared
                        probe_states = new_states
                    except Exception as e:
                        logger.warning("capabilities PATCH failed: %s", e)

            # Try join if not yet registered
            if registered_id is None:
                join_data = {
                    "node_name": node_id,
                    "capabilities": declared,
                    "endpoint": f"http://{node_id}:0",
                    "max_concurrent": max_concurrent,
                }
                # #899: declare the instance identity (omitted when empty —
                # an absent field is the older-worker join contract).
                if instance_token:
                    join_data["instance_token"] = instance_token
                # #892: report free disk at join too (omit when unreadable —
                # an absent field is the older-worker contract the main honours).
                disk_gb = _disk_report()
                if disk_gb is not None:
                    join_data["disk_free_gb"] = disk_gb
                result = _signed_post(
                    cluster_endpoint, "/api/v1/nodes/join", join_data, token, node_id
                )
                if result and "node_id" in result:
                    registered_id = result["node_id"]
                    logger.info(
                        "worker connector: join succeeded, registered as %s",
                        registered_id,
                    )
                else:
                    logger.warning(
                        "worker connector: join failed, will retry in %.1fs",
                        heartbeat_interval,
                    )

            # Send heartbeat if registered
            if registered_id is not None:
                hb_data = {"node_id": registered_id}
                # #892: every heartbeat carries disk_free_gb of the volume
                # holding HERMES_HOME / the lanes dir — the main's watchdog
                # degrades the node while the report sits below the floor and
                # restores it automatically once the volume recovers.
                disk_gb = _disk_report()
                if disk_gb is not None:
                    hb_data["disk_free_gb"] = disk_gb
                # #897: act on the beat's ANSWER. A restarted main whose store
                # lost this node answers {"status":"unknown_node"}; dropping
                # registered_id sends the loop through the (idempotent) join
                # above on the NEXT cycle, so the worker self-heals instead of
                # staying orphaned until its own process restarts — the exact
                # failure this module's original NOTE documented as unhandled.
                # A None result (transport failure — main simply unreachable
                # for the #897 multi-minute windows) keeps registered_id: the
                # beat is retried next cycle, and a join storm is impossible.
                hb_result = _signed_post(
                    cluster_endpoint, "/api/v1/nodes/heartbeat", hb_data, token, node_id
                )
                if isinstance(hb_result, dict) and hb_result.get("status") == "unknown_node":
                    logger.warning(
                        "worker connector: main does not know %s (post-restart?) "
                        "— re-joining next cycle", registered_id)
                    registered_id = None

            time.sleep(heartbeat_interval)

    thread = threading.Thread(target=_loop, daemon=True, name=f"worker-connector-{node_id}")
    thread.start()
