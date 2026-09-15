"""Release-drift poller — the background thread for the main node (#917).

Thread + config + alert-fan-out shape mirrors core/metering.py exactly (the
estate's proven pattern for a default-off background poller on the main):
daemon thread, runtime config re-read every cycle (enable/interval change
without a redeploy, #906 supervisor), last_errors instrument, and alerts
delivered through the EXISTING webhook fan-out (HookManager task_failed —
the Telegram relay subscribes there; no new channel invented).

What it needs NO credential for:
  * fork main head      — hermes-agent-cluster is a PUBLIC repo, plain GET.
  * commits ahead       — GitHub compare API, same public repo.
  * the deployed commit — baked into the image by cluster-image.yml
                          (HERMES_CLUSTER_BUILD_COMMIT env). This is the
                          running system's own truth: drift detected against
                          it answers "is merged code actually running",
                          stronger than reading the manifest pin (desired
                          state) — and ArgoCD already reports the manifest
                          sync, which is exactly the blind spot #917 measured:
                          Synced-to-stale looks healthy everywhere except to
                          this comparison.
  * digest resolution   — Artifact Registry v2 under the pod's Workload
                          Identity (#892 metering precedent): the metadata
                          server answers for the federated GSA, which holds
                          artifactregistry.reader.

Builds predating this feature carry no stamped commit: the poller reports
kind='unpinned' via status() (visible on GET /api/v1/release/drift) and raises
NO alarm — a missing field is "unknown", never a false alarm and never a
silent pass.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

from .release_drift import (DriftConfig, compute_drift,
                            evaluate_drift_alert, parse_drift_settings,
                            resolve_release_digests)
from ..hooks.manager import HookManager
from ..hooks.payload import HookEventType, Payload

logger = logging.getLogger("hermes_cluster.release_drift")

BUILD_COMMIT_ENV = "HERMES_CLUSTER_BUILD_COMMIT"

# Same seam order as metering's resolve_google_token: GKE metadata server
# (Workload Identity) first, then the dev fallbacks. Never logs values.
METADATA_TOKEN_URL = ("http://metadata.google.internal/computeMetadata/v1/"
                      "instance/service-accounts/default/token")


def _adc_token(cfg: DriftConfig) -> Optional[str]:
    """Short-lived OAuth token for registry reads: GKE metadata server, then
    config adc_token_file, then ~/.config/gcloud/adc_token. Returns None when
    nothing is available (the digest then comes back as an error string —
    the ALARM must not depend on it)."""
    try:
        import json as _json
        import urllib.request
        req = urllib.request.Request(
            METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=3) as r:
            tok = _json.load(r).get("access_token")
        if tok:
            return tok
    except Exception:
        pass
    for path in (cfg.adc_token_file,
                 os.path.expanduser("~/.config/gcloud/adc_token")):
        try:
            if path and os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    tok = f.read().strip()
                if tok:
                    return tok
        except Exception:
            continue
    return None


class ReleaseDriftPoller:
    """Polls 'is the running build what fork main says should be running?'
    every interval_s; alarms through the hook fan-out on sustained drift.

    fetchers are injectable so tests never touch the network:
      fetch_main_head()   -> full sha or None
      fetch_compare(pin)  -> dict(ahead_by=int, commits=[{sha,message}]) or None
    """

    def __init__(self, state: Any,
                 hook_manager: Optional[HookManager] = None,
                 fetch_main_head: Optional[Callable[[], Optional[str]]] = None,
                 fetch_compare: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
                 token_provider: Optional[Callable[[DriftConfig], Optional[str]]] = None,
                 digest_resolver: Optional[Callable[..., Dict[str, Any]]] = None,
                 dispatcher_factory: Optional[Callable[[int, int], Any]] = None,
                 clock: Callable[[], float] = time.time):
        self.state = state
        self.hook_manager = hook_manager
        self._fetch_main_head = fetch_main_head or self._gh_main_head
        self._fetch_compare = fetch_compare or self._gh_compare
        self._token_provider = token_provider or _adc_token
        self._digest_resolver = digest_resolver or resolve_release_digests
        self._dispatcher_factory = dispatcher_factory
        self._clock = clock
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._supervisor: Optional[Dict[str, Any]] = None
        self._lock = threading.Lock()
        self._sample: Optional[Dict[str, Any]] = None
        self._alert_state: Dict[str, Any] = {}
        self.last_errors: Dict[str, Any] = {}

    # -- lifecycle (metering shape) -------------------------------------------

    def current_enabled(self) -> bool:
        ok, settings = parse_drift_settings(
            self.state.get_config() if hasattr(self.state, "get_config") else {})
        return bool(ok and settings and settings.enabled)

    def ensure_started(self) -> bool:
        if not self.current_enabled():
            return False
        self.start()
        return True

    def start(self) -> None:
        if self._thread and self._thread.is_alive() and not self._stop.is_set():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="release-drift-poller")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread = None

    def start_supervisor(self, interval_s: float = 60.0) -> bool:
        sup = self._supervisor
        if sup is not None and sup["thread"] is not None and sup["thread"].is_alive():
            return True
        sup = {"stop": threading.Event(), "thread": None,
               "interval_s": max(0.05, float(interval_s))}
        t = threading.Thread(target=self._supervise, args=(sup,), daemon=True,
                             name="release-drift-supervisor")
        sup["thread"] = t
        self._supervisor = sup
        t.start()
        return True

    def stop_supervisor(self) -> None:
        sup = self._supervisor
        if sup is not None:
            sup["stop"].set()
            sup["thread"] = None

    def _supervise(self, sup: Dict[str, Any]) -> None:
        stop_evt: threading.Event = sup["stop"]
        while not stop_evt.is_set():
            try:
                enabled = self.current_enabled()
                running = bool(self._thread and self._thread.is_alive())
                if enabled and not running:
                    logger.info("release_drift supervisor: enabled — arming")
                    self.start()
                elif not enabled and running:
                    logger.info("release_drift supervisor: disabled — stopping")
                    self.stop()
            except Exception as exc:
                logger.warning("release_drift supervisor tick failed: %s", exc)
            stop_evt.wait(sup["interval_s"])

    def _run(self) -> None:
        while not self._stop.is_set():
            interval = 900
            try:
                result = self.poll_once()
                if result.get("interval_seconds"):
                    interval = int(result["interval_seconds"])
            except Exception as exc:  # the loop must never die
                logger.exception("release_drift poll cycle crashed outside poll_once")
                self._record_error("poll_cycle", exc)
            self._stop.wait(max(30, min(int(interval or 900), 86400)))

    def _record_error(self, stage: str, exc: BaseException) -> None:
        prev = self.last_errors.get(stage)
        count = (prev.get("count", 0) + 1) if isinstance(prev, dict) else 1
        self.last_errors[stage] = {
            "type": type(exc).__name__,
            "message": str(exc)[:300],
            "count": count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # -- deployed truth ---------------------------------------------------------

    @staticmethod
    def deployed_commit() -> Optional[str]:
        raw = (os.environ.get(BUILD_COMMIT_ENV) or "").strip().lower()
        return raw if len(raw) == 40 and all(c in "0123456789abcdef" for c in raw) \
            else None

    # -- GitHub (public repo reads; no credential) ------------------------------

    def _gh_main_head(self) -> Optional[str]:
        from .release_drift import FORK_REPO
        data = self._gh_get(f"repos/{FORK_REPO}/commits?per_page=1")
        try:
            return data[0]["sha"]
        except (KeyError, IndexError, TypeError):
            return None

    def _gh_compare(self, pin: str) -> Optional[Dict[str, Any]]:
        from .release_drift import FORK_REPO
        ok, settings = parse_drift_settings(self._config())
        branch = settings.main_branch if settings else "main"
        data = self._gh_get(f"repos/{FORK_REPO}/compare/{pin}...{branch}")
        if not isinstance(data, dict) or "ahead_by" not in data:
            return None
        return {"ahead_by": int(data["ahead_by"]),
                "commits": [{"sha": c.get("sha", ""),
                             "message": (c.get("commit") or {}).get("message", "")[:200]}
                            for c in (data.get("commits") or [])[:20]]}

    def _gh_get(self, path: str) -> Any:
        import json
        import urllib.request
        req = urllib.request.Request(
            f"https://api.github.com/{path}",
            headers={"Accept": "application/vnd.github+json",
                     "User-Agent": "hermes-cluster-main/release-drift"})
        tok = _gh_token_if_present()
        if tok:
            req.add_header("Authorization", f"Bearer {tok}")
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)

    def _config(self) -> Dict[str, Any]:
        try:
            return (self.state.get_config()
                    if hasattr(self.state, "get_config") else {}) or {}
        except Exception:
            return {}

    # -- one cycle ---------------------------------------------------------------

    def poll_once(self) -> Dict[str, Any]:
        """One drift cycle. Never raises (thread loop + manual POST rely on
        that); failures surface via last_errors / sample.last_error."""
        ok, settings = parse_drift_settings(self._config())
        if not ok or settings is None:
            self._record_error("config",
                               ValueError("release_drift section is malformed"))
            return {"ok": False, "stage": "config"}
        if not settings.enabled:
            return {"ok": True, "enabled": False,
                    "interval_seconds": min(settings.interval_s, 60)}

        deployed = self.deployed_commit()
        main_head = None
        try:
            main_head = self._fetch_main_head()
        except Exception as exc:
            self._record_error("main_head", exc)
        now = self._clock()

        if deployed is None:
            sample = {"deployed_commit": None, "status": "unpinned",
                      "main_head": main_head, "fetched_at": _iso(now),
                      "last_error": None}
            with self._lock:
                self._sample = sample
            return {"ok": True, "enabled": True, "status": "unpinned",
                    "interval_seconds": settings.interval_s}

        if not main_head:
            sample = {"deployed_commit": deployed, "status": "unknown",
                      "main_head": None, "fetched_at": _iso(now),
                      "last_error": "fork main head unavailable"}
            self._update(sample)
            return {"ok": False, "enabled": True, "status": "unknown",
                    "interval_seconds": settings.interval_s}

        drifted = deployed != main_head
        commits_ahead = None
        compare = None
        if drifted:
            try:
                compare = self._fetch_compare(deployed)
                if compare:
                    commits_ahead = compare["ahead_by"]
            except Exception as exc:
                self._record_error("compare", exc)

        drift = compute_drift(deployed, main_head, commits_ahead)
        # in-flight acknowledgement: an operator/reviewer who has CONFIRMED an
        # open release PR for exactly this head records it via config
        # (in_flight_head); the pod itself cannot see the private infra repo's
        # PR list. Matching head -> the "owed" alarm suppresses (a stalled PR
        # past 3x max_age still escalates). Empty never suppresses.
        drift["release_in_flight"] = bool(settings.in_flight_head
                                          and settings.in_flight_head == main_head)
        digests: Dict[str, Any] = {}
        if drift["drifted"]:
            try:
                token = self._token_provider(settings)
                digests = self._digest_resolver(settings, deployed, main_head,
                                                adc_token=token)
            except Exception as exc:
                self._record_error("digest", exc)

        sample = {"deployed_commit": deployed, "status": "ok",
                  "main_head": main_head, "drift": drift,
                  "commits_ahead": drift["commits_ahead"],
                  "range": (f"{deployed[:9]}..{main_head[:9]}"
                            if drift["drifted"] else None),
                  "recent_commits": (compare or {}).get("commits", [])[:10],
                  **{k: v for k, v in digests.items() if v is not None},
                  "fetched_at": _iso(now),
                  "last_error": None}
        self._update(sample)
        self._clear_network_errors()  # the cycle reached a good read

        try:
            alert = evaluate_drift_alert(drift, self._alert_state, settings, now)
        except Exception as exc:
            self._record_error("alert_eval", exc)
            alert = None
        if alert is not None:
            if digests.get("main_digest"):
                alert["main_digest"] = digests["main_digest"]
            if sample.get("range"):
                alert["commit_range"] = sample["range"]
            self.emit_alert(alert)
            self._update({**sample, "alert_active": alert})
        return {"ok": True, "enabled": True, "status": "ok",
                "drifted": drift["drifted"], "alert": alert,
                "interval_seconds": settings.interval_s}

    def _update(self, sample: Dict[str, Any]) -> None:
        with self._lock:
            self._sample = sample

    def _clear_network_errors(self) -> None:
        # instrument must be able to scream: only a cycle that reached a
        # GOOD read clears the network-error counters (metering's
        # last_errors.pop("fetch") lesson, minus the bug of clearing on a
        # failed cycle).
        for stage in ("main_head", "compare", "fetch", "digest"):
            self.last_errors.pop(stage, None)

    # -- alert fan-out (metering shape, task_failed hook = Telegram path) -------

    def emit_alert(self, alert: Dict[str, Any]) -> int:
        if self.hook_manager is None:
            logger.warning("release drift alert (no hook manager): %s",
                           alert.get("message"))
            return 0
        hooks = self.hook_manager.get_hooks_for_event(HookEventType.TASK_FAILED)
        if not hooks:
            logger.warning("release drift alert with NO subscribers (register "
                           "a task_failed hook to receive it): %s",
                           alert.get("message"))
            return 0
        payload = Payload(event_type=HookEventType.TASK_FAILED,
                          timestamp=datetime.now(timezone.utc),
                          data={"source": "release_drift", **alert})
        from .metering import _run_coro_blocking  # same fan-out engine
        from ..hooks.payload import DeliveryStatus

        def _make_dispatcher(max_retries, http_timeout):
            # Dedicated Dispatcher per cycle (metering's loop-per-cycle
            # lesson): the shared one binds its semaphore to the first loop.
            from ..hooks.dispatcher import Dispatcher
            return Dispatcher(max_retries=max_retries, http_timeout=http_timeout)
        factory = self._dispatcher_factory or _make_dispatcher

        async def _emit() -> int:
            shared = self.hook_manager._dispatcher
            dispatcher = factory(shared.max_retries, shared.http_timeout)
            dispatcher.start()
            try:
                results = await asyncio.gather(*[
                    dispatcher.deliver(hook_id=h.id, hook_url=h.url,
                                       hook_secret=h.secret, payload=payload,
                                       callback=self.hook_manager._record_delivery)
                    for h in hooks], return_exceptions=True)
            finally:
                dispatcher.stop()
            ok = 0
            for res in results:
                if isinstance(res, BaseException):
                    self._record_error("alert_delivery",
                                       RuntimeError(str(res)[:200]))
                    continue
                if getattr(res, "status", None) == DeliveryStatus.SUCCESS.value:
                    ok += 1
            return ok

        try:
            return _run_coro_blocking(_emit)
        except Exception as exc:
            self._record_error("alert_emit", exc)
            logger.warning("release drift alert emit failed: %s", exc)
            return 0

    # -- read surface -------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        ok, settings = parse_drift_settings(self._config())
        with self._lock:
            sample = dict(self._sample) if isinstance(self._sample, dict) else None
        return {
            "enabled": bool(ok and settings and settings.enabled),
            "interval_s": settings.interval_s if settings else 900,
            "grace_s": settings.grace_s if settings else 1800,
            "max_age_s": settings.max_age_s if settings else 3600,
            "commit_threshold": settings.commit_threshold if settings else 2,
            "deployed_commit": self.deployed_commit(),
            "poller_running": bool(self._thread and self._thread.is_alive()),
            "sample": sample,
            "last_errors": dict(self.last_errors),
        }


def _gh_token_if_present() -> Optional[str]:
    """Optional API-rate token via the estate's node-local resolver
    (lane_gh_token.py get --json), NEVER env, NEVER printed. Absent = 60
    req/hr unauthenticated, which the default 15-min interval comfortably fits."""
    try:
        import json
        import subprocess
        out = subprocess.run(
            ["python3", "scripts/lane_gh_token.py", "get", "--json"],
            capture_output=True, text=True, timeout=20).stdout
        data = json.loads(out)
        tok = data.get("token")
        return tok if isinstance(tok, str) and tok else None
    except Exception:
        return None


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _seed_drift_from_config_file(state: Any) -> None:
    """GitOps bootstrap seed (metering/intake precedent): a YAML
    ``release_drift`` section initialises an EMPTY runtime store only; once
    the store has the section the file is ignored, so runtime
    PUT /api/v1/config is authoritative without a redeploy."""
    path = state.get_config_path() if hasattr(state, "get_config_path") else ""
    if not path or not os.path.exists(path):
        return
    try:
        import yaml
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        return
    section = cfg.get("release_drift") if isinstance(cfg, dict) else None
    if not isinstance(section, dict):
        return
    existing = state.get_config() or {}
    if isinstance(existing, dict) and existing.get("release_drift") is not None:
        return  # runtime store authoritative — never clobber
    merged = dict(existing)
    merged["release_drift"] = section
    state.set_config(merged)
    logger.info("Seeded release_drift config from %s (runtime store now "
                "authoritative)", path)
