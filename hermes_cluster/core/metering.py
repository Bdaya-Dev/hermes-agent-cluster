"""Alibaba Token Plan metering — the ModelStudio OpenAPI poller on the main node.

Why: the hosted main spends ModelStudio Token Plan credits through every lane
it dispatches; before this module the only visibility was the console. The
lead proved (2026-09-14) that the ModelStudio OpenAPI (ROA style, host
``modelstudio.ap-southeast-1.aliyuncs.com``, API version ``2026-02-10``,
ACS3-HMAC-SHA256 auth) answers two GETs for the read-only RAM user
``bdaya-hermes-metering``:

  * ``GET /tokenplan/subscription/seat-detail``  (x-acs-action:
    GetSubscriptionSeatDetails) — per-seat ``EquityList[]`` entries carry
    ``CycleTotalValue`` / ``CycleSurplusValue`` / ``CycleEndTime``;
  * ``GET /tokenplan/subscription/stats``        (x-acs-action:
    GetSubscriptionStats) — subscription-level stats.

The signer here is RE-DERIVED from the official V3 signature spec
(https://www.alibabacloud.com/help/en/sdk/product-overview/v3-request-structure-and-signature)
— not copied from the lead's scratchpad — and is verified against the spec's
own "Verify your signature implementation" worked example
(tests_v3/test_metering_alibaba.py::test_signer_matches_official_spec_vector).

Signature shape (spec, condensed):

  CanonicalRequest = HTTPRequestMethod + '\\n' + CanonicalURI + '\\n'
                     + CanonicalQueryString + '\\n' + CanonicalHeaders + '\\n'
                     + SignedHeaders + '\\n' + HashedRequestPayload
  StringToSign     = "ACS3-HMAC-SHA256" + '\\n' + HexEncode(Sha256(CanonicalRequest))
  Signature        = HexEncode(HmacSHA256(key=AccessKeySecret, StringToSign))
  Authorization    = ACS3-HMAC-SHA256 Credential=<AK>,SignedHeaders=<list>,
                     Signature=<sig>

CanonicalHeaders selects host, content-type and every x-acs-* header,
lowercase names sorted ascending, ``name:trim(value)\\n`` each;
HashedRequestPayload = HexEncode(Sha256(body)) and is echoed in the
x-acs-content-sha256 header. The signing key is the RAW AccessKey secret —
never Base64-encoded (spec FAQ: the single most common integration bug).

Credentials: the AK/SK are read from GCP Secret Manager at startup
(bdaya-website/alibaba-ram-metering-access-key-id + …-secret) using a Bearer
token resolved in order: GKE metadata server (Workload Identity), then a
token file (config ``adc_token_file``, else ~/.config/gcloud/adc_token — the
same file scripts/lane_gh_token.py uses), then ``gcloud auth
application-default print-access-token`` as a dev fallback. NEVER an env var
(owner ruling), and no code path here may log or return a secret value —
the status surface exposes totals only.

Configuration lives in the cluster YAML / runtime config under the
``metering`` section (mirrors the intake seed-once shape): the YAML file only
bootstraps an EMPTY runtime store; afterwards PUT /api/v1/config carries
``metering`` through unchanged (ConfigJSON extra="allow"), so interval,
enabled and alert_below can be changed without a redeploy. Defaults:
``enabled: false``, ``interval_s: 900``, ``alert_below: 25000`` credits,
alert after 3 consecutive fetch failures.

Alerting reuses the cluster's EXISTING webhook fan-out (hooks.manager.
HookManager — the documented third-party integration surface; the Telegram
bot registers through it): an alert is emitted as a TASK_FAILED hook event
with ``source: "metering"`` payload data, HMAC-signed by the dispatcher like
every other hook delivery. No new channel was invented.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..hooks.dispatcher import Dispatcher
from ..hooks.manager import HookManager
from ..hooks.payload import DeliveryStatus, HookEventType, Payload

logger = logging.getLogger("hermes_cluster.metering")

ALGORITHM = "ACS3-HMAC-SHA256"
EMPTY_BODY_SHA256 = hashlib.sha256(b"").hexdigest()

# ---------------------------------------------------------------------------
# Configuration (cluster YAML section `metering` — never env vars)
# ---------------------------------------------------------------------------


@dataclass
class MeteringConfig:
    enabled: bool = False
    interval_s: int = 900
    alert_below: float = 25000.0
    fail_threshold: int = 3
    host: str = "modelstudio.ap-southeast-1.aliyuncs.com"
    api_version: str = "2026-02-10"
    project: str = "bdaya-website"
    ak_id_secret: str = "alibaba-ram-metering-access-key-id"
    ak_secret_secret: str = "alibaba-ram-metering-access-key-secret"
    adc_token_file: str = ""
    http_timeout: int = 30

    @property
    def max_surplus_total(self) -> float:
        return self.alert_below


def parse_metering_settings(config: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[MeteringConfig]]:
    """(valid, parsed) from a runtime/yaml config dict's ``metering`` section.

    valid=False means the section is malformed — the poller refuses to run a
    cycle and records the parse error instead of spinning on bad config.
    """
    section = (config or {}).get("metering") if isinstance(config, dict) else None
    if section is None:
        return True, MeteringConfig()
    if not isinstance(section, dict):
        return False, None
    try:
        cfg = MeteringConfig(
            enabled=bool(section.get("enabled", False)),
            interval_s=int(section.get("interval_s", 900)),
            alert_below=float(section.get("alert_below", 25000)),
            fail_threshold=int(section.get("fail_threshold", 3)),
            host=str(section.get("host", MeteringConfig.host)),
            api_version=str(section.get("api_version", MeteringConfig.api_version)),
            project=str(section.get("project", MeteringConfig.project)),
            ak_id_secret=str(section.get("ak_id_secret", MeteringConfig.ak_id_secret)),
            ak_secret_secret=str(section.get("ak_secret_secret",
                                             MeteringConfig.ak_secret_secret)),
            adc_token_file=str(section.get("adc_token_file", "")),
            http_timeout=int(section.get("http_timeout", 30)),
        )
    except (TypeError, ValueError):
        return False, None
    return True, cfg


# ---------------------------------------------------------------------------
# V3 signer (re-derived from the official spec; see module docstring)
# ---------------------------------------------------------------------------

_UNRESERVED = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")


def percent_encode(s: str) -> str:
    """RFC3986 percent-encoding per the V3 spec: unreserved = A-Z a-z 0-9 - _ . ~;
    every other byte of the UTF-8 form becomes %XX (uppercase hex)."""
    out: List[str] = []
    for byte in s.encode("utf-8"):
        ch = chr(byte)
        out.append(ch if ch in _UNRESERVED else "%%%02X" % byte)
    return "".join(out)


def _canonical_query(query: Optional[Dict[str, Any]]) -> str:
    if not query:
        return ""
    items = sorted((percent_encode(str(k)), percent_encode("" if v is None else str(v)))
                   for k, v in query.items())
    return "&".join(f"{k}={v}" for k, v in items)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_string_to_sign(method: str, path: str, query: Optional[Dict[str, Any]],
                         headers: Dict[str, str], body: bytes) -> Tuple[str, str, str]:
    """Pure signer step: returns (string_to_sign, canonical_request, signed_headers).

    ``headers`` must be the lowercase wire headers including the five required
    common headers (host, x-acs-action, x-acs-version, x-acs-date,
    x-acs-signature-nonce, x-acs-content-sha256). Canonical selection follows
    the spec: host, content-type and every x-acs-* header, lowercase names,
    sorted ascending, value trimmed.
    """
    selected = {
        k.lower(): str(v).strip()
        for k, v in headers.items()
        if k.lower().startswith("x-acs-") or k.lower() in ("host", "content-type")
    }
    ordered = sorted(selected.items())
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in ordered)
    signed_headers = ";".join(k for k, _ in ordered)
    canonical_request = "\n".join([
        method.upper(),
        path,
        _canonical_query(query),
        canonical_headers,   # already ends with \n
        signed_headers,
        _sha256_hex(body),
    ])
    string_to_sign = f"{ALGORITHM}\n{_sha256_hex(canonical_request.encode('utf-8'))}"
    return string_to_sign, canonical_request, signed_headers


def sign_string_to_sign(secret: str, string_to_sign: str) -> str:
    """HMAC-SHA256 over the UTF-8 string-to-sign with the RAW secret as key
    (spec FAQ: do NOT base64-encode the key), lowercase hex output."""
    return hmac.new(secret.encode("utf-8"),
                    string_to_sign.encode("utf-8"),
                    hashlib.sha256).hexdigest()


class ApiError(Exception):
    """ModelStudio API error. Message carries code/http-status only — never a
    request header or body, so an error string is safe to surface in status."""

    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


class ModelStudioClient:
    """Minimal ROA client for the two read-only tokenplan endpoints.

    ``http`` is injectable (async callable(method, url, headers) -> (status,
    body_bytes)) so tests drive real response shapes with a recorded fixture;
    the default uses httpx with a bearer-free HTTPS request.
    """

    def __init__(self, access_key_id: str, access_key_secret: str, *,
                 host: str = MeteringConfig.host,
                 api_version: str = MeteringConfig.api_version,
                 http: Optional[Callable[..., Any]] = None,
                 timeout: int = 30):
        if not access_key_id or not access_key_secret:
            raise ApiError("access key pair required")
        self._ak = access_key_id
        self._sk = access_key_secret
        self.host = host
        self.api_version = api_version
        self.timeout = timeout
        self._http = http or self._httpx_request

    async def _httpx_request(self, method: str, url: str, headers: Dict[str, str]):
        import httpx
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.request(method, url, headers=headers)
            return resp.status_code, resp.content

    async def call(self, action: str, path: str,
                   query: Optional[Dict[str, Any]] = None,
                   now: Optional[datetime] = None) -> Dict[str, Any]:
        """One signed GET. ``now`` is injectable for deterministic signing."""
        t = now or datetime.now(timezone.utc)
        body = b""
        headers = {
            "host": self.host,
            "x-acs-action": action,
            "x-acs-version": self.api_version,
            "x-acs-date": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "x-acs-signature-nonce": secrets.token_hex(16),
            "x-acs-content-sha256": _sha256_hex(body),
            "accept": "application/json",
        }
        string_to_sign, _, signed_headers = build_string_to_sign(
            "GET", path, query, headers, body)
        signature = sign_string_to_sign(self._sk, string_to_sign)
        headers["authorization"] = (
            f"{ALGORITHM} Credential={self._ak},"
            f"SignedHeaders={signed_headers},Signature={signature}")
        url = f"https://{self.host}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        status, payload = await self._http("GET", url, headers)
        if status != 200:
            code = ""
            try:
                code = str(json.loads(payload).get("Code", ""))[:80]
            except Exception:
                pass
            raise ApiError(f"{action}: HTTP {status}" + (f" code={code}" if code else ""),
                           status=status)
        try:
            return json.loads(payload)
        except (ValueError, TypeError) as exc:
            raise ApiError(f"{action}: invalid JSON response ({type(exc).__name__})")

    async def get_seat_details(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        return await self.call("GetSubscriptionSeatDetails",
                               "/tokenplan/subscription/seat-detail", now=now)

    async def get_subscription_stats(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        return await self.call("GetSubscriptionStats",
                               "/tokenplan/subscription/stats", now=now)


# ---------------------------------------------------------------------------
# Response parsing (tolerant: seat list may sit under Data or at top level)
# ---------------------------------------------------------------------------


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_seat_details(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalise a GetSubscriptionSeatDetails response into per-seat entries:
    {seat_id, total, surplus, cycle_end} (recorded fixture, 2026-09-14: the
    seat list lives at ``Data.Items``; each seat's ``EquityList[]`` entries
    carry CycleTotalValue / CycleSurplusValue / CycleEndTime in epoch ms;
    credit figures are summed across a seat's equity entries)."""
    data = raw.get("Data") if isinstance(raw, dict) else None
    items = None
    if isinstance(data, dict):
        items = data.get("Items") or data.get("Seats")
    if items is None and isinstance(raw, dict):
        items = raw.get("Items") or raw.get("Seats")
    if not isinstance(items, list):
        items = data if isinstance(data, list) else None
    if not isinstance(items, list):
        raise ApiError("seat-detail: no Items list in response")

    out: List[Dict[str, Any]] = []
    for idx, seat in enumerate(items):
        if not isinstance(seat, dict):
            continue
        total = surplus = 0.0
        cycle_end = None
        equity = seat.get("EquityList") or []
        for eq in equity:
            if not isinstance(eq, dict):
                continue
            t = _as_float(eq.get("CycleTotalValue"))
            s = _as_float(eq.get("CycleSurplusValue"))
            if t is not None:
                total += t
            if s is not None:
                surplus += s
            ce = eq.get("CycleEndTime")
            if ce and cycle_end is None:
                cycle_end = ce
        out.append({
            "seat_id": str(seat.get("SeatId", seat.get("SeatName", idx))),
            "total": total,
            "surplus": surplus,
            "cycle_end": cycle_end,
        })
    if not out:
        raise ApiError("seat-detail: Items list was empty")
    return out


# ---------------------------------------------------------------------------
# Loop-agnostic async runner (poll_once must work from the poller thread AND
# from the async manual-poll route — asyncio.run() in-thread raises there;
# a fresh thread gets its own loop and never collides with the caller's)
# ---------------------------------------------------------------------------


def _run_coro_blocking(coro_factory) -> Any:
    box: Dict[str, Any] = {}

    def _worker():
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            box["value"] = loop.run_until_complete(coro_factory())
        except BaseException as exc:  # re-raised in the caller below
            box["error"] = exc
        finally:
            try:
                asyncio.set_event_loop(None)
            finally:
                loop.close()

    thread = threading.Thread(target=_worker, name="metering-fetch")
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("value")


# ---------------------------------------------------------------------------
# Credential resolution (GCP SM via Bearer token; env vars never, values
# never printed)
# ---------------------------------------------------------------------------


def _metadata_token() -> str:
    """GCE/GKE metadata identity token (Workload Identity) — empty if absent."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/"
            "service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=1.5) as r:
            return str(json.loads(r.read()).get("access_token", ""))
    except Exception:
        return ""


def _adc_token_file_path(cfg: MeteringConfig) -> str:
    for cand in (cfg.adc_token_file, os.path.expanduser("~/.config/gcloud/adc_token")):
        if cand and os.path.exists(cand):
            try:
                with open(cand, encoding="utf-8") as f:
                    token = f.read().strip()
                if token:
                    return token
            except OSError:
                pass
    return ""


def _gcloud_adc_token() -> str:
    import shutil
    import subprocess
    gcloud = (shutil.which("gcloud") or shutil.which("gcloud.exe")
              or os.path.expanduser("~/AppData/Local/google-cloud-sdk/bin/gcloud"))
    try:
        return subprocess.run([gcloud, "auth", "application-default",
                               "print-access-token"],
                              capture_output=True, text=True,
                              timeout=30, check=True).stdout.strip()
    except Exception:
        return ""


def resolve_google_token(cfg: MeteringConfig) -> str:
    """Bearer token for SM reads: metadata (GKE Workload Identity) → token
    file → gcloud ADC (dev fallback). Order = production first."""
    token = _metadata_token()
    if token:
        return token
    token = _adc_token_file_path(cfg)
    if token:
        return token
    token = _gcloud_adc_token()
    if token:
        return token
    raise ApiError("no Google access token resolvable for Secret Manager reads "
                   "(metadata server absent, no adc_token file, gcloud ADC failed)")


def sm_access(cfg: MeteringConfig, token: str, name: str) -> bytes:
    """Read latest version of a SM secret. The value is returned, never logged;
    error messages carry the secret NAME only, never the payload."""
    url = (f"https://secretmanager.googleapis.com/v1/projects/{cfg.project}"
           f"/secrets/{name}/versions/latest:access")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=cfg.http_timeout) as r:
            payload = json.load(r)
        return base64.b64decode(payload["payload"]["data"])
    except Exception as exc:
        raise ApiError(f"secret-manager read failed for {cfg.project}/{name}: "
                       f"{type(exc).__name__}")


def load_credentials(cfg: MeteringConfig) -> Tuple[str, str]:
    token = resolve_google_token(cfg)
    ak_id = sm_access(cfg, token, cfg.ak_id_secret).decode("utf-8").strip()
    ak_secret = sm_access(cfg, token, cfg.ak_secret_secret).decode("utf-8").strip()
    if not ak_id or not ak_secret:
        raise ApiError("metering RAM credentials incomplete")
    return ak_id, ak_secret


# ---------------------------------------------------------------------------
# Alert evaluation (pure, unit-tested)
# ---------------------------------------------------------------------------


def total_surplus(seats: List[Dict[str, Any]]) -> float:
    return float(sum(float(s.get("surplus") or 0.0) for s in seats))


def evaluate_alert(state: Dict[str, Any], cfg: MeteringConfig,
                   sample: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Decide whether THIS poll cycle should fire a metering alert, updating
    ``state`` (the poller's alert bookkeeping) in place.

    Triggers: (a) total surplus < cfg.alert_below credits, (b) cfg.fail_threshold
    consecutive fetch failures. Suppression: an identical alert key is not
    re-sent while it remains the live condition (prevents 900s message spam);
    any successful cycle clears the suppression and the streak.
    """
    if sample is None:  # fetch failure this cycle
        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        n = state["consecutive_failures"]
        if n >= cfg.fail_threshold:
            key = f"failed:{n // max(cfg.fail_threshold, 1)}"
            if state.get("last_alert_key") != key:
                state["last_alert_key"] = key
                return {
                    "kind": "fetch_failed",
                    "consecutive_failures": n,
                    "message": (f"Alibaba metering fetch failed {n}x in a row "
                                f"(threshold {cfg.fail_threshold})"),
                }
        return None

    # success clears the streak
    state["consecutive_failures"] = 0
    surplus = total_surplus(sample.get("seats", []))
    if surplus < cfg.alert_below:
        key = f"low:{round(surplus, 2)}"
        if state.get("last_alert_key") == key:
            return None
        state["last_alert_key"] = key
        return {
            "kind": "credits_low",
            "surplus_total": surplus,
            "alert_below": cfg.alert_below,
            "message": (f"Alibaba Token Plan surplus {surplus:g} credits below "
                        f"alert threshold {cfg.alert_below:g}"),
        }
    state["last_alert_key"] = None
    return None


# ---------------------------------------------------------------------------
# The poller (same thread shape as routers/intake._GitLabPoller: daemon
# thread, new event loop per cycle, threading.Event stop, last_errors map)
# ---------------------------------------------------------------------------


def _seed_metering_from_config_file(state: Any) -> None:
    """GitOps bootstrap seed (intake precedent): a YAML ``metering`` section
    initialises an EMPTY runtime store only; once the store has the section
    the file is ignored, so runtime PUT /api/v1/config (extra=allow carries
    the section) is authoritative without a redeploy."""
    path = state.get_config_path() if hasattr(state, "get_config_path") else ""
    if not path or not os.path.exists(path):
        return
    try:
        import yaml
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        return
    section = cfg.get("metering") if isinstance(cfg, dict) else None
    if not isinstance(section, dict):
        return
    existing = state.get_config() or {}
    if isinstance(existing, dict) and existing.get("metering") is not None:
        return  # runtime store authoritative — never clobber
    merged = dict(existing)
    merged["metering"] = section
    state.set_config(merged)
    logger.info("Seeded metering config from %s (runtime store now authoritative)", path)


class MeteringPoller:
    """Background thread polling the tokenplan seat API every
    ``metering.interval_s`` seconds (min clamp 30s; the YAML section re-reads
    every cycle so enabled/interval/alert_below change without a restart)."""

    def __init__(self, state: Any,
                 credential_loader: Optional[Callable[[MeteringConfig], Tuple[str, str]]] = None,
                 client_factory: Optional[Callable[[str, str, MeteringConfig], Any]] = None,
                 hook_manager: Optional[HookManager] = None,
                 client_factory_override: Optional[Callable[[MeteringConfig],
                                                            Tuple[Any, Optional[str]]]] = None):
        self.state = state
        self._load_creds = credential_loader or load_credentials
        self._client_factory = client_factory or (
            lambda ak, sk, cfg: ModelStudioClient(ak, sk, host=cfg.host,
                                                  api_version=cfg.api_version,
                                                  timeout=cfg.http_timeout))
        self.hook_manager = hook_manager
        # Test seam: swap the WHOLE client resolution (returns
        # (client_or_None, error_or_None)) — used with an ASGITransport-style
        # fake that needs neither real creds nor a real signer target.
        self._client_factory_override = client_factory_override
        self._creds: Optional[Tuple[str, str]] = None
        self._creds_tried = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._sample: Optional[Dict[str, Any]] = None
        self._alert_state: Dict[str, Any] = {}
        self.last_errors: Dict[str, Any] = {}

    # -- lifecycle ----------------------------------------------------------

    def current_enabled(self) -> bool:
        ok, settings = parse_metering_settings(
            self.state.get_config() if hasattr(self.state, "get_config") else {})
        return bool(ok and settings and settings.enabled)

    def ensure_started(self) -> bool:
        """Start the loop iff metering is currently enabled in the runtime
        store (idempotent; returns running state). Called at boot and after
        runtime config writes (routers/config.py) — a default-off node runs
        NO background thread at all, matching the agent_executor gating and
        keeping test/CI processes free of always-on daemons (CI run
        34828677799 exited 139 with one)."""
        if not self.current_enabled():
            return False
        self.start()
        return True

    def start(self) -> None:
        if self._thread and self._thread.is_alive() and not self._stop.is_set():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="alibaba-metering-poller")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            interval = 900
            try:
                result = self.poll_once()
                if result.get("interval_seconds"):
                    interval = int(result["interval_seconds"])
            except Exception as exc:  # defensive: the loop must never die
                logger.exception("metering poll cycle crashed outside poll_once")
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

    # -- one cycle ----------------------------------------------------------

    def poll_once(self) -> Dict[str, Any]:
        """One poll cycle: read runtime config, fetch, store sample, alert.

        Never raises (the thread loop and the manual test path both rely on
        that); failures surface via last_errors / sample.last_error.
        """
        ok, settings = parse_metering_settings(
            self.state.get_config() if hasattr(self.state, "get_config") else {})
        if not ok or settings is None:
            self._record_error("config", ApiError("metering section is malformed"))
            return {"ok": False, "stage": "config"}
        if not settings.enabled:
            # Re-check enablement cheaply (config read, no network): a
            # disabled poller still wakes every <=60s so PUTting
            # metering.enabled takes effect within a minute, not one full
            # interval.
            return {"ok": True, "enabled": False,
                    "interval_seconds": min(settings.interval_s, 60)}

        client, err = self._client(settings)
        if client is None:
            sample = None
            self._record_error("credentials", ApiError(err or "credentials"))
        else:
            try:
                raw = _run_coro_blocking(client.get_seat_details)
                seats = parse_seat_details(raw)
                sample = {
                    "source": "alibaba-modelstudio-tokenplan",
                    "seats": seats,
                    "credits_remaining_total": total_surplus(seats),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "last_error": None,
                }
                with self._lock:
                    self._sample = sample
                # instrument must be able to scream (intake 097d7ea7 lesson)
                self.last_errors.pop("fetch", None)
            except Exception as exc:
                sample = None
                with self._lock:
                    if self._sample is not None:
                        self._sample = dict(self._sample)
                        self._sample["last_error"] = str(exc)[:300]
                    else:
                        self._sample = {"source": "alibaba-modelstudio-tokenplan",
                                        "seats": [], "credits_remaining_total": 0.0,
                                        "fetched_at": None,
                                        "last_error": str(exc)[:300]}
                self._record_error("fetch", exc)
                logger.warning("metering fetch failed (streak counted): %s", exc)

        try:
            alert = evaluate_alert(self._alert_state, settings, sample)
        except Exception as exc:
            self._record_error("alert_eval", exc)
            alert = None
        if alert is not None:
            if sample is not None:
                alert["detail"] = {
                    "credits_remaining_total": sample.get("credits_remaining_total"),
                    "seats": sample.get("seats"),
                }
            self.emit_alert(alert)
            with self._lock:
                if self._sample is not None:
                    self._sample = dict(self._sample)
                    self._sample["alert_active"] = alert
        return {"ok": sample is not None, "interval_seconds": settings.interval_s,
                "alert": alert}

    def _client(self, settings: MeteringConfig):
        """Lazily load + cache the AK/SK at first use (startup-equivalent;
        SM reads are cheap but per-credential-per-process, never per cycle)."""
        if self._client_factory_override is not None:
            return self._client_factory_override(settings)
        if not self._creds_tried:
            self._creds_tried = True
            try:
                self._creds = self._load_creds(settings)
                logger.info("metering credentials loaded (values never printed)")
            except Exception as exc:
                self._creds = None
                self._cred_error = str(exc)[:300]
                # A transient SM/token failure must not stick: allow the NEXT
                # cycle to retry (one attempt per interval, never per-fetch).
                self._creds_tried = False
        if self._creds is None:
            return None, getattr(self, "_cred_error", "credentials unavailable")
        ak, sk = self._creds
        try:
            return self._client_factory(ak, sk, settings), None
        except Exception as exc:
            return None, str(exc)[:300]

    # -- alert fan-out (existing webhook channel, end-to-end) ----------------

    def emit_alert(self, alert: Dict[str, Any]) -> int:
        """Deliver the alert through the cluster's EXISTING webhook stack:
        HookManager.get_hooks_for_event (registry) -> Dispatcher.deliver
        (X-Hub-Signature-256 HMAC, retry ladder) -> _record_delivery (history,
        visible on GET /api/v1/hooks/{id}/deliveries). The Telegram bot
        subscribes to task_failed like any third party — no new channel.

        The dispatcher is start()ed on demand: the app never starts it (no
        prior emitter existed) and deliver() fails closed while
        ``_running`` is False — that silent no-op is exactly what this method
        must not regress into. Runs on the poller's fresh per-cycle loop via
        _run_coro_blocking, so it works from both the thread and the async
        manual-poll route. Returns delivered-to count; failures are recorded
        in last_errors.
        """
        if self.hook_manager is None:
            logger.warning("metering alert (no hook manager): %s", alert.get("message"))
            return 0
        hooks = self.hook_manager.get_hooks_for_event(HookEventType.TASK_FAILED)
        if not hooks:
            logger.warning("metering alert with NO subscribers (register a "
                           "task_failed hook to receive it): %s",
                           alert.get("message"))
            return 0
        payload = Payload(event_type=HookEventType.TASK_FAILED,
                          timestamp=datetime.now(timezone.utc),
                          data={"source": "metering", **alert})

        async def _emit() -> int:
            # A dedicated Dispatcher per cycle: the SHARED manager dispatcher
            # binds its semaphore to whichever loop first awaits it, and this
            # poller owns a FRESH loop per cycle — reusing the shared one
            # across loops would crash (asyncio "bound to a different event
            # loop"). Same class, same retry ladder + HMAC + delivery-history
            # callback, so the wire contract is unchanged.
            shared = self.hook_manager._dispatcher
            dispatcher = Dispatcher(max_retries=shared.max_retries,
                                    http_timeout=shared.http_timeout)
            dispatcher.start()
            try:
                results = await asyncio.gather(*[
                    dispatcher.deliver(
                        hook_id=h.id, hook_url=h.url, hook_secret=h.secret,
                        payload=payload,
                        callback=self.hook_manager._record_delivery)
                    for h in hooks], return_exceptions=True)
            finally:
                dispatcher.stop()
            ok = 0
            for hook, res in zip(hooks, results):
                if isinstance(res, BaseException):
                    self._record_error("alert_delivery", res)
                    continue
                if res.status == DeliveryStatus.SUCCESS.value:
                    ok += 1
                else:
                    self._record_error("alert_delivery", ApiError(
                        f"metering alert to "
                        f"{urllib.parse.urlparse(hook.url).netloc}: "
                        f"{res.status} {res.error}"[:200]))
            return ok

        try:
            return _run_coro_blocking(_emit)
        except Exception as exc:
            self._record_error("alert_emit", exc)
            logger.warning("metering alert emit failed: %s", exc)
            return 0

    # -- read surface ---------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """The /api/v1/metering/alibaba JSON — totals only, never credentials."""
        with self._lock:
            sample = dict(self._sample) if isinstance(self._sample, dict) else self._sample
        ok, settings = parse_metering_settings(
            self.state.get_config() if hasattr(self.state, "get_config") else {})
        return {
            "enabled": bool(ok and settings and settings.enabled),
            "interval_s": settings.interval_s if settings else 900,
            "alert_below": settings.alert_below if settings else 25000,
            "fail_threshold": settings.fail_threshold if settings else 3,
            "poller_running": bool(self._thread and self._thread.is_alive()),
            "credits_available": bool(sample) and sample.get("last_error") is None,
            "seats": (sample or {}).get("seats", []),
            "credits_remaining_total": (sample or {}).get("credits_remaining_total"),
            "fetched_at": (sample or {}).get("fetched_at"),
            "last_error": (sample or {}).get("last_error"),
            "alert_active": (sample or {}).get("alert_active"),
            "last_errors": dict(self.last_errors),
        }
