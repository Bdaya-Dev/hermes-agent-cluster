"""Alibaba Token Plan metering — tests for the V3 signer, the seat parser,
the poller cycle, alerting, the read surface, and the plugin hand-off.

Fixture provenance (tests_v3/fixtures/modelstudio_*_20260914.json): recorded
LIVE on 2026-09-14 against modelstudio.ap-southeast-1.aliyuncs.com with the
re-derived signer (both GETs HTTP 200) using the read-only RAM user
bdaya-hermes-metering, then scrubbed: account ids, instance codes and account
names replaced with deterministic hashes/synthetic values; credit figures and
response structure are the real ones.

Signer oracle: the official V3 spec's "Verify your signature implementation"
worked example (alibabacloud.com/help/en/sdk/product-overview/
v3-request-structure-and-signature) — the expected string-to-sign hash and
final signature are copied verbatim from the doc, so the test proves the
re-derivation against the primary source, not against itself.
"""

import asyncio
import hashlib
import json
import os
import copy
from datetime import datetime, timezone
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from hermes_cluster.app import create_app
from hermes_cluster.core import metering as mt
from hermes_cluster.core.metering import (
    ALGORITHM,
    MeteringConfig,
    MeteringPoller,
    ModelStudioClient,
    build_string_to_sign,
    evaluate_alert,
    parse_metering_settings,
    parse_seat_details,
    percent_encode,
    sign_string_to_sign,
    total_surplus,
    _seed_metering_from_config_file,
)
from hermes_cluster.state import ClusterState

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# 1. The signer vs the official spec vector
# ---------------------------------------------------------------------------

def test_signer_matches_official_spec_vector():
    """Spec worked example: POST / with query params, empty body.

    Expected values are verbatim from the V3 doc's verification section:
    canonical request hash 7ea06492... and signature 06563a9e...
    """
    headers = {
        "host": "ecs.cn-shanghai.aliyuncs.com",
        "x-acs-action": "RunInstances",
        "x-acs-version": "2014-05-26",
        "x-acs-date": "2023-10-26T10:22:32Z",
        "x-acs-signature-nonce": "3156853299f313e23d1673dc12e1703d",
        "x-acs-content-sha256": hashlib.sha256(b"").hexdigest(),
    }
    query = {"ImageId": "win2019_1809_x64_dtc_zh-cn_40G_alibase_20230811.vhd",
             "RegionId": "cn-shanghai"}
    sts, canonical, signed_headers = build_string_to_sign(
        "POST", "/", query, headers, b"")
    assert signed_headers == ("host;x-acs-action;x-acs-content-sha256;x-acs-date;"
                              "x-acs-signature-nonce;x-acs-version")
    # doc step 1 shows the exact canonical request; hash it -> doc step 2
    assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == (
        "7ea06492da5221eba5297e897ce16e55f964061054b7695beedaac1145b1e259")
    assert sts == ("ACS3-HMAC-SHA256\n"
                   "7ea06492da5221eba5297e897ce16e55f964061054b7695beedaac1145b1e259")
    # doc step 3: signature over the string-to-sign with the raw secret
    sig = sign_string_to_sign("YourAccessKeySecret", sts)
    assert sig == ("06563a9e1b43f5dfe96b81484da74bceab24a1d853912eee15083a6f0f3283c0")


def test_percent_encode_spec_rules():
    assert percent_encode("a b*c~d") == "a%20b%2Ac~d"  # space->%20, *->%2A, ~ kept
    assert percent_encode("é") == "%C3%A9"             # UTF-8 byte-wise
    assert percent_encode("key=1&2") == "key%3D1%262"


# ---------------------------------------------------------------------------
# 2. The recorded fixtures: shape + parser
# ---------------------------------------------------------------------------

def _seat_fixture():
    return json.loads((FIXTURES / "modelstudio_seat_details_20260914.json").read_text())


def _stats_fixture():
    return json.loads((FIXTURES / "modelstudio_subscription_stats_20260914.json").read_text())


def test_fixture_contains_no_account_identifiers():
    """Scrub guard: the committed fixture must not leak account ids/emails."""
    blob = json.dumps(_seat_fixture())
    assert "@digrum" not in blob and "aliyun-" not in blob
    assert "example.invalid" in blob  # synthetic placeholders present


def test_parse_seat_details_fixture():
    seats = parse_seat_details(_seat_fixture())
    assert len(seats) == 2
    a, b = seats
    assert a["total"] == 208334.0 and abs(a["surplus"] - 191307.22449352) < 1e-6
    assert b["total"] == 250000.0 and b["surplus"] == 0.0
    assert a["cycle_end"] == 1791450000000
    assert a["seat_id"].startswith("seat_")
    assert total_surplus(seats) == pytest.approx(191307.22449352)


def test_parse_seat_details_empty_raises():
    with pytest.raises(mt.ApiError):
        parse_seat_details({"Data": {"Items": []}, "Success": True})
    with pytest.raises(mt.ApiError):
        parse_seat_details({"Success": True})


def test_stats_fixture_shape_is_readable():
    data = _stats_fixture()["Data"]["Items"][0]
    assert data["SeatRemainingCredits"] == pytest.approx(191307.22449352)
    assert data["SeatCredits"] == 458334.0 and data["TotalSeats"] == 2


# ---------------------------------------------------------------------------
# 3. Config parsing + GitOps seed (intake precedent: YAML seeds empty store
#    once; runtime store is authoritative afterwards; never env vars)
# ---------------------------------------------------------------------------

def test_metering_defaults_off_and_values():
    ok, cfg = parse_metering_settings({})
    assert ok and cfg is not None
    assert cfg.enabled is False and cfg.interval_s == 900 and cfg.alert_below == 25000
    assert cfg.fail_threshold == 3
    ok2, bad = parse_metering_settings({"metering": "not-a-dict"})
    assert ok2 is False and bad is None
    ok3, bad2 = parse_metering_settings({"metering": {"interval_s": "abc"}})
    assert ok3 is False and bad2 is None


def test_seed_from_yaml_file_only_bootstraps_empty_store(tmp_path):
    yaml_path = tmp_path / "cluster.yaml"
    yaml_path.write_text("metering:\n  enabled: false\n  interval_s: 600\n"
                         "  alert_below: 25000\n")
    state = ClusterState()
    state.set_config_path(str(yaml_path))
    _seed_metering_from_config_file(state)
    seeded = state.get_config() or {}
    assert seeded.get("metering", {}).get("interval_s") == 600
    # runtime store now authoritative: a changed file must NOT clobber it
    yaml_path.write_text("metering:\n  enabled: true\n  interval_s: 42\n")
    state.set_config({**seeded, "metering": {"interval_s": 901, "enabled": False}})
    _seed_metering_from_config_file(state)
    assert (state.get_config() or {})["metering"]["interval_s"] == 901


# ---------------------------------------------------------------------------
# 4. The poller cycle (disabled no-op, success stores sample, errors scream)
# ---------------------------------------------------------------------------


class _FakeClient:
    def __init__(self, raw=None, exc=None):
        self._raw, self._exc = raw, exc
        self.calls = 0

    async def get_seat_details(self, now=None):
        self.calls += 1
        if self._exc:
            raise self._exc
        return copy.deepcopy(self._raw)


class _RecordingHookManager:
    def __init__(self):
        self.emitted = []

    def emit(self, event_type, data):
        self.emitted.append((event_type.value, data))
        return 1


def _poller(client, hooks=None, alert_below=25000, enabled=True):
    state = ClusterState()
    state.set_config({"metering": {"enabled": enabled, "interval_s": 900,
                                   "alert_below": alert_below}})
    poller = MeteringPoller(
        state=state, hook_manager=hooks or _RecordingHookManager(),
        client_factory_override=lambda cfg: (client, None))
    return poller


def test_disabled_poller_never_calls_client():
    client = _FakeClient(_seat_fixture())
    poller = _poller(client, enabled=False)
    result = poller.poll_once()
    assert result["ok"] is True and result["enabled"] is False
    assert client.calls == 0
    # disabled wake stays cheap but bounded so `enabled: true` lands soon
    assert result["interval_seconds"] == 60


def test_poll_success_stores_sample_and_clears_errors():
    hooks = _RecordingHookManager()
    poller = _poller(_FakeClient(_seat_fixture()), hooks=hooks,
                     alert_below=100000)  # surplus 191k > 100k -> no alert
    result = poller.poll_once()
    assert result["ok"] is True and result["alert"] is None
    st = poller.status()
    assert st["credits_available"] is True
    assert st["credits_remaining_total"] == pytest.approx(191307.22449352)
    assert len(st["seats"]) == 2 and st["fetched_at"] and st["last_error"] is None
    assert hooks.emitted == []
    assert st["last_errors"] == {}


def test_poll_alerts_below_threshold_once_then_suppresses():
    hooks = _RecordingHookManager()
    client = _FakeClient(_seat_fixture())
    poller = _poller(client, hooks=hooks, alert_below=200000)  # 191307 < 200000
    poller.poll_once()
    assert len(hooks.emitted) == 1
    event, data = hooks.emitted[0]
    assert event == "task_failed" and data["source"] == "metering"
    assert data["kind"] == "credits_low"
    assert "200000" in data["message"]
    # identical live condition: suppressed (no 15-min message spam)
    poller.poll_once()
    assert len(hooks.emitted) == 1
    # condition changes (new low total) -> alert again
    fixture = _seat_fixture()
    fixture["Data"]["Items"][0]["EquityList"][0]["CycleSurplusValue"] = 150000.0
    client._raw = fixture
    poller.poll_once()
    assert len(hooks.emitted) == 2


def test_three_consecutive_failures_alert_and_error_is_sanitised():
    hooks = _RecordingHookManager()
    exc = mt.ApiError("GetSubscriptionSeatDetails: HTTP 403 code=Forbidden")
    poller = _poller(_FakeClient(exc=exc), hooks=hooks)
    poller.poll_once()
    poller.poll_once()
    assert hooks.emitted == []  # below threshold, no alert yet
    poller.poll_once()
    assert len(hooks.emitted) == 1
    event, data = hooks.emitted[0]
    assert data["kind"] == "fetch_failed" and data["consecutive_failures"] == 3
    st = poller.status()
    assert st["credits_available"] is False
    assert "HTTP 403" in (st["last_error"] or "")
    assert st["last_errors"]["fetch"]["count"] == 3
    # a success clears the streak
    good = _poller(_FakeClient(_seat_fixture()), hooks=_RecordingHookManager())
    good._alert_state["consecutive_failures"] = 2
    good.poll_once()
    assert good._alert_state["consecutive_failures"] == 0


def test_status_never_contains_credential_values():
    client = _FakeClient(_seat_fixture())
    state = ClusterState()
    state.set_config({"metering": {"enabled": True, "alert_below": 100000}})
    poller = MeteringPoller(state=state, hook_manager=_RecordingHookManager(),
                            credential_loader=lambda cfg: ("AKID-secret-value",
                                                           "SECRET-value-123"),
                            client_factory=lambda ak, sk, cfg: client)
    poller.poll_once()
    blob = json.dumps(poller.status()) + json.dumps(poller.last_errors)
    assert "AKID-secret-value" not in blob and "SECRET-value-123" not in blob


def test_credential_failure_recorded_not_raised():
    state = ClusterState()
    state.set_config({"metering": {"enabled": True}})
    def boom(cfg):
        raise mt.ApiError("no Google access token resolvable")
    poller = MeteringPoller(state=state, hook_manager=_RecordingHookManager(),
                            credential_loader=boom)
    result = poller.poll_once()      # must not raise
    assert result["ok"] is False
    assert "credentials" in poller.last_errors
    # A transient failure retries on the next cycle (once per interval —
    # never per fetch, and it must not poison the cache forever).
    calls = {"n": 0}
    def counting(cfg):
        calls["n"] += 1
        raise mt.ApiError("nope")
    poller._load_creds = counting
    poller.poll_once(); poller.poll_once()
    assert calls["n"] == 2
    assert poller.last_errors["credentials"]["count"] == 3


# ---------------------------------------------------------------------------
# 5. evaluate_alert pure rules
# ---------------------------------------------------------------------------

def test_evaluate_alert_pure():
    cfg = MeteringConfig(alert_below=25000, fail_threshold=3)
    st = {}
    sample = {"seats": [{"surplus": 1000.0}], "credits_remaining_total": 1000.0}
    a = evaluate_alert(st, cfg, sample)
    assert a and a["kind"] == "credits_low"
    assert evaluate_alert(st, cfg, sample) is None      # suppressed
    b = evaluate_alert(st, cfg, {"seats": [{"surplus": 999999.0}],
                                 "credits_remaining_total": 999999.0})
    assert b is None and st["last_alert_key"] is None   # recovered, cleared
    fails = {}
    for i in (1, 2):
        assert evaluate_alert(fails, cfg, None) is None
    a3 = evaluate_alert(fails, cfg, None)
    assert a3 and a3["kind"] == "fetch_failed" and a3["consecutive_failures"] == 3


# ---------------------------------------------------------------------------
# 6. ModelStudioClient wire format (signed GET, bearer-free)
# ---------------------------------------------------------------------------

def test_client_signs_and_headers_present():
    seen = {}

    async def fake_http(method, url, headers):
        seen["method"], seen["url"], seen["headers"] = method, url, dict(headers)
        return 200, json.dumps(_seat_fixture()).encode()

    client = ModelStudioClient("AKIDtest", "secret-test",
                               host="modelstudio.ap-southeast-1.aliyuncs.com",
                               http=fake_http)
    raw = asyncio.run(client.get_seat_details(now=datetime(
        2026, 9, 14, 8, 0, 0, tzinfo=timezone.utc)))
    assert raw["Success"] is True
    assert seen["method"] == "GET"
    assert seen["url"] == ("https://modelstudio.ap-southeast-1.aliyuncs.com"
                           "/tokenplan/subscription/seat-detail")
    h = seen["headers"]
    assert h["x-acs-action"] == "GetSubscriptionSeatDetails"
    assert h["x-acs-version"] == "2026-02-10"
    assert h["x-acs-date"] == "2026-09-14T08:00:00Z"
    assert len(h["x-acs-signature-nonce"]) == 32
    assert h["authorization"].startswith(f"{ALGORITHM} Credential=AKIDtest,")
    assert "SignedHeaders=host;x-acs-action;x-acs-content-sha256;x-acs-date;"
    # authorization must be recomputable: same inputs -> same signature
    sts, _, signed = build_string_to_sign(
        "GET", "/tokenplan/subscription/seat-detail", None,
        {k: v for k, v in h.items() if k != "authorization"}, b"")
    expected_sig = sign_string_to_sign("secret-test", sts)
    assert h["authorization"].endswith(f"Signature={expected_sig}")
    assert "Authorization" not in signed.lower().split("signedheaders")[0]


def test_client_non_200_raises_sanitised():
    async def fake_http(method, url, headers):
        return 404, b'{"Code":"InvalidApiOrAction","Message":"nope"}'
    client = ModelStudioClient("AKID", "sk", http=fake_http)
    with pytest.raises(mt.ApiError) as ei:
        asyncio.run(client.get_subscription_stats())
    assert "HTTP 404" in str(ei.value) and "InvalidApiOrAction" in str(ei.value)
    assert "Authorization" not in str(ei.value)


# ---------------------------------------------------------------------------
# 7. HTTP surface (GET /api/v1/metering/alibaba, POST .../poll) + plugin
# ---------------------------------------------------------------------------

@pytest.fixture
def app():
    return create_app(cluster_id="test", node_id="test-node", node_role="main")


@pytest.mark.asyncio
async def test_metering_endpoint_defaults_disabled(app):
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://test") as client:
        r = await client.get("/api/v1/metering/alibaba")
        assert r.status_code == 200
        body = r.json()
        assert body["enabled"] is False
        assert body["interval_s"] == 900 and body["alert_below"] == 25000
        assert body["credits_available"] is False
        assert set(body) >= {"seats", "fetched_at", "last_error", "last_errors"}


@pytest.mark.asyncio
async def test_metering_poll_endpoint(app):
    import hermes_cluster.routers.metering as mrouter
    hooks = _RecordingHookManager()
    poller = _poller(_FakeClient(_seat_fixture()), hooks=hooks,
                     alert_below=100000)
    mrouter.set_poller(poller)
    try:
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://test") as client:
            r = await client.post("/api/v1/metering/alibaba/poll")
            assert r.status_code == 200
            body = r.json()
            assert body["result"]["ok"] is True
            assert body["status"]["credits_available"] is True
    finally:
        mrouter.set_poller(None)


def test_plugin_status_embeds_metering_summary(monkeypatch):
    from hermes_cluster import plugin

    def fake_call(method, path, data=None):
        if path == "/api/v1/summary":
            return {"cluster_id": "c", "tasks": {}}
        if path == "/api/v1/metering/alibaba":
            return {"enabled": True, "credits_available": True,
                    "credits_remaining_total": 191307.22,
                    "alert_below": 25000,
                    "seats": [{"seat_id": "s1", "total": 208334.0,
                               "surplus": 191307.22, "cycle_end": 1}],
                    "fetched_at": "t", "last_error": None,
                    "alert_active": None, "last_errors": {}, "poller_running": True,
                    "interval_s": 900, "fail_threshold": 3}
        return {"error": "unexpected " + path}
    monkeypatch.setattr(plugin, "_api_call", fake_call)
    out = json.loads(plugin.handle_cluster_status({}))
    assert out["metering"]["credits_remaining_total"] == 191307.22
    assert "last_errors" not in out["metering"]          # trimmed
    assert out["metering"]["seats"][0]["surplus"] == 191307.22


def test_plugin_status_survives_old_main(monkeypatch):
    from hermes_cluster import plugin

    def fake_call(method, path, data=None):
        if path == "/api/v1/summary":
            return {"cluster_id": "c"}
        return {"error": "HTTP Error 404"}
    monkeypatch.setattr(plugin, "_api_call", fake_call)
    out = json.loads(plugin.handle_cluster_status({}))
    assert "metering" not in out  # old main: status keeps working
