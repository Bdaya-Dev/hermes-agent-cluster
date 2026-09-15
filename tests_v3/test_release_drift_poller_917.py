"""Release-drift POLLER tests (#917) — the end-to-end alarm proof.

The acceptance clause: "the drift check must have a test that goes RED on a
deliberately stale pin — assert the alarm FIRES, not merely that the code
runs." The alarm here is a task_failed hook delivery (the estate's proven
Telegram path) — captured by a recording fake, with fetchers injected so no
network is touched (registry/GitHub reads are proven separately by the live
probe in the PR).
"""

import time
from types import SimpleNamespace

import pytest

from hermes_cluster.core import release_drift as rd
from hermes_cluster.core.release_drift_poller import (BUILD_COMMIT_ENV,
                                                      ReleaseDriftPoller)

PIN = "25da11c5fd8500b0bab503e7d2aeceae2635d8a9"   # train 5 pin (real)
MAIN = "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a"  # measured head (real)


class _State:
    def __init__(self, cfg):
        self._cfg = cfg

    def get_config(self):
        return self._cfg


def _cfg(**over):
    base = {"enabled": True, "interval_s": 300, "grace_s": 0, "max_age_s": 3600,
            "commit_threshold": 2}
    base.update(over)
    return {"release_drift": base}


class _RecordingHooks:
    """Captures emit_alert fan-out without starting the dispatcher."""

    def __init__(self):
        self.events = []

    def get_hooks_for_event(self, event):
        self._ev = event
        return [SimpleNamespace(id="h1", url="https://example.invalid/hook",
                                secret="s")]

    def _record_delivery(self, record):
        pass

    def _noop_emit(self, alert):
        self.events.append(alert)


def _poller(cfg, deployed=MAIN, main_head=MAIN, ahead=0, hooks=None):
    calls = {"head": 0, "compare": 0}

    def fetch_head():
        calls["head"] += 1
        return main_head

    def fetch_compare(pin):
        calls["compare"] += 1
        assert pin == deployed
        return {"ahead_by": ahead, "commits": [{"sha": MAIN, "message": "x"}]}

    p = ReleaseDriftPoller(state=_State(cfg), hook_manager=hooks,
                           fetch_main_head=fetch_head,
                           fetch_compare=fetch_compare,
                           token_provider=lambda c: None,
                           digest_resolver=lambda *a, **k: {})
    # freeze the alert fan-out at the seam: record instead of dispatch
    emitted = []
    p.emit_alert = emitted.append
    p._emitted = emitted
    p._calls = calls
    return p


def test_default_off_and_never_starts(monkeypatch):
    """A node with no release_drift section starts NO drift behavior
    (CI exit-139 discipline: default-off means default-off)."""
    p = _poller({}, deployed=MAIN, main_head=MAIN)
    assert p.current_enabled() is False
    assert p.ensure_started() is False
    r = p.poll_once()
    assert r["ok"] and r.get("enabled") is False


def test_deployed_commit_reads_env_and_validates(monkeypatch):
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    assert ReleaseDriftPoller.deployed_commit() == PIN
    monkeypatch.setenv(BUILD_COMMIT_ENV, "short")
    assert ReleaseDriftPoller.deployed_commit() is None
    monkeypatch.setenv(BUILD_COMMIT_ENV, "")
    assert ReleaseDriftPoller.deployed_commit() is None


def test_no_stamp_reports_unpinned_not_an_alarm(monkeypatch):
    """A build predating the stamp must report the blind spot (status
    'unpinned' on the read surface) without screaming — an env of no commit
    is UNKNOWN, never a false alarm and never a silent pass."""
    monkeypatch.delenv(BUILD_COMMIT_ENV, raising=False)
    p = _poller(_cfg(), deployed=None, main_head=MAIN)
    r = p.poll_once()
    assert r["status"] == "unpinned"
    assert "alert" not in r or r["alert"] is None
    assert not p._emitted
    assert p.status()["sample"]["status"] == "unpinned"
    assert p.status()["sample"]["main_head"] == MAIN


def test_in_sync_reports_ok_and_nothing_fires(monkeypatch):
    monkeypatch.setenv(BUILD_COMMIT_ENV, MAIN)
    p = _poller(_cfg(), deployed=MAIN, main_head=MAIN)
    r = p.poll_once()
    assert r["ok"] and r["drifted"] is False
    assert not p._emitted


# --- THE ACCEPTANCE TEST: a deliberately stale pin RAISES the alarm ---------

def test_stale_pin_fires_alarm_end_to_end(monkeypatch):
    """Deployed build = train 5 pin, fork main has moved to e6b58f9 (one
    commit ahead, grace 0): the alarm MUST fire through emit_alert with the
    pin, the head, the range and a GitOps remedy in the message — asserted,
    not awaited."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    p = _poller(_cfg(), deployed=PIN, main_head=MAIN, ahead=1)
    r = p.poll_once()
    assert r["ok"] and r["drifted"] is True
    assert len(p._emitted) == 1, "stale pin MUST raise the drift alarm"
    alert = p._emitted[0]
    assert alert["kind"] == "release_drift"
    assert PIN[:9] in alert["message"] and MAIN[:9] in alert["message"]
    assert alert["commit_range"] == f"{PIN[:9]}..{MAIN[:9]}"
    assert "never kubectl" in alert["message"]
    # the read surface carries it for a human querying the main
    st = p.status()["sample"]
    assert st["alert_active"]["kind"] == "release_drift"
    assert st["commits_ahead"] == 1


def test_alarm_goes_through_task_failed_hook_fanout(monkeypatch):
    """emit_alert's real contract: the alert rides the EXISTING webhook
    stack as a task_failed event with source=release_drift (the Telegram
    relay subscribes there — metering precedent). The dispatcher is replaced
    at the seam; the REAL emit_alert and poll path are exercised."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)

    delivered = []

    class _FakeDispatcher:
        max_retries = 1
        http_timeout = 1

        def start(self):
            pass

        def stop(self):
            pass

        async def deliver(self, hook_id, hook_url, hook_secret, payload,
                          callback):
            delivered.append(payload)
            from hermes_cluster.hooks.payload import DeliveryStatus
            return SimpleNamespace(status=DeliveryStatus.SUCCESS.value)

    hooks = SimpleNamespace(
        _dispatcher=_FakeDispatcher(),
        get_hooks_for_event=lambda ev: [SimpleNamespace(
            id="h1", url="https://example.invalid/hook", secret="s")],
        _record_delivery=lambda rec: None)
    # no emit seam here: the real emit_alert must run
    p = ReleaseDriftPoller(state=_State(_cfg()), hook_manager=hooks,
                           fetch_main_head=lambda: MAIN,
                           fetch_compare=lambda pin: {"ahead_by": 5,
                                                      "commits": []},
                           token_provider=lambda c: None,
                           digest_resolver=lambda *a, **k: {},
                           dispatcher_factory=lambda mr, ht: _FakeDispatcher())
    r = p.poll_once()  # deployed=PIN via env, main=MAIN -> drift -> alert
    assert r["drifted"] is True
    assert delivered, "alert never reached the dispatcher"
    ev = delivered[0]
    assert ev.event_type.value == "task_failed"
    assert ev.data["source"] == "release_drift"
    assert ev.data["kind"] == "release_drift"
    assert PIN[:9] in ev.data["message"]


def test_grace_window_does_not_fire_then_fires(monkeypatch):
    """Noticed-the-moment semantics: within grace (image-build + ArgoCD-sync
    window) a stale-but-new drift is SILENT; past it, one scream."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    now = time.time()
    p = _poller(_cfg(grace_s=1800), deployed=PIN, main_head=MAIN, ahead=1)
    p._clock = lambda: now
    r = p.poll_once()
    assert r["drifted"] and not p._emitted, "must not fire inside grace"
    p._clock = lambda: now + 1801
    p.poll_once()
    assert len(p._emitted) == 1


def test_threshold_trips_without_waiting(monkeypatch):
    """A train-sized backlog (> commit_threshold) alarms on first sight —
    no grace wait (the measured 10-commit case must never repeat quietly)."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    p = _poller(_cfg(grace_s=86400, commit_threshold=2),
                deployed=PIN, main_head=MAIN, ahead=10)
    p.poll_once()
    assert len(p._emitted) == 1
    assert p._emitted[0]["trigger"] == "commits"


def test_head_fetch_failure_is_visible_not_fatal(monkeypatch):
    """GitHub down / rate-limited: the cycle records the error and reports
    status unknown — the thread lives, no alert, and last_errors screams on
    the read surface."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)

    def boom():
        raise OSError("api rate limited")
    p = ReleaseDriftPoller(state=_State(_cfg()), hook_manager=None,
                           fetch_main_head=boom,
                           fetch_compare=lambda pin: None,
                           token_provider=lambda c: None)
    p.emit_alert = lambda a: None
    r = p.poll_once()
    assert r["ok"] is False and r["status"] == "unknown"
    assert p.status()["last_errors"]["main_head"]["count"] == 1


def test_alert_key_suppression_and_head_change_rearm(monkeypatch):
    """Same head, repeated cycles: one alert. The gap reopens with a NEW
    head (the measured 13-minute relapse): the alarm must re-arm."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    p = _poller(_cfg(grace_s=1800, max_age_s=7200), deployed=PIN,
                main_head=MAIN, ahead=1)
    t = [time.time()]
    p._clock = lambda: t[0]
    p.poll_once()
    t[0] += 1801
    p.poll_once()
    assert len(p._emitted) == 1, "identical alert must not re-fire per cycle"
    p.poll_once()
    assert len(p._emitted) == 1
    # a newer head appears: fresh alarm with it
    new_head = "f" * 40
    p._fetch_main_head = lambda: new_head
    p.poll_once()
    assert len(p._emitted) == 2
    assert p._emitted[1]["main_sha"] == new_head


def test_acknowledged_in_flight_head_silences_and_clears_on_merge(monkeypatch):
    """Poller end-to-end of the acknowledgement seam: drifted + in_flight_head
    set to the CURRENT main head -> no owed alarm; head moves again (new
    commits land after the PR was cut) -> the ack no longer matches and the
    alarm is owed again."""
    monkeypatch.setenv(BUILD_COMMIT_ENV, PIN)
    p = _poller({"release_drift": {"enabled": True, "interval_s": 300,
                                   "grace_s": 0, "max_age_s": 3600,
                                   "commit_threshold": 2,
                                   "in_flight_head": MAIN}},
                deployed=PIN, main_head=MAIN, ahead=1)
    r = p.poll_once()
    assert r["drifted"] and not p._emitted, "acknowledged PR must not re-owe"
    # main moves past the acknowledged head
    newer = "d" * 40
    p._fetch_main_head = lambda: newer
    p._fetch_compare = lambda pin: {"ahead_by": 2, "commits": []}
    p.poll_once()
    assert len(p._emitted) == 1, "unacknowledged NEW head must owe again"


def test_malformed_config_records_error_runs_nothing():
    """parse-invalid shape (metering contract): the cycle refuses with
    stage='config' and the instrument records it."""
    p = _poller({"release_drift": "not-a-dict"})
    r = p.poll_once()
    assert r["ok"] is False and r["stage"] == "config"
    assert "config" in p.status()["last_errors"]
