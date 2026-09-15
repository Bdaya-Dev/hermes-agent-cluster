"""Release-drift automation tests (shared/claude-plugins#917).

The defect (#917, measured): a merge to the fork main builds an image and the
pin only moves when a human notices — ten commits sat merged-and-not-running
for ~8h, then the gap reopened 13 minutes after the hand-cut release. The
detector must therefore FIRE on a stale pin — asserted by test, never by
waiting for it to happen (acceptance #917).

RED discipline: the detector, the alert-evaluation rules, and the digest
resolution below all assert on behavior that does not exist yet.
"""

import time

import pytest

from hermes_cluster.core import release_drift as rd
from hermes_cluster.core.release_drift import (
    DriftConfig,
    compute_drift,
    evaluate_drift_alert,
    extract_pinned_sha,
    parse_drift_settings,
    resolve_release_digests,
)


# ---------------------------------------------------------------------------
# 1. the pin is read from the manifest — from the REAL train-comment shape
# ---------------------------------------------------------------------------

TRAIN5_TEXT = """
images:
  - name: hermes-cluster-main
    newName: europe-west1-docker.pkg.dev/bdaya-website/bdaya-docker/hermes-cluster-main
    newTag: "25da11c5fd8500b0bab503e7d2aeceae2635d8a9"  # RELEASE TRAIN 5 — rolls the hosted main aebf9d9 -> 25da11c (hermes-agent-cluster fork main).
      # Ships 10 ALREADY-MERGED, ALREADY-RV-1'd cluster commits (fork diff aebf9d9f..25da11c5, paths hermes_cluster/**).
      # digest sha256:8e74bc0e3f81f8875587448138777f6b83c7271a62d9599a26539fd71c3850c9
"""

UNQUOTED_TEXT = """
images:
  - name: hermes-cluster-main
    newName: europe-west1-docker.pkg.dev/bdaya-website/bdaya-docker/hermes-cluster-main
    newTag: 8c26790f1234567890abcdef1234567890abcdef
"""

OTHER_IMAGE_TEXT = """
images:
  - name: some-other-app
    newName: eu.gcr.io/other/thing
    newTag: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  - name: hermes-cluster-main
    newName: europe-west1-docker.pkg.dev/bdaya-website/bdaya-docker/hermes-cluster-main
    newTag: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
"""


def test_extract_reads_train5_quoted_form():
    assert extract_pinned_sha(TRAIN5_TEXT) == \
        "25da11c5fd8500b0bab503e7d2aeceae2635d8a9"


def test_extract_reads_unquoted_form():
    assert extract_pinned_sha(UNQUOTED_TEXT) == \
        "8c26790f1234567890abcdef1234567890abcdef"


def test_extract_targets_our_image_block_not_the_first_newTag():
    assert extract_pinned_sha(OTHER_IMAGE_TEXT) == \
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def test_extract_none_when_no_images_block():
    assert extract_pinned_sha("apiVersion: kustomize.config.k8s.io/v1beta1\n") is None


# ---------------------------------------------------------------------------
# 2. THE ACCEPTANCE-RED TEST: a deliberately stale pin raises the alarm
# ---------------------------------------------------------------------------

def _now():
    return time.time()


def test_deliberately_stale_pin_fires_after_grace():
    """The gap: pin 25da11c, main e6b58f9 — the exact measured train-5 state
    (one commit merged AFTER the release was cut). Past the grace window the
    alarm MUST fire with the head named in the message."""
    cfg = DriftConfig(enabled=True, grace_s=1800, max_age_s=3600,
                      commit_threshold=2)
    drift = compute_drift(
        "25da11c5fd8500b0bab503e7d2aeceae2635d8a9",
        "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a", commits_ahead=1)
    assert drift["drifted"] is True
    state = {}
    t0 = _now()
    # grace not passed yet — the image-build + Argo-sync window must not
    # alarm on every push (and CI must not scream during a normal release).
    assert evaluate_drift_alert(drift, state, cfg, t0) is None
    # grace passed on the SAME head -> fire
    alert = evaluate_drift_alert(drift, state, cfg, t0 + 1801)
    assert alert is not None, "stale pin MUST raise the drift alarm"
    assert alert["kind"] == "release_drift"
    assert "25da11c" in alert["message"]
    assert "e6b58f9" in alert["message"]


def test_many_commits_fire_immediately_no_grace_wait():
    cfg = DriftConfig(enabled=True, grace_s=1800, max_age_s=3600,
                      commit_threshold=2)
    drift = compute_drift(
        "0" * 40, "1" * 40, commits_ahead=3)
    alert = evaluate_drift_alert(drift, {}, cfg, _now())
    assert alert is not None
    assert alert["trigger"] == "commits"
    assert "3 commit(s)" in alert["message"]


def test_max_age_is_the_absolute_backstop_one_commit_per_interval():
    """Drift creeping by one commit per interval never crosses
    commit_threshold, and if grace were (wrongly) re-armed on every new head
    it would NEVER alarm. max_age_s must catch it anyway.

    This pins the rule: the age basis survives a head change within the same
    continuous drift episode.
    """
    cfg = DriftConfig(enabled=True, grace_s=86400, max_age_s=1800,
                      commit_threshold=99)
    state = {}
    t0 = _now()
    base = "a" * 39
    for i in range(40):  # 40 intervals of 1 commit each, past max_age
        head = base + format(i % 16, "x")
        drift = compute_drift("b" * 40, head, commits_ahead=1)
        alert = evaluate_drift_alert(drift, state, cfg, t0 + i * 60)
        if alert is not None:
            assert alert["trigger"] == "max_age"
            assert i * 60 >= 1800, "fired before max_age elapsed"
            assert alert["main_sha"] == head
            return
    pytest.fail("max_age_s backstop never fired under creeping drift")


def test_same_head_alerts_once_identical_key_suppressed():
    cfg = DriftConfig(enabled=True, grace_s=0, max_age_s=1,
                      commit_threshold=0)
    drift = compute_drift("c" * 40, "d" * 40, commits_ahead=1)
    state = {}
    t0 = _now()
    first = evaluate_drift_alert(drift, state, cfg, t0)
    assert first is not None
    assert evaluate_drift_alert(drift, state, cfg, t0 + 10) is None


def test_new_drifted_head_realerts():
    """The #917 measured case: train 5 closed the gap, and 13 minutes later
    two commits reopened it — the detector must treat that as a FRESH alarm,
    not 'already warned'."""
    cfg = DriftConfig(enabled=True, grace_s=0, max_age_s=3600,
                      commit_threshold=99)
    state = {}
    t0 = _now()
    assert evaluate_drift_alert(compute_drift("e" * 40, "f" * 40, 1),
                                state, cfg, t0) is not None
    # world moves again: a newer head
    a2 = evaluate_drift_alert(compute_drift("e" * 40, "0" * 40, 2),
                              state, cfg, t0 + 5)
    assert a2 is not None and a2["main_sha"] == "0" * 40


def test_pin_equals_main_clears_state_and_never_alarms():
    cfg = DriftConfig(enabled=True, grace_s=0, max_age_s=1,
                      commit_threshold=0)
    state = {}
    t0 = _now()
    stale = compute_drift("1" * 40, "2" * 40, commits_ahead=5)
    assert evaluate_drift_alert(stale, state, cfg, t0) is not None
    ok = compute_drift("2" * 40, "2" * 40)
    assert evaluate_drift_alert(ok, state, cfg, t0 + 1) is None
    assert state["cycles"] == 0 and state["last_alert_key"] is None
    # the next drift episode starts a FRESH age basis, not a stale one
    assert evaluate_drift_alert(compute_drift("2" * 40, "3" * 40, 1),
                                state, cfg, t0 + 2) is not None


def test_compute_drift_derives_commits_when_unknown():
    d = compute_drift("4" * 40, "5" * 40)
    assert d["drifted"] and d["commits_ahead"] >= 1
    d2 = compute_drift("4" * 40, "4" * 40)
    assert not d2["drifted"] and d2["commits_ahead"] == 0


def test_compute_drift_rejects_malformed_shas():
    with pytest.raises(ValueError):
        compute_drift("not-a-sha", "5" * 40)
    with pytest.raises(ValueError):
        compute_drift("4" * 40, "4" * 39)


# ---------------------------------------------------------------------------
# 3. config parsing (fail-loud like the metering section's contract)
# ---------------------------------------------------------------------------

def test_parse_default_off():
    ok, cfg = parse_drift_settings({})
    assert ok and cfg.enabled is False


def test_parse_malformed_section_is_invalid():
    ok, cfg = parse_drift_settings({"release_drift": "not-a-dict"})
    assert ok is False and cfg is None


def test_parse_bad_interval_is_invalid():
    ok, cfg = parse_drift_settings(
        {"release_drift": {"enabled": True, "interval_s": "ten"}})
    assert ok is False and cfg is None


def test_parse_threshold_below_one_invalid():
    ok, cfg = parse_drift_settings(
        {"release_drift": {"enabled": True, "commit_threshold": 0}})
    assert ok is False and cfg is None


def test_parse_valid_carries_values():
    ok, cfg = parse_drift_settings(
        {"release_drift": {"enabled": True, "interval_s": 300,
                           "grace_s": 600, "max_age_s": 900,
                           "commit_threshold": 5}})
    assert ok and cfg.enabled and cfg.interval_s == 300
    assert cfg.grace_s == 600 and cfg.max_age_s == 900
    assert cfg.commit_threshold == 5


# ---------------------------------------------------------------------------
# 3b. in-flight acknowledgement: an OPEN release PR suppresses the "owed"
#     alarm (the train-6 case — drift noticed, awaiting merge), but a PR that
#     stalls past 3x max_age escalates anyway. The pod cannot read the private
#     infra repo's PR list; in_flight arrives via config acknowledgement.
# ---------------------------------------------------------------------------

def test_in_flight_head_suppresses_owed_alarm():
    cfg = DriftConfig(enabled=True, grace_s=0, max_age_s=3600,
                      commit_threshold=0)
    drift = compute_drift("9" * 40, "8" * 40, commits_ahead=1)
    drift["release_in_flight"] = True
    state = {}
    t0 = _now()
    assert evaluate_drift_alert(drift, state, cfg, t0) is None
    assert evaluate_drift_alert(drift, state, cfg, t0 + 3500) is None
    # but 3x max_age with the PR still un-merged escalates: an open PR that
    # never merges is the train-6 failure mode and must re-scream as STALLED.
    a = evaluate_drift_alert(drift, state, cfg, t0 + 3 * 3600)
    assert a is not None and a["trigger"] == "stalled"
    assert "stalled" in a["message"] or "open for" in a["message"]


def test_stalled_alert_names_the_head():
    cfg = DriftConfig(enabled=True, max_age_s=100, commit_threshold=0)
    drift = compute_drift("7" * 40, "6" * 40, commits_ahead=2)
    drift["release_in_flight"] = True
    state = {}
    t0 = _now()
    assert evaluate_drift_alert(drift, state, cfg, t0) is None  # first sighting
    a = evaluate_drift_alert(drift, state, cfg, t0 + 4 * 100)
    assert a and a["trigger"] == "stalled"
    assert "666666666" in a["message"]


def test_no_acknowledgement_means_alarm_fires_normally():
    """The suppression is ONLY for a CONFIRMED open PR for this exact head.
    Anything else — the default — keeps the owed-alarm contract: a stale pin
    fires (that clause is the whole point of #917)."""
    cfg = DriftConfig(enabled=True, grace_s=0, max_age_s=10,
                      commit_threshold=0)
    drift = compute_drift("5" * 40, "4" * 40, commits_ahead=1)
    alert = evaluate_drift_alert(drift, {}, cfg, _now())
    assert alert and alert["trigger"] in ("commits", "grace", "max_age")
    assert "release PR is owed" in alert["message"]


# ---------------------------------------------------------------------------
# 4. digest resolution — RESOLVED from the registry, never copied
# ---------------------------------------------------------------------------

def test_build_digest_url():
    cfg = DriftConfig()
    u = rd.build_digest_url(cfg, "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a")
    assert u.endswith("/v2/projects/bdaya-website/repos/bdaya-docker/images/"
                      "hermes-cluster-main/manifests/"
                      "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a")
    assert u.startswith("https://europe-west1-docker.pkg.dev")


def test_digest_resolution_survives_registry_error(monkeypatch):
    """A missing digest must not sink the drift alarm — digest=None + reason,
    never an exception out of the poll cycle."""
    cfg = DriftConfig()
    def boom(url, token):
        raise OSError("registry unreachable")
    monkeypatch.setattr(rd, "_http_head_digest", boom, raising=False)
    # the module must expose a seam the poller can use; if the seam is absent
    # the test still proves the function contract below.
    out = resolve_release_digests(cfg, "6" * 40, "7" * 40)
    assert out["pin_digest"] is None
    assert out["main_digest"] is None
    assert out["pin_error"] and out["main_error"]


def test_digest_shape_validated_not_trusted(monkeypatch):
    cfg = DriftConfig()
    monkeypatch.setattr(rd, "_http_head_digest",
                        lambda url, token: "not-a-digest", raising=False)
    out = resolve_release_digests(cfg, "6" * 40, "7" * 40)
    assert out["pin_digest"] is None and out["pin_error"]
