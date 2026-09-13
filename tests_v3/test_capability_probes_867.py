"""#867 — capability probes: a worker declares github-write ONLY while its
local probe passes, so dispatch-time filtering replaces the silent mid-lane
failure where a lane produced a verdict it could not post.

Two layers:
  1. declared_capabilities() — the pure gating maths (no subprocess).
  2. run_probe() — command exit-code mapping, incl. the failure modes the
     estate actually hit (#788: credential on one member, not another).
"""
import sys

from hermes_cluster.core.worker_connector import declared_capabilities, run_probe


def test_static_caps_pass_through_when_no_probes():
    assert declared_capabilities(["native", "tooling"], {}, {}) == ["native", "tooling"]


def test_probed_cap_declared_only_while_passing():
    probes = {"github-write": {"command": ["true"]}}
    caps = ["native-win", "github-write"]  # config ALSO lists it -> still gated
    assert "github-write" in declared_capabilities(caps, probes, {"github-write": True})
    assert "github-write" not in declared_capabilities(caps, probes, {"github-write": False})
    # the ungated caps survive the probe failure — the node stays dispatchable
    # for non-GitHub work (windows-pc read-only reviews keep flowing).
    assert declared_capabilities(caps, probes, {"github-write": False}) == ["native-win"]


def test_multiple_probes_independent():
    probes = {"github-write": {"command": ["true"]}, "gcp-admin": {"command": ["true"]}}
    states = {"github-write": True, "gcp-admin": False}
    out = declared_capabilities(["planning"], probes, states)
    assert out == ["github-write", "planning"]


def test_run_probe_maps_exit_codes():
    assert run_probe("ok", {"command": [sys.executable, "-c", "import sys; sys.exit(0)"]}) is True
    assert run_probe("bad", {"command": [sys.executable, "-c", "import sys; sys.exit(1)"]}) is False


def test_run_probe_empty_command_is_false_not_crash():
    # a config typo (missing command) fails CLOSED: capability simply absent.
    assert run_probe("typo", {}) is False


def test_run_probe_timeout_is_false():
    # unreachable probe (slow/absent resolver) must not hang the connector loop
    assert run_probe("slow", {"command": [sys.executable, "-c", "import time; time.sleep(5)"],
                              "timeout_s": 1}) is False
