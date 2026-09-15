"""Release-drift app integration tests (#917) — wiring, not logic.

create_app's contract for a default-off node is the CI exit-139 rule the
metering poller already honors: NO background thread, NO network at boot. The
drift poller is gated identically, and the read surface behaves like every
other main router.
"""

import asyncio
import threading

import pytest
from fastapi import HTTPException

from hermes_cluster.app import create_app
from hermes_cluster.routers import release as release_mod
from hermes_cluster.state import ClusterState


def test_default_main_creates_no_drift_thread(tmp_path):
    """A default (release_drift absent) main starts no drift poller thread at
    all — same pinned rule as test_default_main_creates_no_metering_thread."""
    before = {t.name for t in threading.enumerate()}
    create_app(cluster_id="test", node_id="test-node", node_role="main")
    after = {t.name for t in threading.enumerate()}
    assert "release-drift-poller" not in (after - before)
    assert "release-drift-supervisor" not in (after - before)


def test_router_503_without_poller():
    release_mod.set_poller(None)
    with pytest.raises(HTTPException) as e:
        asyncio.run(release_mod.release_drift())
    assert e.value.status_code == 503


def test_router_read_surface_shape():
    from hermes_cluster.core.release_drift_poller import ReleaseDriftPoller
    p = ReleaseDriftPoller(state=ClusterState(), hook_manager=None)
    release_mod.set_poller(p)
    try:
        out = asyncio.run(release_mod.release_drift())
        assert out["enabled"] is False            # default-off
        assert out["deployed_commit"] is None or len(out["deployed_commit"]) == 40
        assert "sample" in out and "last_errors" in out
        assert set(out) >= {"interval_s", "grace_s", "max_age_s",
                            "commit_threshold"}
    finally:
        release_mod.set_poller(None)


def test_seed_from_yaml_file(tmp_path):
    """GitOps seed once (metering/intake precedent): YAML bootstraps an EMPTY
    store; a store that already has the section is NEVER clobbered — the
    runtime PUT surface is authoritative without a redeploy."""
    from hermes_cluster.core.release_drift_poller import _seed_drift_from_config_file
    cfg_file = tmp_path / "cluster.yaml"
    cfg_file.write_text(
        "release_drift:\n  enabled: true\n  interval_s: 600\n"
        "  grace_s: 1800\n  max_age_s: 3600\n  commit_threshold: 2\n")
    st = ClusterState()
    st.set_config_path(str(cfg_file))
    _seed_drift_from_config_file(st)
    assert st.get_config()["release_drift"]["enabled"] is True
    assert st.get_config()["release_drift"]["interval_s"] == 600
    # a second seed with a DIFFERENT file must not clobber runtime authority
    cfg_file.write_text("release_drift:\n  enabled: false\n")
    _seed_drift_from_config_file(st)
    assert st.get_config()["release_drift"]["enabled"] is True


def test_enabled_starts_and_stops_thread_with_network_stubbed(tmp_path):
    """End-to-end wiring without touching the network: boot seed enabled ->
    ensure_started arms the loop; stop() disarms. The fetch stubs mean even a
    racing cycle makes no call out."""
    from hermes_cluster.core.release_drift_poller import (_seed_drift_from_config_file,
                                                          ReleaseDriftPoller)
    cfg_file = tmp_path / "cluster.yaml"
    cfg_file.write_text("release_drift:\n  enabled: true\n  interval_s: 300\n")
    st = ClusterState()
    st.set_config_path(str(cfg_file))
    _seed_drift_from_config_file(st)
    p = ReleaseDriftPoller(state=st, hook_manager=None,
                           fetch_main_head=lambda: None,
                           fetch_compare=lambda pin: None,
                           token_provider=lambda c: None,
                           digest_resolver=lambda *a, **k: {})
    assert p.ensure_started() is True
    assert p._thread is not None and p._thread.is_alive()
    t = p._thread
    p.stop()
    t.join(timeout=5)
    assert not t.is_alive()


def test_disabled_yaml_seed_arms_nothing(tmp_path):
    from hermes_cluster.core.release_drift_poller import (_seed_drift_from_config_file,
                                                          ReleaseDriftPoller)
    cfg_file = tmp_path / "cluster.yaml"
    cfg_file.write_text("release_drift:\n  enabled: false\n")
    st = ClusterState()
    st.set_config_path(str(cfg_file))
    _seed_drift_from_config_file(st)
    p = ReleaseDriftPoller(state=st, hook_manager=None)
    assert p.ensure_started() is False
    assert p._thread is None
