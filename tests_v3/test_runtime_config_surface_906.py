"""#906 — hosted main runtime-config surface: two defects.

1. PUT /api/v1/config returned 500 on the read-only GitOps ConfigMap mount
   AFTER the store write succeeded — the handler treated the YAML mirror as
   authoritative when the runtime store is (post-boot the file is
   seed-once and /etc/hermes/cluster.yaml is EROFS by design). Operators saw
   a loud failure for a change that HAD persisted, and retried/rolled back.

2. Enabling metering at runtime never started the poller without a pod
   restart: the ONLY arming path was the PUT /config post-save callback —
   which sat behind the raise of defect 1 (so on the hosted main it never
   ran) and covered only writes made THROUGH that endpoint. A store-side
   flip left `poller_running: false` forever. The loop cannot simply
   "always run and gate the fetch": CI run 3488... exit-139 pinned
   test_default_main_creates_no_metering_thread — a disabled main starts NO
   thread. Fix: a lightweight supervisor tick that starts the loop when the
   runtime config turns enabled and stops it when off — started on APP
   startup (uvicorn lifespan), never inside bare create_app(), so test/CI
   processes stay daemon-free.

Acceptance (issue text, verbatim targets):
  (a) PUT /config with an unwritable config path returns 200 and the store
      holds the new value (RED today: 500).
  (b) enabling metering via the store while the poller is stopped results
      in poller_running: true within one supervisor tick, no restart
      (RED today: no supervisor exists).
"""

import threading
import time

import pytest
import httpx
from httpx import ASGITransport

from hermes_cluster.app import create_app
from hermes_cluster.core.metering import MeteringPoller
from hermes_cluster.state import ClusterState


def _full_config() -> dict:
    """A PUT-able full config: create_app's GET shape (validation requires
    cluster.id + node.id)."""
    return {
        "cluster": {"id": "c906", "role": "main"},
        "node": {"id": "n906"},
        "server": {"port": 8787},
    }


# ---------------------------------------------------------------------------
# (a) PUT /config survives an unwritable config FILE with 200 + stored value
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_config_unwritable_path_returns_200(tmp_path):
    """tmp_path IS a directory — open(path, "w") raises IsADirectoryError,
    the same OSError family as EROFS on the ConfigMap mount (the code path
    is one `except OSError`), cross-platform deterministic."""
    app = create_app(cluster_id="c", node_id="n", node_role="main",
                     config_path=str(tmp_path))
    import hermes_cluster.routers.metering as mrouter
    poller = mrouter._poller
    poller.poll_once = lambda: {"ok": True, "enabled": True,
                                "interval_seconds": 3600}
    try:
        async with httpx.AsyncClient(transport=ASGITransport(app=app),
                                     base_url="http://t") as c:
            cfg = _full_config()
            cfg["metering"] = {"enabled": True}
            r = await c.put("/api/v1/config", json=cfg)
            # RED today: 500 {"detail": "failed to save config to ..."} even
            # though the store write ALREADY succeeded.
            assert r.status_code == 200, (
                "PUT /config 500s on an unwritable YAML mirror after the store "
                f"write succeeded (#906 defect 1): {r.text[:200]}"
            )
            # The persisted view must show the change — the store is authoritative.
            r2 = await c.get("/api/v1/config")
            assert r2.status_code == 200
            assert (r2.json().get("metering") or {}).get("enabled") is True
    finally:
        poller.stop()


@pytest.mark.asyncio
async def test_put_config_unwritable_path_arms_metering(tmp_path):
    """The two defects compound: the post-save callback that arms the metering
    poller sat BEHIND the 500 raise, so on the hosted main enabling
    metering via PUT never started the poller. With the write best-effort,
    the callback fires and the poller is live."""
    app = create_app(cluster_id="c", node_id="n", node_role="main",
                     config_path=str(tmp_path))
    import hermes_cluster.routers.metering as mrouter
    poller = mrouter._poller
    assert poller is not None and poller._thread is None
    # neutralise the fetch body — arming is what's asserted, not the API call
    poller.poll_once = lambda: {"ok": True, "enabled": True,
                                "interval_seconds": 3600}
    try:
        async with httpx.AsyncClient(transport=ASGITransport(app=app),
                                     base_url="http://t") as c:
            cfg = _full_config()
            cfg["metering"] = {"enabled": True}
            r = await c.put("/api/v1/config", json=cfg)
            assert r.status_code == 200
        assert poller._thread is not None and poller._thread.is_alive(), (
            "PUT /config enabled metering but the poller never armed — the "
            "post-save callback is unreachable behind the YAML 500 (#906)"
        )
    finally:
        poller.stop()


# ---------------------------------------------------------------------------
# (b) store-side enablement is honored within one supervisor tick
# ---------------------------------------------------------------------------

def _wait_until(predicate, timeout=3.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def test_supervisor_starts_poller_on_store_enable():
    state = ClusterState()
    state.set_config({"metering": {"enabled": False}})
    poller = MeteringPoller(state=state)
    poller.poll_once = lambda: {"ok": True, "enabled": True,
                                "interval_seconds": 3600}
    try:
        # The supervisor is a SEPARATE, cheap ticker; the fetch loop itself
        # must stay absent while disabled (the exit-139 CI rule).
        poller.start_supervisor(interval_s=0.05)
        assert poller._thread is None, (
            "supervisor must not start the fetch loop while disabled")

        # store-side flip — no PUT, no restart:
        state.set_config({"metering": {"enabled": True}})
        _wait_until(lambda: poller.status()["poller_running"],
                    what="supervisor to arm the poller after a store-side "
                         "enable (#906 defect 2)")

        # and flipping it back off disarms without a restart:
        state.set_config({"metering": {"enabled": False}})
        _wait_until(lambda: not poller.status()["poller_running"],
                    what="supervisor to stop the poller after a store-side "
                         "disable (#906: 'stops it when off')")
    finally:
        poller.stop_supervisor()
        poller.stop()


def test_create_app_starts_no_supervisor_thread():
    """The supervisor must NOT exist in bare create_app() processes — CI and
    unit tests build apps constantly; the exit-139 rule ('a disabled main
    starts NO thread') extends to the supervisor. It rides the app startup
    event (uvicorn lifespan), not import/creation."""
    app = create_app(cluster_id="c", node_id="n", node_role="main")
    time.sleep(0.1)  # any leaked ticker would have registered by now
    names = [t.name for t in threading.enumerate()]
    assert not any("metering" in n for n in names), (
        f"metering thread(s) spawned by bare create_app: {names}")
    del app
