"""The capability vocabulary is published, and stalls are visible (#907).

`GET /api/v1/capabilities` exists because the fleet published its vocabulary
NOWHERE: a `requires` naming something no node advertises matched nothing, and
produced nothing — no error, no log line, no degraded node — so the task sat
READY forever looking exactly like one waiting its turn. Lanes then invented
names (`merge`, `land`, `flutter`, `invora`, `author`) because there was
nothing to read.

The `stalled` half is the one that earns its keep: it names every capability a
LIVE task is waiting on that no schedulable node can serve. Empty on a healthy
fleet, so non-empty is an alarm — instead of an operator mining 1394 tasks by
hand, which is how bayader-flutter!221's landing task was eventually found
after sitting unclaimable while the cluster ran at 4 of 22 slots.
"""

import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from hermes_cluster.app import create_app

PROBE = str(Path(__file__).resolve().parents[1] / "scripts" / "capability_probe.py")


def _client():
    app = create_app(cluster_id="test-cluster", node_id="node_main", node_role="main")
    return TestClient(app)


def _join(client, name, caps, max_concurrent=4):
    return client.post(
        "/api/v1/nodes/join",
        json={"node_name": name, "capabilities": caps,
              "max_concurrent": max_concurrent},
    ).json()["node_id"]


# ---------------------------------------------------------------------------
# The vocabulary
# ---------------------------------------------------------------------------

def test_vocabulary_reports_every_capability_and_who_has_it():
    client = _client()
    _join(client, "w1", ["tooling", "review"])
    _join(client, "w2", ["tooling", "flutter"])

    body = client.get("/api/v1/capabilities").json()

    assert body["known"] == ["flutter", "review", "tooling"]
    by_cap = {e["capability"]: e for e in body["capabilities"]}
    assert sorted(by_cap["tooling"]["nodes"]) == ["node_w1", "node_w2"]
    assert by_cap["flutter"]["nodes"] == ["node_w2"]
    assert by_cap["review"]["servable"] is True
    assert body["schedulable_node_count"] == 2


def test_free_slots_track_capacity_not_just_presence():
    """A capability everyone is too busy to serve is still servable.

    Reporting a fully-loaded node's capability as absent would turn an ordinary
    queue into a phantom outage — the opposite error from the one this endpoint
    exists to catch, and just as misleading.
    """
    client = _client()
    _join(client, "w1", ["tooling"], max_concurrent=2)

    before = {e["capability"]: e for e in
              client.get("/api/v1/capabilities").json()["capabilities"]}
    assert before["tooling"]["free_slots"] == 2

    client.post("/api/v1/tasks", json={"title": "a", "requires": ["tooling"]})
    client.post("/api/v1/schedule/trigger")

    after = {e["capability"]: e for e in
             client.get("/api/v1/capabilities").json()["capabilities"]}
    assert after["tooling"]["free_slots"] == 1
    assert after["tooling"]["servable"] is True


def test_unlimited_capacity_is_distinguishable_from_full():
    """max_concurrent=0 means unlimited; it must not read as zero free slots."""
    client = _client()
    _join(client, "w1", ["tooling"], max_concurrent=0)

    caps = {e["capability"]: e for e in
            client.get("/api/v1/capabilities").json()["capabilities"]}
    assert caps["tooling"]["free_slots"] == -1
    assert caps["tooling"]["servable"] is True


# ---------------------------------------------------------------------------
# The stall alarm
# ---------------------------------------------------------------------------

def test_stalled_names_a_capability_nobody_serves():
    client = _client()
    _join(client, "w1", ["tooling"])

    # Submitted through the store so this test measures the ALARM, not the
    # submit-time refusal — both guards exist, and a stall can still arrive
    # from an older task, a restored DB, or a node losing a probed capability
    # after the task was accepted.
    import hermes_cluster.routers.tasks as tasks_mod
    tasks_mod._state.create_task("t_stuck", "land !221", ["merge"])
    tasks_mod._state.trigger_pending_tasks()

    body = client.get("/api/v1/capabilities").json()
    stalled = {e["capability"]: e for e in body["stalled"]}
    assert "merge" in stalled, body["stalled"]
    assert stalled["merge"]["task_count"] == 1
    assert stalled["merge"]["task_ids"] == ["t_stuck"]
    assert stalled["merge"]["oldest_created_at"] is not None


def test_stalled_is_EMPTY_on_a_healthy_fleet():
    """The alarm must be quiet when nothing is wrong, or nobody will read it."""
    client = _client()
    _join(client, "w1", ["tooling"])
    client.post("/api/v1/tasks", json={"title": "fine", "requires": ["tooling"]})

    assert client.get("/api/v1/capabilities").json()["stalled"] == []


def test_stalled_ignores_FINISHED_tasks():
    """A completed task that once required an absent capability is history."""
    client = _client()
    _join(client, "w1", ["tooling"])
    import hermes_cluster.routers.tasks as tasks_mod
    tasks_mod._state.create_task("t_old", "old", ["merge"])
    from hermes_cluster.models import TaskStatus
    tasks_mod._state.set_task_status("t_old", TaskStatus.completed)

    assert client.get("/api/v1/capabilities").json()["stalled"] == []


def test_a_capability_only_an_OFFLINE_node_has_is_not_servable():
    """Advertised-but-unreachable is a different remedy from nobody-has-it.

    So the node is still named, under `unavailable_nodes`, rather than the
    capability vanishing — "restart w2" and "install Flutter somewhere" are not
    the same fix, and a bare absence cannot tell them apart.
    """
    client = _client()
    _join(client, "w1", ["tooling"])
    node_id = _join(client, "w2", ["tooling", "flutter"])

    import hermes_cluster.routers.nodes as nodes_mod
    from hermes_cluster.models import NodeStatus
    nodes_mod._state.set_node_status(node_id, NodeStatus.offline)

    body = client.get("/api/v1/capabilities").json()
    by_cap = {e["capability"]: e for e in body["capabilities"]}
    assert by_cap["flutter"]["servable"] is False
    assert by_cap["flutter"]["nodes"] == []
    assert by_cap["flutter"]["unavailable_nodes"] == [node_id]
    assert "flutter" not in body["known"]


# ---------------------------------------------------------------------------
# The probe registry
# ---------------------------------------------------------------------------

def _probe(*args):
    return subprocess.run([sys.executable, PROBE, *args],
                          capture_output=True, text=True, timeout=180)


def test_probe_list_is_the_registry():
    out = _probe("--list")
    assert out.returncode == 0
    names = out.stdout.split()
    assert {"flutter", "dotnet", "gcp", "k8s"} <= set(names)


def test_unknown_capability_fails_CLOSED_and_says_so():
    """A typo must not advertise a capability the node cannot serve.

    Failing open here would recreate the forever-queue the whole mechanism
    exists to prevent, one typo at a time.
    """
    out = _probe("flutterr")
    assert out.returncode == 2
    assert "unknown capability" in out.stderr
    assert "flutter" in out.stderr  # the real vocabulary is offered


def test_credential_probe_output_is_REDACTED():
    """A probe that succeeds by minting a token must never echo the token.

    This guard exists because the first run of `--all` while writing these
    probes printed a live GCP access token to the console. The estate's rule
    ("never print a secret value; assert a length or checksum") is enforced
    here in code so the next probe cannot re-learn it the same way.
    """
    from importlib.machinery import SourceFileLoader
    mod = SourceFileLoader("capability_probe", PROBE).load_module()

    secret = "ya29.THIS-IS-A-FAKE-TOKEN-VALUE"
    ok, detail = mod._run([sys.executable, "-c", f"print({secret!r})"], redact=True)
    assert ok is True
    assert secret not in detail
    assert "bytes" in detail and "sha256:" in detail

    # Unredacted is still the default for version probes — the version string
    # is the useful evidence there, and redacting it would make every probe's
    # report unreadable.
    ok, detail = mod._run([sys.executable, "-c", "print('3.44.0')"])
    assert ok is True and detail == "3.44.0"


def test_failure_output_is_NOT_redacted():
    """A failing credential command prints an error, not a credential — and
    that error is the entire diagnostic value of the probe."""
    from importlib.machinery import SourceFileLoader
    mod = SourceFileLoader("capability_probe", PROBE).load_module()

    ok, detail = mod._run(
        [sys.executable, "-c",
         "import sys; print('you must be logged in', file=sys.stderr); sys.exit(1)"],
        redact=True,
    )
    assert ok is False
    assert "you must be logged in" in detail


def test_missing_binary_is_a_clean_FALSE_not_a_crash():
    from importlib.machinery import SourceFileLoader
    mod = SourceFileLoader("capability_probe", PROBE).load_module()

    ok, detail = mod._run(["definitely-not-a-real-binary-907"])
    assert ok is False
    assert "not on PATH" in detail
