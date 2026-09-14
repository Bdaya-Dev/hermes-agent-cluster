"""#867 E2E: a task requiring github-write is NOT scheduled onto a node whose
probe fails, and IS scheduled once the probe passes — the dispatch-time
loudness that replaces the silent mid-lane failure #867 records.

Drives the real scheduler + node manager with a mocked probe: no network, no
credential. run_probe is monkeypatched; everything else is production code.
"""
from hermes_cluster.core import worker_connector as wc
from hermes_cluster.core.node_manager import NodeManager
from hermes_cluster.core.scheduler import FairScheduler, node_can_run
from hermes_cluster.state import ClusterState


def _main():
    state = ClusterState()
    state.cluster_id = "probe-test"
    nm = NodeManager(state)
    return state, nm


def test_missing_probe_capability_blocks_and_unblocks_dispatch(monkeypatch):
    # probe gate: false -> true, exactly the refresh-after-provision shape
    gate = {"ok": False}
    monkeypatch.setattr(wc, "run_probe", lambda name, spec: gate["ok"])

    static = ["native", "tooling"]
    probes = {"github-write": {"command": ["anything"]}}

    state, nm = _main()
    # Simulate the connector's declared-caps maths for two probe outcomes:
    states_fail = {name: wc.run_probe(name, spec) for name, spec in probes.items()}
    caps_no_write = wc.declared_capabilities(static, probes, states_fail)
    assert "github-write" not in caps_no_write

    gate["ok"] = True
    states_pass = {name: wc.run_probe(name, spec) for name, spec in probes.items()}
    caps_write = wc.declared_capabilities(static, probes, states_pass)
    assert "github-write" in caps_write

    # join as the PROVISIONED node would (declares the gated cap)
    node = nm.join("node_a", "worker-a", capabilities=caps_write)
    nm.join("node_b", "worker-b", capabilities=caps_no_write)  # unprovisioned twin

    sched = FairScheduler()
    pick = sched.choose(["github-write"], nm.online_nodes(), {})
    assert pick is not None and pick.id == "node_a", \
        "github-write task must land on the node whose probe passes"

    # unprovisioned twin proves the negative: same probe logic, no cap -> not eligible
    assert not node_can_run(["github-write"], nm.get_node("node_b"))

    # and once its key is provisioned mid-life (the #788 shape), a capability
    # PATCH (what the connector loop now sends) flips eligibility:
    nm.update_capabilities("node_b", caps_write)
    assert node_can_run(["github-write"], nm.get_node("node_b"))
