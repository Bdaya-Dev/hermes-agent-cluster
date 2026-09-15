"""#898 — reviewer hand-off must be submit-once-and-finish; main dedupes
open reviewer tasks per lane_key; cancel must terminate the child process.

Three measured defects (issue + its 10:35Z note, task_4dc179469397acfe):
1. The hand-off text has no idempotency rule: the author lane re-submitted
   its reviewer four times in 36 minutes, cancelling each queued one when
   "it had not picked up". The brief must say: submit exactly once, then
   finish; NEVER cancel or resubmit.
2. Main must keep that promise mechanically: a reviewer submit for a
   lane_key that already has a live (pending/ready/assigned/running)
   reviewer task returns the EXISTING task (idempotent, status 200) —
   a re-submit cannot fork a second queued reviewer.
3. Cancel is bookkeeping only: the spawned hermes process (and its whole
   tree) stayed alive 10+ minutes after the task flipped to cancelled and
   submitted ANOTHER reviewer. The executor's reap cycle must observe the
   cancel and kill the tree.
4. And a cancelled/terminal task must refuse further lifecycle calls from
   the still-live lane (kanban_cluster_complete on it 409s — the /fail ack
   path stays allowed).

Reds are import-clean against main (no ImportError) and pin the NEW
surface with getattr-style presence asserts first, per the fork's
getattr-pin rule.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core import agent_executor as ae
from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig
from hermes_cluster import plugin
from hermes_cluster.models import TaskStatus


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(create_app(cluster_id="t", node_id="t-node", node_role="main"))


def _submit(client, title="review claude-plugins#main MR !935", **kw):
    body = {"title": title, "requires": ["review"], "priority": 0,
            "role": "reviewer", "lane_key": "claude-plugins#main-rev"}
    body.update(kw)
    return client.post("/api/v1/tasks", json=body)


def _executor(tmp_path):
    return AgentExecutor(
        config=AgentExecutorConfig(enabled=True, poll_interval=60,
                                   working_dir=str(tmp_path)),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _brief(tmp_path, role):
    ex = _executor(tmp_path)
    p = ex._write_brief("task_abc", "review the thing", "DETAILS",
                        lane_key="repo#br", role=role)
    return p.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# leg 1 — the author brief carries the submit-once rule (RED at main)
# ---------------------------------------------------------------------------

def test_author_brief_carries_submit_once_rule(tmp_path):
    text = _brief(tmp_path, "author")
    assert "submit" in text.lower()
    # #898: the hand-off must forbid cancel-and-resubmit explicitly.
    assert "submit exactly one reviewer" in text.lower(), (
        "author brief must instruct: submit exactly one reviewer task")
    assert "never cancel" in text.lower().replace("-", " "), (
        "author brief must forbid cancelling/resubmitting the reviewer task")


def test_bundle_brief_carries_submit_once_rule():
    from hermes_cluster.core.intake_grouping import BundlePlan, bundle_brief
    b = BundlePlan(lane_key="claude-plugins#main", iids=[898],
                   project_paths=["shared/claude-plugins"], priority=0,
                   issue_ids=["shared/claude-plugins#898"], reason="test")
    text = bundle_brief(b)
    assert "submit exactly one reviewer" in text.lower(), (
        "the bundled sitting brief carries the same hand-off; #898's loop "
        "was measured on exactly this text")
    assert "never cancel" in text.lower().replace("-", " ")


def test_reviewer_brief_still_has_no_author_handoff(tmp_path):
    # by-design control (already true at main): reviewer lanes must not be
    # told to dispatch reviewers at all.
    text = _brief(tmp_path, "reviewer")
    assert "Author hand-off" not in text


# ---------------------------------------------------------------------------
# leg 2 — main dedupes live reviewer tasks per lane_key (RED at main)
# ---------------------------------------------------------------------------

def test_second_live_reviewer_submit_returns_existing_task(client):
    r1 = _submit(client)
    assert r1.status_code == 200
    t1 = r1.json()
    r2 = _submit(client, title="review claude-plugins#main MR !935 (v2 brief)")
    assert r2.status_code == 200
    t2 = r2.json()
    assert t2["id"] == t1["id"], (
        "a reviewer submit on a lane_key with a LIVE reviewer task must "
        "return the existing task, not fork a second queued reviewer (#898)")
    assert t2["status"] == t1["status"]


def test_completed_reviewer_is_replaced_by_a_fresh_submit(client):
    t1 = _submit(client).json()
    # walk it to completed
    client.post(f"/api/v1/tasks/{t1['id']}/claim",
                json={"node_id": client.post("/api/v1/nodes/join",
                      json={"node_name": "w", "capabilities": ["review"]}).json()["node_id"]})
    # #919: a reviewer completion now requires verdict grammar (the same
    # vocabulary #914's landing gate parses) — a bare "PASS" is exactly the
    # verdict-less shape the gate refuses. The test's subject (terminal
    # predecessor must not block the next hand-off) is unchanged.
    client.post(f"/api/v1/tasks/{t1['id']}/complete",
                json={"result": "Reviewer verdict: PASS\nSHA: deadbeef1234\n"})
    r2 = _submit(client, title="review claude-plugins#main MR !935 round 2")
    assert r2.status_code == 200
    assert r2.json()["id"] != t1["id"], (
        "a terminal previous reviewer must not block the next sitting's hand-off")
    assert r2.json()["status"] in ("pending", "ready")


def test_non_reviewer_submits_are_never_deduped(client):
    """By-design control: the idempotency window is reviewer tasks only."""
    a = client.post("/api/v1/tasks", json={
        "title": "author sitting A", "lane_key": "x#main", "role": "author"}).json()
    b = client.post("/api/v1/tasks", json={
        "title": "author sitting B", "lane_key": "x#main", "role": "author"}).json()
    assert a["id"] != b["id"]


# ---------------------------------------------------------------------------
# leg 3 — executor kills the process tree when main reports the task cancelled
# ---------------------------------------------------------------------------

class _FakeProc:
    def __init__(self):
        self.pid = 4242
        self.terminated = False
        self.killed = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


class _FakePopen:
    def __init__(self):
        self.procs = []

    def __call__(self, *a, **k):
        p = _FakeProc()
        self.procs.append(p)
        return p


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)


def test_reap_kills_cancelled_spawn_and_drops_it(tmp_path, monkeypatch, no_sleep):
    ex = _executor(tmp_path)
    proc = _FakeProc()
    spawn = ae.ActiveSpawn(
        task_id="task_cancel", task_title="t", process=proc,
        started_at=time.time() - 999, lane_name="hermes-task_cancel",
        mode="hermes", result_path=str(tmp_path / "r.md"),
        stdout_path=str(tmp_path / "o.log"), stderr_path=str(tmp_path / "e.log"),
    )
    ex._active_spawns["task_cancel"] = spawn

    def fake_signed(endpoint, method, path, body, token, node_id):
        if path == "/api/v1/tasks/task_cancel":
            return {"id": "task_cancel", "status": "cancelled"}
        return None
    monkeypatch.setattr(ae, "_signed_request", fake_signed)

    ex._reap_finished_spawns()

    assert proc.terminated or proc.killed, (
        "a task cancelled on main must have its child process tree killed "
        "(#898: the cancelled lane kept running and re-submitted reviewers)")
    assert "task_cancel" not in ex._active_spawns, (
        "the cancelled spawn must leave the active set")


def test_reap_leaves_live_spawns_untouched(tmp_path, monkeypatch, no_sleep):
    """By-design control: a still-running task's spawn is not killed."""
    ex = _executor(tmp_path)
    proc = _FakeProc()
    spawn = ae.ActiveSpawn(
        task_id="task_live", task_title="t", process=proc,
        started_at=time.time() - 5, lane_name="hermes-task_live",
        mode="hermes", result_path=str(tmp_path / "r.md"),
        stdout_path=str(tmp_path / "o.log"), stderr_path=str(tmp_path / "e.log"),
    )
    ex._active_spawns["task_live"] = spawn

    def fake_signed(endpoint, method, path, body, token, node_id):
        if path == "/api/v1/tasks/task_live":
            return {"id": "task_live", "status": "running"}
        return None
    monkeypatch.setattr(ae, "_signed_request", fake_signed)

    ex._reap_finished_spawns()
    assert not (proc.terminated or proc.killed)
    assert "task_live" in ex._active_spawns


# ---------------------------------------------------------------------------
# leg 4 — terminal lifecycle refuses the still-live lane; plugin is honest
# ---------------------------------------------------------------------------

def test_complete_after_cancelled_is_refused(client):
    """A cancelled task must refuse further /complete — the still-live lane's
    late post must not resurrect it (#898 note: session kept acting after
    cancel). The two-phase ack path (cancel_requested -> complete ->
    cancelled) stays allowed by design (B2); this pins the terminal end.
    Control: the immediate phase (no lease) -> cancelled is what the
    incident's queued reviewers hit."""
    t = _submit(client).json()  # ready, never claimed -> no lease
    r = client.post(f"/api/v1/tasks/{t['id']}/cancel", json={"reason": "stale"})
    assert r.json()["phase"] == "immediate"
    r2 = client.post(f"/api/v1/tasks/{t['id']}/complete", json={"result": "x"})
    assert r2.status_code == 409, (
        "complete on a cancelled task must be 409 (terminal) — the live "
        "lane's late post cannot resurrect a cancelled reviewer")
    r3 = client.post(f"/api/v1/tasks/{t['id']}/fail", json={"reason": "late"})
    assert r3.status_code == 409


def test_plugin_surfaces_deduped_submit(monkeypatch):
    """kanban_cluster_submit on a deduped response returns the existing id
    (pass-through) and does NOT re-trigger scheduling — the loop's other
    half: even a deduped submit must not fan scheduler churn."""
    calls = []

    def fake_api(method, path, data=None):
        calls.append((method, path))
        if path == "/api/v1/tasks":
            return {"id": "task_existing", "status": "ready", "deduped": True}
        return {"scheduled": 0}

    monkeypatch.setattr(plugin, "_api_call", fake_api)
    import json as _json
    out = _json.loads(plugin.handle_cluster_submit({
        "title": "review claude-plugins#main MR !935 again",
        "requires": ["review"], "priority": 0,
        "role": "reviewer", "lane_key": "claude-plugins#main-rev",
    }))
    assert out["id"] == "task_existing"
    assert out.get("deduped") is True, (
        "a deduped reviewer submit must be visible to the caller lane — "
        "'you already have a reviewer queued; finish your turn'")
    assert ("POST", "/api/v1/schedule/trigger") not in calls, (
        "a DEDUPED submit must not re-trigger the scheduler — the existing "
        "task is already the scheduler's problem, not the author's")
