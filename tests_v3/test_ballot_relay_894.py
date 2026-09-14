"""Ballot relay tests (#894) — a lane's decision ballot reaches the owner.

Covers the fork half of shared/claude-plugins#894:

  1. POST /tasks/{id}/block   — a running delivery escalates: the task flips
     to blocked WITH the ballot (question + options + asked_at + class)
     attached, and the worker's lease is released so the lane is resumable.
  2. POST /tasks/{id}/answer  — the owner's answer is stored on the ballot
     and the task unblocks to READY (lane affinity re-dispatches it; the
     executor resumes the SAME session with the answer as its next message).
  3. GET /ballots/pending     — the read surface the gateway notifier polls.
  4. Routing class (owner ruling 2026-09-14, note 138210): every ballot
     carries an explicit class ("product" | "technical", DEFAULT technical)
     so the relay never guesses the destination chat.
  5. Security: /block, /answer and /ballots/pending ride the SAME peer-HMAC
     middleware as submit/cancel (deny-by-default when auth is enabled).
  6. Executor: the escalation side-file turns a clean hermes exit into the
     ``blocked`` report (never ``done``), and an answered ballot is rendered
     into the resumed delivery's brief instead of a fresh brief.
"""

import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.core import peer_auth as peer_auth_mod
from hermes_cluster.core.ballot import BallotError, build_ballot, validate_ballot
from hermes_cluster.models import TaskStatus


# ---------------------------------------------------------------------------
# App fixtures (fresh SQLite state + one with peer auth ON)
# ---------------------------------------------------------------------------

@pytest.fixture
def client(tmp_path):
    app = create_app(cluster_id="t", node_id="main", node_role="main",
                     db_path=str(tmp_path / "ballot.db"))
    with TestClient(app) as c:
        yield c


@pytest.fixture
def authed_client(tmp_path, monkeypatch):
    monkeypatch.setenv("PEER_TOKEN", "a" * 64)
    monkeypatch.setenv("PEER_TOKENS", f"gateway:{'b' * 64}")
    app = create_app(cluster_id="t", node_id="main", node_role="main",
                     db_path=str(tmp_path / "ballot_auth.db"))
    with TestClient(app) as c:
        yield c


def _sign(method, path, body=b"", node="gateway", token=None):
    token = token or ("b" * 64)
    import hashlib
    import hmac as _hmac
    ts = int(time.time())
    digest = hashlib.sha256(body).hexdigest()
    msg = f"{node}:{ts}:{method}:{path}:{digest}"
    sig = _hmac.new(token.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {"X-Peer-Node": node, "X-Peer-Timestamp": str(ts),
            "X-Peer-Signature": sig}


def _state(client):
    """The ClusterState the tasks router handlers operate on."""
    from hermes_cluster.routers import tasks as tasks_mod
    return tasks_mod._state


def _make_running(client, lane_key="repo#feat/x"):
    """Create + assign a task so it sits in `running` with a lease-free row
    (the block endpoint mirrors cancel's lease handling: it releases any
    active lease, so a lease-less running row is the common test shape)."""
    r = client.post("/api/v1/tasks", json={"title": "do work", "lane_key": lane_key})
    assert r.status_code == 200
    tid = r.json()["id"]
    # join a worker and move the task to running like the scheduler would
    jr = client.post("/api/v1/nodes/join", json={"node_name": "w", "capabilities": []})
    node_id = jr.json()["node_id"]
    st = _state(client)
    st.set_task_status(tid, TaskStatus.running)
    # pin assigned_to (the SQLite store has no _tasks dict — patch the row)
    if hasattr(st, "_tasks"):
        with st._tasks_lock:
            st._tasks[tid].assigned_to = node_id
    elif hasattr(st, "_conn"):
        st._conn.execute("UPDATE tasks SET assigned_to = ? WHERE id = ?",
                         (node_id, tid))
        st._conn.commit()
    return tid


# ---------------------------------------------------------------------------
# 1. ballot model helpers
# ---------------------------------------------------------------------------

class TestBallotHelpers:
    def test_build_ballot_defaults_technical(self):
        b = build_ballot("Which DB?", ["pg", "sqlite"])
        assert b["question"] == "Which DB?"
        assert b["options"] == ["pg", "sqlite"]
        assert b["class"] == "technical"          # owner ruling: default
        assert b["asked_at"]
        assert b["answer"] is None

    def test_product_class_is_explicit(self):
        b = build_ballot("Ship v1?", ["yes", "no"], cls="product")
        assert b["class"] == "product"

    def test_bad_class_rejected(self):
        with pytest.raises(BallotError):
            build_ballot("q", ["a"], cls="vibes")

    def test_missing_question_rejected(self):
        with pytest.raises(BallotError):
            build_ballot("", ["a"])

    def test_options_must_be_string_list(self):
        with pytest.raises(BallotError):
            validate_ballot({"question": "q", "options": [1, 2], "class": "technical"})


# ---------------------------------------------------------------------------
# 2. POST /block — escalation attaches the ballot and flips to blocked
# ---------------------------------------------------------------------------

class TestBlockEndpoint:
    def test_block_running_task_attaches_ballot(self, client):
        tid = _make_running(client)
        r = client.post(f"/api/v1/tasks/{tid}/block", json={
            "question": "Merge as Draft or mark ready?",
            "options": ["Draft", "Ready"],
        })
        assert r.status_code == 200, r.text
        task = client.get(f"/api/v1/tasks/{tid}").json()
        assert task["status"] == "blocked"
        b = task["ballot"]
        assert b["question"] == "Merge as Draft or mark ready?"
        assert b["options"] == ["Draft", "Ready"]
        assert b["class"] == "technical"
        assert b["asked_at"] and b["answer"] is None

    def test_block_accepts_product_class(self, client):
        tid = _make_running(client)
        r = client.post(f"/api/v1/tasks/{tid}/block", json={
            "question": "Price at 19 or 29 SAR?", "options": ["19", "29"],
            "class": "product",
        })
        assert r.status_code == 200
        assert client.get(f"/api/v1/tasks/{tid}").json()["ballot"]["class"] == "product"

    def test_block_invalid_ballot_422(self, client):
        tid = _make_running(client)
        r = client.post(f"/api/v1/tasks/{tid}/block", json={"question": "", "options": []})
        assert r.status_code == 422

    def test_block_terminal_409(self, client):
        r = client.post("/api/v1/tasks", json={"title": "x"})
        tid = r.json()["id"]
        client.post(f"/api/v1/tasks/{tid}/complete")
        r = client.post(f"/api/v1/tasks/{tid}/block",
                        json={"question": "q?", "options": ["a", "b"]})
        assert r.status_code == 409

    def test_block_releases_active_lease(self, client):
        """The escalating worker's lease must be revoked: the block is the
        end of THAT delivery (the lane parks; the answer re-dispatches it)."""
        tid = _make_running(client)
        lr = client.post("/api/v1/leases", json={"task_id": tid, "node_id": "w",
                                                 "ttl_seconds": 60})
        assert lr.status_code in (200, 201), lr.text
        r = client.post(f"/api/v1/tasks/{tid}/block",
                        json={"question": "q?", "options": ["a", "b"]})
        assert r.status_code == 200
        leases = client.get("/api/v1/leases").json()
        active = [l for l in (leases if isinstance(leases, list)
                              else leases.get("leases", []))
                  if l.get("task_id") == tid and l.get("status") == "active"]
        assert active == []


# ---------------------------------------------------------------------------
# 3. POST /answer — stores the answer and unblocks to ready
# ---------------------------------------------------------------------------

class TestAnswerEndpoint:
    def test_answer_unblocks_to_ready_and_stores(self, client):
        tid = _make_running(client)
        client.post(f"/api/v1/tasks/{tid}/block",
                    json={"question": "q?", "options": ["a", "b"]})
        r = client.post(f"/api/v1/tasks/{tid}/answer",
                        json={"answer": "b", "answered_by": "921626919"})
        assert r.status_code == 200, r.text
        task = client.get(f"/api/v1/tasks/{tid}").json()
        assert task["status"] == "ready"          # re-dispatchable NOW
        assert task["ballot"]["answer"] == "b"
        assert task["ballot"]["answered_by"] == "921626919"
        assert task["ballot"]["answered_at"]

    def test_answer_without_ballot_409(self, client):
        tid = _make_running(client)
        r = client.post(f"/api/v1/tasks/{tid}/answer", json={"answer": "x"})
        assert r.status_code == 409

    def test_answer_on_unblocked_task_409(self, client):
        r = client.post("/api/v1/tasks", json={"title": "x"})
        tid = r.json()["id"]
        rr = client.post(f"/api/v1/tasks/{tid}/answer", json={"answer": "x"})
        assert rr.status_code == 409

    def test_second_answer_rejected(self, client):
        """A ballot answers once: a late second tap cannot flip the task
        back or stomp the recorded decision."""
        tid = _make_running(client)
        client.post(f"/api/v1/tasks/{tid}/block",
                    json={"question": "q?", "options": ["a", "b"]})
        assert client.post(f"/api/v1/tasks/{tid}/answer",
                           json={"answer": "a"}).status_code == 200
        r = client.post(f"/api/v1/tasks/{tid}/answer", json={"answer": "b"})
        assert r.status_code == 409

    def test_empty_answer_422(self, client):
        tid = _make_running(client)
        client.post(f"/api/v1/tasks/{tid}/block",
                    json={"question": "q?", "options": ["a", "b"]})
        r = client.post(f"/api/v1/tasks/{tid}/answer", json={"answer": "  "})
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# 4. GET /ballots/pending — the notifier's read surface
# ---------------------------------------------------------------------------

class TestPendingBallots:
    def test_lists_only_unanswered_blockeds(self, client):
        t1 = _make_running(client, lane_key="repo#a")
        t2 = _make_running(client, lane_key="repo#b")
        client.post(f"/api/v1/tasks/{t1}/block",
                    json={"question": "one?", "options": ["a", "b"], "class": "product"})
        client.post(f"/api/v1/tasks/{t2}/block",
                    json={"question": "two?", "options": ["c"]})
        client.post(f"/api/v1/tasks/{t2}/answer", json={"answer": "c"})

        r = client.get("/api/v1/ballots/pending")
        assert r.status_code == 200
        items = r.json()["ballots"]
        assert [i["task_id"] for i in items] == [t1]
        item = items[0]
        assert item["question"] == "one?"
        assert item["class"] == "product"
        assert item["lane_key"] == "repo#a"
        assert item["options"] == ["a", "b"]


# ---------------------------------------------------------------------------
# 5. Security: the new endpoints ride the peer-HMAC middleware
# ---------------------------------------------------------------------------

class TestPeerAuthSurface:
    """Deny-by-default (M2) says everything not PUBLIC is peer-signed. These
    tests pin that for /block, /answer and /ballots/pending — the answer
    endpoint MUST be exactly as privileged as submit/cancel (issue #894:
    'the answer endpoint rides the same 3-leg action gate + peer HMAC')."""

    def test_unsigned_write_and_read_denied(self, authed_client):
        # create signed so a task exists
        body = json.dumps({"title": "x"}).encode()
        h = _sign("POST", "/api/v1/tasks", body)
        r = authed_client.post("/api/v1/tasks", content=body, headers={**h, "Content-Type": "application/json"})
        assert r.status_code == 200
        tid = r.json()["id"]
        assert authed_client.post(f"/api/v1/tasks/{tid}/block",
                                  json={"question": "q?", "options": ["a"]}).status_code == 401
        assert authed_client.post(f"/api/v1/tasks/{tid}/answer",
                                  json={"answer": "a"}).status_code == 401
        assert authed_client.get("/api/v1/ballots/pending").status_code == 401

    def test_signed_block_and_answer_pass(self, authed_client):
        body = json.dumps({"title": "x", "lane_key": "l"}).encode()
        h = _sign("POST", "/api/v1/tasks", body)
        r = authed_client.post("/api/v1/tasks", content=body,
                               headers={**h, "Content-Type": "application/json"})
        assert r.status_code == 200
        tid = r.json()["id"]
        # put it in running directly (no HTTP needed; same process state)
        st = _state(authed_client)
        st.set_task_status(tid, TaskStatus.running)

        bbody = json.dumps({"question": "q?", "options": ["a", "b"]}).encode()
        r = authed_client.post(f"/api/v1/tasks/{tid}/block", content=bbody,
                               headers={**_sign("POST", f"/api/v1/tasks/{tid}/block", bbody),
                                        "Content-Type": "application/json"})
        assert r.status_code == 200, r.text
        abody = json.dumps({"answer": "a", "answered_by": "owner"}).encode()
        r = authed_client.post(f"/api/v1/tasks/{tid}/answer", content=abody,
                               headers={**_sign("POST", f"/api/v1/tasks/{tid}/answer", abody),
                                        "Content-Type": "application/json"})
        assert r.status_code == 200, r.text
        r = authed_client.get("/api/v1/ballots/pending",
                              headers=_sign("GET", "/api/v1/ballots/pending"))
        assert r.status_code == 200 and r.json()["ballots"] == []


# ---------------------------------------------------------------------------
# 6. Executor: escalation side-file -> blocked report; answer -> resume brief
# ---------------------------------------------------------------------------

def _executor_cfg(tmp_path):
    from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig
    cfg = AgentExecutorConfig(enabled=True, worker="hermes",
                              working_dir=str(tmp_path), poll_interval=9999)
    ex = AgentExecutor(cfg, node_id="w1", cluster_endpoint="http://main.invalid",
                       peer_token="t" * 64)
    return ex


class _FakeResp:
    def __init__(self, payload): self._p = payload
    def read(self): return json.dumps(self._p).encode()


class TestExecutorEscalation:
    def test_escalation_sidefile_makes_blocked_not_done(self, tmp_path, monkeypatch):
        ex = _executor_cfg(tmp_path)
        calls = []
        monkeypatch.setattr("hermes_cluster.core.agent_executor._signed_request",
                            lambda ep, m, p, d, t, n, **k: (calls.append((m, p, d)) or {"ok": True}))

        spawn = _mk_spawn(tmp_path, ex)
        # Lane exits 0 with a real deliverable AND an escalation side-file
        Path(spawn.result_path).write_text("I need a decision to proceed.", encoding="utf-8")
        Path(spawn.escalation_path).write_text(json.dumps({
            "question": "Keep the legacy column?", "options": ["keep", "drop"],
            "class": "technical"}), encoding="utf-8")

        resolved = []
        spawn.process.poll = lambda: 0
        ex._reap_hermes_spawn(spawn.task_id, spawn, 1.0, resolved)
        assert resolved and resolved[0][2] == "blocked", resolved
        # report path: POST /block with the ballot attached
        tid = spawn.task_id
        ballot = json.loads(Path(spawn.escalation_path).read_text(encoding="utf-8"))
        assert ex._report_blocked(tid, ballot)
        assert any(c[1] == f"/api/v1/tasks/{tid}/block" and
                   c[2]["question"] == "Keep the legacy column?" for c in calls), calls
        # the session row is KEPT (the lane must resume on answer) — no lane drop
        assert not any("lanes" in (c[1] or "") for c in calls), calls

    def test_no_sidefile_behaves_exactly_as_before(self, tmp_path, monkeypatch):
        ex = _executor_cfg(tmp_path)
        monkeypatch.setattr("hermes_cluster.core.agent_executor._signed_request",
                            lambda *a, **k: {})
        spawn = _mk_spawn(tmp_path, ex)
        Path(spawn.result_path).write_text("plain deliverable", encoding="utf-8")
        resolved = []
        spawn.process.poll = lambda: 0
        ex._reap_hermes_spawn(spawn.task_id, spawn, 1.0, resolved)
        assert resolved[0][2] == "done"

    def test_resume_brief_carries_the_answer(self, tmp_path):
        """The next delivery on an ANSWERED ballot reads as: the owner's
        answer IS the next message into the same session."""
        ex = _executor_cfg(tmp_path)
        ballot = {"question": "Keep the legacy column?", "options": ["keep", "drop"],
                  "class": "technical", "asked_at": "now",
                  "answer": "drop", "answered_by": "921626919", "answered_at": "later"}
        p = ex._write_brief("t9", "do work", "", lane_key="lane/x", role="author",
                            deliverable_path=str(tmp_path / "r.md"), ballot=ballot)
        text = p.read_text(encoding="utf-8")
        assert "Keep the legacy column?" in text
        assert "drop" in text
        assert "decision-answer delivery" in text.lower() or "ANSWERED" in text


def _mk_spawn(tmp_path, ex):
    from hermes_cluster.core.agent_executor import ActiveSpawn
    class _P:
        pid = 4242
        def poll(self): return None
    result_path = tmp_path / "hermes-results" / "task_escal.result.md"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    spawn = ActiveSpawn(task_id="task_escal", task_title="t", process=_P(),
                        mode="hermes", lane_key="lane/escal",
                        result_path=str(result_path),
                        stdout_path=str(result_path) + ".out",
                        stderr_path=str(result_path) + ".err",
                        escalation_path=str(result_path) + ".escalation.json",
                        session_id="sess-1")
    return spawn
