"""Regression tests for shared/claude-plugins#870 — a lane that never ran is
recorded COMPLETED because the reap treats ANY non-whitespace result.md as a
deliverable (`contents.strip()` is the whole success gate at
agent_executor.py:1216-1231 on main 7e2a9cba0).

Three production specimens, all reported `completed`, none of which ran:

  1+2. a 137-B provider-error body (byte-exact from the upstream generator:
       ``f"API call failed after {n} retries: {summary}"`` in
       hermes-agent agent/turn_recovery.py, summary from
       agent/api_error_summary.py's offline hint),
  3.   the !908 review lane: 1375 B whose tail is its OWN dispatched brief
       echoed back (the lane crashed before producing a turn; the query echo
       plus a truncation boundary is all result.md ever held), with the
       stderr signature ``Session <id> found but has no messages.
       Starting fresh.`` — hermes_cli/cli_agent_setup_mixin.py:470, emitted
       only when a RESUMED session restored zero messages.

The guard rejects both shapes at reap and re-queues under a retry cap.

False-positive constraint (the real design risk): a legitimate deliverable may
QUOTE its brief, and a legitimate deliverable may contain the word "error".
The echo detector therefore fires only when the WHOLE whitespace/case-flattened
body is contained in the dispatched brief (allowing <=16 normalised chars of
cut junk at each end for the observed mid-word truncation, and >=120
normalised chars overall). Quoting cannot pass that test: a quote sandwiches
foreign prose around the quoted lines, so the body as a whole appears nowhere
in the brief; an echo has no foreign prose anywhere. The provider detector is
anchored at the body start and requires the full upstream sentence shape,
never the bare word "error". Guards below include MUST-pass fixtures for all
three traps: quoting deliverable, error-word prose, and a short all-brief
fragment under the length floor.

Every test in this file runs on every platform — none of the three production
failures was OS-specific (contrast: #868's rename-mechanic guard).

The tests are import-clean against main: the new detector module is imported
INSIDE the tests that pin it, so a missing detector shows up as the defect's
assertion (a reap that resolved 'done'), never as an ImportError cascade.
"""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
)


# ---------------------------------------------------------------------------
# Fixtures: the production specimens, byte-exact
# ---------------------------------------------------------------------------

def provider_error_body() -> str:
    """Byte-exact provider/transport failure an agent's -Q stdout yields when
    the turn dies in max_retries (hermes-agent
    agent/turn_recovery.py:851 + api_error_summary offline hint)."""
    return (
        "API call failed after 3 retries: Hermes can't reach the model "
        "provider. You may be offline. Check your internet connection and "
        "try again."
    )


def dispatched_brief(task_id: str = "task_e602d752d1214559") -> str:
    """The !908 reviewer lane's dispatched brief (exact text captured
    on-disk at hermes-briefs/task_e602d752d1214559.md, macbook_worker)."""
    return (
        "## Hermes cluster task " + task_id + "\n"
        "\n"
        "**Title:** REVIEW-ONLY, fresh reviewer lane - **shared/claude-plugins "
        "MR !908** at head `951637f9`, branch `feat/869-alibaba-pool`. Refs #869 #854.\n"
        "\n"
        "Read the MR body's live-proof transcript before judging. WHAT THIS SERVES, "
        "in the owner's own words: *'make sure that if we buy additional alibaba "
        "seats that they would be easily get added to the pool if i just provide you "
        "with the API key'*. So the acceptance question is not 'is the YAML tidy' - "
        "it is **can a human add a seat with nothing but a key, and does traffic "
        "actually move to it when the first seat is exhausted.**\n"
        "\n"
        "Judge against that. **THE AUTHOR DEVIATED FROM ITS BRIEF, DELIBERATELY AND "
        "ON EVIDENCE - THIS IS THE CRUX.** The brief specified a `providers.<key>` "
        "DICT shape derived from the #869 research; the MR implements a list per "
        "provider with an env-var override. The config loader normalises both, and "
        "the pool probe proves traffic lands on the second seat only when the first "
        "is exhausted. The heuristic is documented as `exhausted -> failover` - "
        "failover is the ONLY trigger. Say whether the "
        "shipped config actually guarantees fill-first behaviour rather than "
        "round-robin. VERDICT VOCABULARY: PASS = no defect that should block merge. "
        "NEEDS-CHANGES = a specific defect you name. 'Never approve or merge' "
        "constrains your ACTIONS, not your verdict word. Unsure about one thing? "
        "PASS and name the caveat. Post the verdict as a note on !908, read it back, "
        "and quote its id. Write the verdict to your result file too, first lines: "
        "'Reviewer verdict: ...' then 'SHA: 951637f9'. NEVER approve or merge.\n"
        "\n"
        "**Stateful lane:** `claude-plugins!908` (**reviewer** role)\n"
        "You are a continuing lane: this task is the next delivery in an existing "
        "lane session. Read the lane's prior context (you are resuming it), and "
        "answer THIS brief as the next message. Keep the lane's thread and "
        "decisions consistent.\n"
        "\n"
        "### Standing lane rules\n"
        "- You are a headless worker spawned by the Hermes cluster executor on "
        "node `macbook_worker`; report blockers in your RETURN VALUE, never "
        "AskUserQuestion.\n"
        "- NEVER approve or merge your own work; open MRs as Draft and hand off "
        "for independent review.\n"
        "- Cheap models only; never print a secret value.\n"
        "- When done, state exactly what you produced (files, MR links, proof) "
        "in your final message.\n"
    )


def echo_specimen() -> str:
    """task_e602d752d1214559.result.md captured on-disk: the brief's tail
    (running to its exact end) with a ONE-character head misalignment — the
    real specimen starts with '` is the ONLY trigger' where the brief reads
    'failover is the ONLY trigger': the echoer's cut landed one char short.
    Exact-slice tests miss this; the trim-budget containment catches it."""
    brief = dispatched_brief()
    idx = brief.index(" is the ONLY trigger")
    return "`" + brief[idx:]  # stray '`' at a position the brief does not have


def good_deliverable_quoting_brief() -> str:
    """A REAL reviewer verdict that quotes its brief (two verbatim lines) and
    must still pass every guard — the false-positive constraint made test."""
    b = dispatched_brief()
    quote1 = "Cheap models only; never print a secret value."
    quote2 = ("You are a continuing lane: this task is the next delivery in an "
              "existing lane session.")
    assert quote1 in b and quote2 in b  # fixture honesty
    return (
        "Reviewer verdict: PASS\n"
        "SHA: 951637f9\n"
        "\n"
        "## Findings\n"
        "The pool probe I ran shows traffic moves to the second seat within one\n"
        "request once the first seat's key is rate-exhausted — the acceptance\n"
        "question the brief asks is answered YES by evidence, not by reading YAML:\n"
        "I dispatched 40 chat completions against the live pool with seat A's key\n"
        "spent and observed 38/40 land on seat B (2 transient 503s retried clean).\n"
        "\n"
        f"The brief said: \"{quote1}\" — applying that vocabulary, I found no\n"
        "defect that should block merge. The list-per-provider shape the author\n"
        "chose normalises identically to the dict shape the research suggested,\n"
        "and the loader's `_merge_config` keeps env overrides winning; deviation\n"
        "was on evidence and I verified the evidence.\n"
        "\n"
        f"Quoting the lane contract: {quote2}\n"
        "\n"
        "Caveat (named, per the brief): fill-first is guaranteed by ordering in\n"
        "_pick_seat(), not by an explicit mutex; under concurrent dispatch two\n"
        "workers can observe the same exhausted snapshot and both hop — worth a\n"
        "follow-up, not a blocker. Verdict posted on !908 (note id 135299),\n"
        "read back. NEVER approved or merged.\n"
    )


# ---------------------------------------------------------------------------
# helpers (same shape as the #868 test file's harness)
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """Fresh main-side app (in-memory store) — mirrors tests_v3/test_agent_executor.py."""
    from fastapi.testclient import TestClient
    from hermes_cluster.app import create_app
    app = create_app(cluster_id="test-cluster-870", node_id="test-node",
                     node_role="main")
    return TestClient(app)


def _register_node(client, name="worker-870"):
    resp = client.post("/api/v1/nodes/join", json={
        "node_name": name,
        "capabilities": ["tooling"],
    })
    return resp.json()["node_id"]


def _submit_task(client, title="Test task", requires=None, priority=0):
    resp = client.post("/api/v1/tasks", json={
        "title": title,
        "requires": requires or [],
        "priority": priority,
    })
    return resp.json()


def _executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60, worker="hermes")
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _hermes_spawn(task_id: str, result_path: Path, stderr_path: Path = None,
                  **kw) -> ActiveSpawn:
    mock_proc = MagicMock()
    mock_proc.poll.return_value = 0  # clean exit
    mock_proc.pid = 4242
    mock_proc.stderr = None
    mock_proc.stdout = None
    return ActiveSpawn(
        task_id=task_id,
        task_title=f"task {task_id}",
        process=mock_proc,
        lease_id=f"lease_{task_id}",
        started_at=time.time() - 60.0,
        lane_name=f"hermes-{task_id}",
        mode="hermes",
        result_path=str(result_path),
        stderr_path=str(stderr_path) if stderr_path else "",
        **kw,
    )


def _reap(executor, spawn, elapsed=60.0):
    resolved = []
    executor._reap_hermes_spawn(spawn.task_id, spawn, elapsed, resolved)
    return resolved


# ---------------------------------------------------------------------------
# 1. The unit oracle: the classifier itself (mutation target)
# ---------------------------------------------------------------------------

def test_provider_error_shape_classified():
    from hermes_cluster.core.deliverable_guard import classify_non_deliverable
    reason = classify_non_deliverable(
        provider_error_body(), dispatched_brief("task_931baa3968bbb088"), "")
    assert reason is not None, (
        "#870: a 137-B provider-error body must not read as a deliverable"
    )
    assert "provider" in reason.lower() or "transport" in reason.lower()


def test_echo_specimen_classified():
    from hermes_cluster.core.deliverable_guard import classify_non_deliverable
    reason = classify_non_deliverable(
        echo_specimen(), dispatched_brief(), "")
    assert reason is not None, (
        "#870: a result that is the dispatched brief echoed back must not "
        "read as a deliverable"
    )
    assert "brief" in reason.lower() or "echo" in reason.lower()


def test_real_deliverable_never_classified():
    """FALSE-POSITIVE GUARD: a genuine verdict that quotes its brief verbatim
    (two lines) must pass — rejecting real work is worse than the bug."""
    from hermes_cluster.core.deliverable_guard import classify_non_deliverable
    reason = classify_non_deliverable(
        good_deliverable_quoting_brief(), dispatched_brief(),
        "Session 20260912_013519_45b697 found but has no messages. Starting fresh.\n"
        "session_id: 20260912_013519_45b697\n")
    assert reason is None, (
        f"#870 false-positive: legitimate deliverable rejected: {reason!r}"
    )


def test_error_word_alone_is_not_a_provider_shape():
    """FALSE-POSITIVE GUARD: 'error' as a vocabulary word is not a transport
    failure; only the anchored upstream sentence shape is."""
    from hermes_cluster.core.deliverable_guard import classify_non_deliverable
    body = ("Reviewer verdict: PASS. The MR originally errored on seat pickup; "
            "the author fixed it — there was an API call failed path in the "
            "tests that no longer triggers. All good.\n")
    reason = classify_non_deliverable(body, dispatched_brief(), "")
    # must not match provider shape; also not an echo (mixed content)
    assert reason is None, f"legitimate content rejected: {reason!r}"
    assert "API call failed" in body  # fixture honesty: it mentions the phrase


def test_short_contained_answer_not_echoed():
    """FALSE-POSITIVE GUARD for the length floor: a SHORT body that is
    entirely brief text (a contiguous run starting at the lane header, under
    the 120-normalised-char echo floor) is deliberately NOT judged an echo —
    containment of a two-line answer proves nothing, and the guard's job is
    the production shape (the bulk-of-a-brief echo)."""
    from hermes_cluster.core.deliverable_guard import classify_non_deliverable
    b = dispatched_brief()
    start = b.index("**Stateful lane:**")
    end = start + 130  # spans two brief lines; 112 normalised chars < 120 floor
    body = b[start:end]
    assert "You are a continuing lane" in body  # fixture honesty: contiguous
    assert classify_non_deliverable(body, b) is None


def test_no_turn_stderr_detected():
    from hermes_cluster.core.deliverable_guard import has_no_turn_stderr
    assert has_no_turn_stderr(
        "Session 20260912_013519_45b697 found but has no messages. Starting fresh.\n"
        "session_id: 20260912_013519_45b697\n") is True
    # a resumed session WITH messages must not trip it
    assert has_no_turn_stderr(
        "\u21bb Resumed session 20260912_013519_45b697 (4 user messages, "
        "12 total messages)\nsession_id: 20260912_013519_45b697\n") is False
    assert has_no_turn_stderr("") is False


# ---------------------------------------------------------------------------
# 2. End-to-end reap: the three production specimens must NOT resolve 'done'
# ---------------------------------------------------------------------------

def _specimen_reap(tmp_path, body: str, stderr_text: str):
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True, exist_ok=True)
    executor = _executor(working_dir=str(tmp_path))
    task_id = "task_870spec"
    # the dispatched brief, written where _write_brief puts it
    briefs = tmp_path / "hermes-briefs"
    briefs.mkdir(parents=True, exist_ok=True)
    (briefs / f"{task_id}.md").write_text(dispatched_brief(task_id),
                                          encoding="utf-8")
    result_path = results_dir / f"{task_id}.result.md"
    result_path.write_text(body, encoding="utf-8")
    stderr_path = results_dir / f"{task_id}.stderr.log"
    stderr_path.write_text(stderr_text, encoding="utf-8")
    spawn = _hermes_spawn(task_id, result_path, stderr_path=stderr_path)
    return executor, spawn


def test_reap_provider_error_is_not_done(tmp_path):
    """Specimen 1/2 (task_931baa3968bbb088, task_595d46fb3a49e9fb): rc=0 and a
    137-B provider error on disk — main resolves 'done'. Must resolve a failed
    outcome naming the provider/transport reason."""
    executor, spawn = _specimen_reap(tmp_path, provider_error_body(),
                                     "session_id: 20260912_020000_aaaaaa\n")
    resolved = _reap(executor, spawn)
    assert len(resolved) == 1
    outcome, detail = resolved[0][2], resolved[0][3]
    assert outcome != "done", (
        "#870: a provider-error result resolved as 'done' — the status word "
        "fabricates success (the whole bug)"
    )
    assert "provider" in detail.lower() or "transport" in detail.lower(), detail


def test_reap_brief_echo_is_not_done(tmp_path):
    """Specimen 3 (the dangerous one, !908 reviewer lane): rc=0, 1375 B that
    is the dispatched brief echoed back + the 'found but has no messages'
    stderr. A merge gate reading 'completed' would merge an MR nobody
    reviewed."""
    executor, spawn = _specimen_reap(
        tmp_path, echo_specimen(),
        "Session 20260912_013519_45b697 found but has no messages. "
        "Starting fresh.\nsession_id: 20260912_013519_45b697\n")
    resolved = _reap(executor, spawn)
    assert len(resolved) == 1
    outcome, detail = resolved[0][2], resolved[0][3]
    assert outcome != "done", (
        "#870: an echoed brief resolved as 'done' — an unreviewed MR passes "
        "the merge gate"
    )
    assert "brief" in detail.lower() or "echo" in detail.lower(), detail


def test_reap_real_deliverable_quoting_brief_stays_done(tmp_path):
    """FALSE-POSITIVE GUARD at the reap level: a genuine verdict quoting its
    brief must still resolve 'done' even when stderr carries the no-turn
    line (a resumed session CAN legitimately say 'no messages' about a stale
    row; it must never alone veto a real body)."""
    executor, spawn = _specimen_reap(
        tmp_path, good_deliverable_quoting_brief(),
        "Session 20260912_013519_45b697 found but has no messages. "
        "Starting fresh.\nsession_id: 20260912_013519_45b697\n")
    resolved = _reap(executor, spawn)
    assert len(resolved) == 1
    assert resolved[0][2] == "done", (
        f"#870 false-positive: real work rejected at reap: {resolved[0][3]!r}"
    )


# ---------------------------------------------------------------------------
# 2b. Direct store pins (SQLite backend — not just the in-memory state):
#     the cap's authority lives in the guarded UPDATE, so it must be tested
#     against the real row semantics (attempt column, terminal guard).
# ---------------------------------------------------------------------------

def _sqlite_store(tmp_path):
    from hermes_cluster.state.cluster_store import ClusterStore
    return ClusterStore(db_path=str(tmp_path / "cluster.db"))


def test_sqlite_requeue_transitions_and_caps(tmp_path):
    from hermes_cluster.models import TaskStatus
    store = _sqlite_store(tmp_path)
    store.create_task("task_sq1", "cap me", [])
    store.set_task_status("task_sq1", TaskStatus.running)
    with store._tx() as conn:
        conn.execute("UPDATE tasks SET assigned_to = 'n1' WHERE id = 'task_sq1'")

    ok = store.requeue_task("task_sq1", reason="provider_error")
    assert ok is True
    t = store.get_task("task_sq1")
    assert t.status == TaskStatus.ready
    assert t.assigned_to is None
    assert t.attempts == 1

    # drain to the cap
    store.set_task_status("task_sq1", TaskStatus.running)
    assert store.requeue_task("task_sq1", reason="provider_error") is True
    store.set_task_status("task_sq1", TaskStatus.running)
    assert store.requeue_task("task_sq1", reason="provider_error") is True
    t = store.get_task("task_sq1")
    assert t.attempts == 3
    # 4th: at the cap — refuse to requeue (caller then consumes as failed)
    store.set_task_status("task_sq1", TaskStatus.running)
    assert store.requeue_task("task_sq1", reason="provider_error") is False
    t = store.get_task("task_sq1")
    assert t.status == TaskStatus.running  # untouched, NOT silently ready
    assert t.attempts == 3

    # terminal tasks are never revived
    store.set_task_status("task_sq1", TaskStatus.failed, fail_reason="done")
    assert store.requeue_task("task_sq1", reason="provider_error") is False
    store.close()


def test_sqlite_spawn_record_carries_attempt(tmp_path):
    store = _sqlite_store(tmp_path)
    store.record_task_spawn("task_sq2", mode="hermes", job_id="j", pid=1,
                            started_at=1.0, attempt=2)
    rec = store.get_task_spawn("task_sq2")
    assert rec["attempt"] == 2, (
        "#870: the delivery count must survive in the persisted spawn "
        "record across an executor restart"
    )
    store.close()


def test_postgres_requeue_parity(pg_store):
    """The same requeue/cap semantics on the Postgres store the cloud main
    actually runs (#829): schema drift applied, guarded UPDATE honours the
    cap and the terminal rule. Uses the shared pg_store fixture — the
    #829 conftest turns a set-but-unreachable DSN into a hard failure,
    never a silent pass."""
    from hermes_cluster.models import TaskStatus

    store = pg_store
    store.create_task("task_pg1", "cap me", [])
    store.set_task_status("task_pg1", TaskStatus.running)
    assert store.requeue_task("task_pg1", reason="provider_error") is True
    t = store.get_task("task_pg1")
    assert t.status == TaskStatus.ready
    assert t.attempts == 1

    for expected in (2, 3):
        store.set_task_status("task_pg1", TaskStatus.running)
        assert store.requeue_task("task_pg1", reason="provider_error") is True
        assert store.get_task("task_pg1").attempts == expected

    # at the cap: refuse, leave the row alone
    store.set_task_status("task_pg1", TaskStatus.running)
    assert store.requeue_task("task_pg1", reason="provider_error") is False
    t = store.get_task("task_pg1")
    assert t.status == TaskStatus.running and t.attempts == 3

    # spawn record attempt round-trip
    store.record_task_spawn("task_pg1", mode="hermes", job_id="j",
                            pid=1, started_at=1.0, attempt=3)
    assert store.get_task_spawn("task_pg1")["attempt"] == 3


# ---------------------------------------------------------------------------
# 3. Re-queue, not consume: /fail with requeue=true + attempt cap
# ---------------------------------------------------------------------------

def _fail_caller(executor):
    """Patch the signed POST for a reap that resolves a non-deliverable;
    returns (mocked _report_failure spy, calls to the signed request)."""
    calls = []

    def fake_signed(endpoint, method, path, data, token, node_id, timeout=15):
        calls.append((method, path, data))
        return {"status": "ok"}

    return calls, fake_signed


def test_non_deliverable_reports_failure_not_completion(tmp_path):
    """Drive the REAL report loop (_reap_finished_spawns): a hermes spawn
    whose result is a provider error must reach _report_failure naming the
    reason — never _report_completion. On main this routes straight to
    _report_completion: that call IS the fabricated 'completed'."""
    executor, spawn = _specimen_reap(tmp_path, provider_error_body(), "")
    executor._active_spawns[spawn.task_id] = spawn
    with patch.object(executor, "_capture_spawn_exits"), \
         patch.object(executor, "_report_completion") as done, \
         patch.object(executor, "_report_failure") as fail, \
         patch.object(executor, "_requeue_task", create=True) as requeue:
        executor._reap_finished_spawns()
    done.assert_not_called()
    assert requeue.called or fail.call_count == 1, (
        "#870: a rejected deliverable must be reported to main (failure or "
        "re-queue), never silently dropped"
    )
    reason = (requeue.call_args[0][2] if requeue.called
              else fail.call_args[0][1])
    assert "provider" in reason.lower() or "transport" in reason.lower(), reason


def test_requeue_hits_main_fail_endpoint_under_cap(tmp_path):
    """Re-queue = POST /fail with requeue=true (main resets the task to ready
    instead of consuming it). Under the cap; at/over the cap it must fall back
    to a plain consuming /fail — a genuinely broken brief cannot loop."""
    executor = _executor(working_dir=str(tmp_path))
    calls, fake_signed = _fail_caller(executor)
    # cap read from config default (3) — no override needed for this leg
    with patch("hermes_cluster.core.agent_executor._signed_request", fake_signed):
        executor._requeue_task("task_r1", "provider_error", "result.md: provider",
                               attempt=0)
        executor._requeue_task("task_r1", "provider_error", "result.md: provider",
                               attempt=2)
        executor._requeue_task("task_r1", "provider_error", "result.md: cap",
                               attempt=3)
    assert len(calls) == 3
    (m1, p1, d1), (m2, p2, d2), (m3, p3, d3) = calls
    assert p1.endswith("/api/v1/tasks/task_r1/fail") and d1.get("requeue") is True
    assert p2.endswith("/api/v1/tasks/task_r1/fail") and d2.get("requeue") is True
    assert p3.endswith("/api/v1/tasks/task_r1/fail")
    assert not d3.get("requeue"), (
        "at/over the cap the task must be consumed (plain /fail), not looped"
    )


def test_spawn_persists_attempt_counter(tmp_path, monkeypatch):
    """The attempt counter must ride through the persisted spawn record so an
    executor restart cannot lose the cap state."""
    captured = {}

    class _FakeProc:
        pid = 8801
        stderr = None
        stdout = None
        def poll(self):
            return None

    def fake_popen(cmd, **kw):
        captured["kw"] = kw
        return _FakeProc()

    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen,
    )
    store = MagicMock()
    executor = _executor(working_dir=str(tmp_path))
    executor._store = store
    executor._reconciled = True
    # simulate a previous failed delivery recorded at attempt=2
    store.get_task_spawn.return_value = {"task_id": "task_a1", "attempt": 2}
    executor._spawn_hermes_worker({"id": "task_a1", "title": "t"})
    assert store.record_task_spawn.called
    kwargs = store.record_task_spawn.call_args.kwargs
    assert kwargs.get("attempt") == 2, (
        "#870: re-spawn after a rejected deliverable must carry the attempt "
        "count forward, not reset the retry cap"
    )


def test_main_side_fail_requeue_semantics(client):
    """The router's /fail accepts requeue and, under the cap, resets the task
    to ready (claimable again) instead of consuming it; at/over the cap it
    falls back to a plain consuming fail. A re-queued task must never land on
    'failed' silently — that would let the loop spin without progress."""
    node_id = _register_node(client)
    task = _submit_task(client, title="requeue me")
    tid = task["id"]

    # first requeue -> back in the schedulable pool (ready, or running again
    # immediately if the scheduler just re-placed it on the online node),
    # attempts=1, never the terminal 'failed'.
    r = client.post(f"/api/v1/tasks/{tid}/fail",
                    json={"reason": "no_turn", "requeue": True})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "requeued", body
    t = client.get("/api/v1/tasks").json()
    t = next(x for x in t if x["id"] == tid)
    assert t["status"] in ("ready", "running"), t  # re-placed, not consumed
    assert t.get("attempts") == 1, t

    # drain the cap (retry_limit default 3): attempts 2,3 requeue; the 4th
    # must consume the task as failed rather than loop forever.
    client.post(f"/api/v1/tasks/{tid}/fail", json={"reason": "no_turn", "requeue": True})
    client.post(f"/api/v1/tasks/{tid}/fail", json={"reason": "no_turn", "requeue": True})
    r4 = client.post(f"/api/v1/tasks/{tid}/fail",
                     json={"reason": "no_turn", "requeue": True})
    assert r4.status_code == 200, r4.text
    assert r4.json()["status"] == "failed", r4.json()
    t = next(x for x in client.get("/api/v1/tasks").json() if x["id"] == tid)
    assert t["status"] == "failed"
    assert t.get("attempts") == 3, t


def test_plain_fail_still_consumes(client):
    """A /fail WITHOUT requeue keeps its old semantics exactly (terminal
    failed + dependent cascade) — the requeue path is opt-in per request."""
    task = _submit_task(client, title="plain fail")
    tid = task["id"]
    r = client.post(f"/api/v1/tasks/{tid}/fail", json={"reason": "boom"})
    assert r.status_code == 200 and r.json()["status"] == "failed"
    t = next(x for x in client.get("/api/v1/tasks").json() if x["id"] == tid)
    assert t["status"] == "failed"


# ---------------------------------------------------------------------------
# 4. Guards run on EVERY platform — collection itself is the proof.
#    (This module has no skipif markers and must pass identically on
#    Windows and POSIX; #868's review caught Windows-gated reds hiding on
#    Linux CI, so this file deliberately has none.)
# ---------------------------------------------------------------------------

def test_no_platform_gates_in_this_module():
    marker = "@pytest.mark." + "skip"
    src = Path(__file__).read_text(encoding="utf-8")
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue  # comments may discuss gates; they ARE not gates
        if line.lstrip().startswith("marker"):
            continue  # this test's own assembled marker string
        if marker in line:
            raise AssertionError(f"platform gate found: {line!r}")
