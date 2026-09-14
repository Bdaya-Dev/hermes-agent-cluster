"""Regression tests for shared/claude-plugins#871 — false failure of a lane
that delivered by printing its final message.

#868 moved the child's inherited stdout from result.md to
``<task>.stdout.log`` and left result.md free — correct for its own bug, but
lanes deliver by PRINTING their final message. Pre-#868 the print WAS
result.md and reaped done; post-#868 the lane must write result.md explicitly,
and #868 refused any transcript promotion — so a lane that did everything
right reaped ``no_result`` → ``_report_failure``. Measured twice on the same
lane (the captured specimens in ``fixtures.specimens_871``):
task_b6130afa6be27c64 and task_1c15d3876544db7a both posted PASS verdicts on
devops/aggregate!280 (notes 135356 / 135349) with full transcripts and no
result.md — and both were recorded FAILED, feeding an unbounded re-dispatch
loop.

The fix (option 3 in the issue):

1. The wrapper makes the delivery contract EXPLICIT in every hermes-mode
   brief: the exact result.md path, written down, with the instruction to
   write it.
2. Reap falls back to the transcript under the narrow rule (rc=0, no
   result.md, non-empty stdout.log, no stranded .hermes-tmp.* from this
   delivery — the last condition already holds structurally because the
   stranded check runs FIRST and returns), and the promotion is VISIBLE:
   - the outcome is ``transcript_promoted``, never plain ``done``, and
   - result.md is written with a loud machine-checkable marker header.

Nothing here is OS-specific: every test in this file runs on every platform.
The #868 guards are asserted as CONTROLS (a real deliverable still reaps
done; the stranded-tmp and rc!=0 paths still reap loudly) so the fix cannot
reopen the false-success hole it is closing the false-failure side of.
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
from tests_v3.fixtures.specimens_871 import (
    _1C15D387_TRANSCRIPT,
    B6130AFA_TRANSCRIPT,
    PR23_TRUNCATED_TRANSCRIPT,
    STRANDED_VERDICT,
)


def _executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _spawn(task_id: str, results_dir: Path, *, transcript: str = "",
           deliverable: str = None, rc=0, started_at=None) -> ActiveSpawn:
    """Build a finished hermes spawn in exactly the captured incident shape:
    transcript in stdout.log, deliverable optionally written by the lane."""
    results_dir.mkdir(parents=True, exist_ok=True)
    result_path = results_dir / f"{task_id}.result.md"
    stdout_path = results_dir / f"{task_id}.stdout.log"
    if transcript:
        stdout_path.write_text(transcript, encoding="utf-8")
    if deliverable is not None:
        result_path.write_text(deliverable, encoding="utf-8")
    proc = MagicMock()
    proc.poll.return_value = rc
    return ActiveSpawn(
        task_id=task_id, task_title="t", process=proc,
        mode="hermes", result_path=str(result_path),
        stdout_path=str(stdout_path),
        started_at=started_at or (time.time() - 600.0),
    )


def _reap(executor, spawn):
    resolved = []
    executor._reap_hermes_spawn(spawn.task_id, spawn, 600.0, resolved)
    return resolved


# ---------------------------------------------------------------------------
# The #871 defect: the two captured specimens must NOT reap as a failure,
# and must NOT reap as a plain done either.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("task_id,transcript", [
    ("task_b6130afa6be27c64", B6130AFA_TRANSCRIPT),
    ("task_1c15d3876544db7a", _1C15D387_TRANSCRIPT),
], ids=["b6130afa-rev2", "1c15d387-rev"])
def test_specimen_lane_that_printed_reaps_transcript_promoted(
        tmp_path, task_id, transcript):
    """rc=0 + non-empty transcript + NO result.md + no stranded tmp →
    transcript_promoted. This is the exact measured shape of both !280
    reviewer lanes that posted real PASS verdicts and were recorded FAILED."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn(task_id, results_dir, transcript=transcript)

    resolved = _reap(executor, spawn)

    assert len(resolved) == 1
    outcome = resolved[0][2]
    assert outcome not in ("no_result", "lost_deliverable", "spawn_failed"), (
        f"#871: a lane that delivered by printing must NOT reap as a failure "
        f"shape; got {outcome}: {resolved[0][3]}"
    )
    assert outcome == "transcript_promoted"
    assert outcome != "done", "#868: a promotion is never a verdict-grade done"


def test_promotion_lands_visible_marker_plus_full_transcript(tmp_path):
    """The promoted deliverable is the transcript VERBATIM under a loud,
    machine-checkable marker header (grep-able first line + provenance)."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_mark", results_dir, transcript=B6130AFA_TRANSCRIPT)

    _reap(executor, spawn)

    result_md = results_dir / "task_mark.result.md"
    assert result_md.is_file()
    text = result_md.read_text(encoding="utf-8")
    first_line = text.splitlines()[0]
    assert "TRANSCRIPT-PROMOTED" in first_line
    assert "NOT a verdict-grade" in first_line  # the gate hint is IN the marker
    assert "promoted_from" in text and "task_mark" in text
    # The verdict itself survived into the deliverable, unaltered.
    assert "## Reviewer verdict: PASS" in text
    assert "135356" in text or "a2d3072c9" in text
    # The promotion staged via a tmp and renamed it away — no litter.
    assert not list(results_dir.glob(".hermes-promoted-tmp.*"))


def test_promoted_delivery_reports_completion_not_failure(tmp_path):
    """End-to-end through _reap_finished_spawns: the #871 complaint is that
    the cluster RECORDS the lane failed. transcript_promoted must drive
    /complete, never /fail, and must still bind the lane session (#858
    stateful-lane bookkeeping) so the next brief resumes the same session."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_e2e", results_dir, transcript=B6130AFA_TRANSCRIPT)
    spawn.lane_key = "Bdaya-Dev/hermes-agent-cluster#fix/871-false-failure"
    executor._active_spawns["task_e2e"] = spawn

    with patch.object(executor, "_report_completion") as done, \
         patch.object(executor, "_report_failure") as fail, \
         patch.object(executor, "_drop_persisted_spawn"), \
         patch.object(executor, "_touch_lane_from_spawn") as touch:
        executor._reap_finished_spawns()

    fail.assert_not_called()
    done.assert_called_once()
    assert done.call_args[0][0] == "task_e2e"
    assert "promoted" in done.call_args[1]["detail"].lower()
    touch.assert_called_once()  # lane session still captured on completion


# ---------------------------------------------------------------------------
# Controls — #868's guards must keep holding. Each is also the RED proof that
# a naive "promote any transcript" reopens the false-success hole.
# ---------------------------------------------------------------------------

def test_real_deliverable_still_reaps_plain_done(tmp_path):
    """A compliant lane that WRITES result.md reaps done — the promotion path
    must not touch it and must not add a marker."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_clean", results_dir,
                   transcript="noisy startup line\n",
                   deliverable="## My verdict: PASS\n\nreal deliverable")

    resolved = _reap(executor, spawn)

    assert resolved[0][2] == "done"
    text = (results_dir / "task_clean.result.md").read_text(encoding="utf-8")
    assert "TRANSCRIPT-PROMOTED" not in text
    assert text.startswith("## My verdict")


def test_pr23_shape_stranded_tmp_never_promotes(tmp_path):
    """The !23 / PR-23 near-miss: truncated transcript carrying per-section
    'Verdict: CORRECT' lines while the REAL NEEDS-CHANGES verdict stranded in
    a .hermes-tmp.*. Must reap lost_deliverable — NOT promoted, NOT done, NOT
    silently passed — and the stranded file is surfaced, not swept.
    (Mutation check: if the promotion branch ran BEFORE the stranded check,
    this transcript's 'CORRECT' lines would promote to a pass-shaped file.)"""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_pr23", results_dir, transcript=PR23_TRUNCATED_TRANSCRIPT)
    stranded = results_dir / ".hermes-tmp.PR23"
    stranded.write_text(STRANDED_VERDICT, encoding="utf-8")

    resolved = _reap(executor, spawn)

    assert resolved[0][2] == "lost_deliverable"
    assert "PR23" in resolved[0][3]
    assert stranded.exists(), "surfaced, not swept"
    assert not (results_dir / "task_pr23.result.md").exists(), (
        "#868: a truncated verdict-bearing transcript must never be copied "
        "into the deliverable while a stranded tmp is unexplained"
    )


def test_promotion_is_refused_before_stranded_tmp_check_cannot_run(tmp_path):
    """Ordering guard: with a stranded tmp from THIS delivery the reap must
    return before any promotion is attempted, even though the transcript
    looks complete. (A future refactor that moves the promotion above the
    stranded check fails HERE first.)"""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_order", results_dir, transcript=B6130AFA_TRANSCRIPT)
    stranded = results_dir / ".hermes-tmp.ORDER"
    stranded.write_text("the real thing the lane tried to write", encoding="utf-8")

    resolved = _reap(executor, spawn)

    assert resolved[0][2] == "lost_deliverable"
    assert not (results_dir / "task_order.result.md").exists()


@pytest.mark.parametrize("rc", [1, 3, -9], ids=["rc1", "rc3", "signaled"])
def test_nonzero_exit_never_promotes(tmp_path, rc):
    """rc!=0 keeps failing (spawn_failed) even with a full transcript — the
    narrow rule requires rc=0; a crashed mid-write lane must not be promoted.

    #892 note: the crashed lane now leaves an executor-written diagnostic file
    (the 05:32Z full-disk deaths left NOTHING but a last-line fail_reason) —
    the invariant this test holds is that the lane's TRANSCRIPT never rides a
    nonzero exit into the deliverable, and that stays true."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_rc", results_dir,
                   transcript=B6130AFA_TRANSCRIPT, rc=rc)

    resolved = _reap(executor, spawn)

    assert resolved[0][2] == "spawn_failed"
    diag = results_dir / "task_rc.result.md"
    assert diag.read_text(encoding="utf-8").startswith("<!-- LANE-STARTED:")
    assert B6130AFA_TRANSCRIPT[:200] not in diag.read_text(encoding="utf-8")


def test_whitespace_transcript_still_no_result(tmp_path):
    """The rule requires a NON-EMPTY transcript; a silent rc=0 with an empty
    stdout.log keeps reaping no_result → failure (nothing was delivered).

    #892: the no-deliverable lane now leaves the executor's first-line
    diagnostic (not a promotion — there was no transcript to promote)."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_silent", results_dir, transcript="   \n\n")

    resolved = _reap(executor, spawn)

    assert resolved[0][2] == "no_result"
    diag = results_dir / "task_silent.result.md"
    assert diag.read_text(encoding="utf-8").startswith("<!-- LANE-STARTED:")
    assert "NO deliverable" in diag.read_text(encoding="utf-8")


def test_promotion_write_failure_surfaces_loudly(tmp_path):
    """If the promotion write itself fails (path un-writable), the reap must
    fall back to the LOST-deliverable surface — silent loss stays forbidden
    (#868). Simulate by pointing result_path into a directory that cannot
    hold it (result_path parent is a regular FILE)."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    spawn = _spawn("task_blocked", results_dir, transcript=B6130AFA_TRANSCRIPT)
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir", encoding="utf-8")
    spawn.result_path = str(blocker / "task_blocked.result.md")

    resolved = _reap(executor, spawn)

    outcome = resolved[0][2]
    assert outcome == "lost_deliverable"
    assert "promotion write FAILED" in resolved[0][3], (
        "the loud surface must name the failed promotion, not just point at "
        "the transcript — otherwise a silent skip of promotion is indistinguishable"
    )


# ---------------------------------------------------------------------------
# Option 1 half: the wrapper states the contract in the brief it generates.
# ---------------------------------------------------------------------------

def test_hermes_brief_states_delivery_contract_with_exact_path(monkeypatch, tmp_path):
    """Every hermes-mode brief must name the EXACT result.md path and say
    printing is not delivery — the contract two diligent lanes never saw."""
    captured = {}

    class _FakeProc:
        pid = 9955
        stderr = None
        stdout = None

        def poll(self):
            return None

    def fake_popen(cmd, **kw):
        captured["cmd"] = cmd
        return _FakeProc()

    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", fake_popen
    )
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    executor._spawn_hermes_worker({"id": "task_brief", "title": "t"})

    brief = (tmp_path / "hermes-briefs" / "task_brief.md").read_text(
        encoding="utf-8")
    result_path = str(tmp_path / "hermes-results" / "task_brief.result.md")
    assert result_path in brief, "brief must name the lane's exact deliverable"
    assert "Delivery contract" in brief
    # The contract is honest about the consequence: printing now completes
    # the task as a promotion, so re-dispatch loops stop either way — but the
    # lane is told plainly which outcome each path produces.
    assert "TRANSCRIPT-PROMOTED" in brief or "transcript" in brief.lower()
    # bdaya-dispatch mode keeps its old brief shape (no hermes deliverable).
    assert captured["cmd"], "spawn happened with the patched Popen"
