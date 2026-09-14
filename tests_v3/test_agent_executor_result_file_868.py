"""Regression tests for shared/claude-plugins#868 — the executor held the
lane's DELIVERABLE path (``<task>.result.md``) open for the child's whole
lifetime as its inherited stdout, so on Windows the lane's own write_file —
which stages to a tmp file and renames into place — failed with a sharing
violation and the deliverable was silently stranded as ``.hermes-tmp.*``.

The fix splits the two roles the one path was serving:

  - ``<task>.stdout.log``  — the child's inherited stdout (a transcript); this
    is the path the executor holds open.
  - ``<task>.result.md``   — the lane's deliverable; the executor never opens
    it, so a tmp-then-rename write into it succeeds even on Windows.

Plus a reap-time lost-deliverable check: a ``.hermes-tmp.*`` file in the
results directory, freshly written within a delivery's window, is a stranded
deliverable and must be surfaced (loud failure), never swept and never
resolved as a plain ``done``. A transcript with no deliverable is surfaced the
same way — the stdout log is deliberately NOT copied into result.md, because
a transcript masquerading as a deliverable is exactly the !23 near-miss.

Windows-only mechanics
----------------------
POSIX rename(2) replaces an open destination happily, so the sharing-violation
mechanic CANNOT be reproduced there. The test that asserts the failure itself
is gated on Windows via ``_win_only`` and SKIPS honestly elsewhere — it never
passes vacuously off-Windows. The structural half of the fix (stdout handle on
its own path, reap checks, spawn cleanup) is platform-independent and runs
everywhere.
"""

import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hermes_cluster.core.agent_executor import (
    ActiveSpawn,
    AgentExecutor,
    AgentExecutorConfig,
)

IS_WINDOWS = sys.platform == "win32"
_win_only = pytest.mark.skipif(
    not IS_WINDOWS,
    reason="#868's rename-over-open-file failure is a Windows sharing violation; "
    "POSIX replaces an open file happily, so this cannot be reproduced here "
    "(structural tests in this file run on every platform)",
)


def _executor(**cfg_overrides) -> AgentExecutor:
    defaults = dict(enabled=True, poll_interval=60)
    defaults.update(cfg_overrides)
    return AgentExecutor(
        config=AgentExecutorConfig(**defaults),
        node_id="test-node",
        cluster_endpoint="http://127.0.0.1:9999",
    )


def _fake_popen(captured):
    class _FakeProc:
        pid = 9911
        stderr = None
        stdout = None

        def poll(self):
            return None

    def fake_popen(cmd, **kw):
        captured["kw"] = kw
        captured["cmd"] = cmd
        return _FakeProc()

    return fake_popen


# ---------------------------------------------------------------------------
# Windows mechanic: an open destination genuinely refuses a rename — this is
# the bug's ground truth, asserted against the OS, not against our code.
# ---------------------------------------------------------------------------

@_win_only
def test_windows_rename_over_open_destination_fails(tmp_path):
    """Prove the failure MECHANIC exists on this OS before anything else:
    holding the destination open the way the executor held result.md open
    makes Move-Item refuse with the sharing violation the lanes hit; closing
    the handle makes the identical rename succeed."""
    target = tmp_path / "task_win.result.md"
    staged = tmp_path / ".hermes-tmp.WINTEST"
    staged.write_text("the real deliverable", encoding="utf-8")

    held = open(target, "w", encoding="utf-8")
    try:
        rc = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"Move-Item -Force -Path '{staged}' -Destination '{target}'"],
            capture_output=True, text=True,
        )
        assert rc.returncode != 0, (
            "expected Move-Item over an open destination to FAIL on Windows; "
            "the mechanic this issue is about did not reproduce"
        )
    finally:
        held.close()

    # Closing the handle makes the identical rename succeed — proves the
    # refusal came from the held handle, not from the path or the content.
    rc2 = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         f"Move-Item -Force -Path '{staged}' -Destination '{target}'"],
        capture_output=True, text=True,
    )
    assert rc2.returncode == 0, rc2.stderr
    assert target.read_text(encoding="utf-8") == "the real deliverable"


# ---------------------------------------------------------------------------
# The fix, structural half (runs on every platform)
# ---------------------------------------------------------------------------

def test_spawn_child_stdout_handle_is_stdout_log_not_result_md(monkeypatch, tmp_path):
    """The inherited-stdout handle must be <task>.stdout.log; the executor
    must never have <task>.result.md open at all."""
    captured = {}
    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", _fake_popen(captured)
    )
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    executor._spawn_hermes_worker({"id": "task_868a", "title": "t"})

    spawn = executor._active_spawns["task_868a"]
    results_dir = tmp_path / "hermes-results"
    assert spawn.result_path == str(results_dir / "task_868a.result.md")
    assert Path(spawn.stdout_path) == results_dir / "task_868a.stdout.log"
    assert captured["kw"]["stdout"].name == str(results_dir / "task_868a.stdout.log")

    # The result path was never opened by the parent: pre-fix,
    # open(result_path, "w") created it at spawn time — the Windows sharing
    # violation then locked the lane out of its own deliverable path.
    assert not Path(spawn.result_path).exists(), (
        "#868: the executor opened the deliverable path at spawn"
    )
    # The held handle points at the stdout log, never the result.
    assert spawn.stdout_file.name == spawn.stdout_path


def test_stdout_log_is_inherited_and_result_md_receives_lane_write(monkeypatch, tmp_path):
    """Live end-to-end with a real child process: the child's inherited stdout
    lands in the stdout log, and the DELIVERABLE the lane renames into the
    result path after spawn survives the whole spawn→reap cycle — pre-fix the
    executor's open handle would truncate it on reap-close (POSIX) or block
    the rename outright (Windows)."""
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True)
    deliverable = results_dir / "task_868b.result.md"

    # The child: emit on stdout, stage a tmp file, rename it into the
    # deliverable path — the exact sequence a lane's write_file performs.
    child = tmp_path / "child.py"
    child.write_text(
        "import sys, pathlib\n"
        "print('child transcript line')\n"
        f"d = pathlib.Path({str(deliverable)!r})\n"
        "tmp = d.with_name('.hermes-tmp.CHILD')\n"
        "tmp.write_text('THE DELIVERABLE', encoding='utf-8')\n"
        "tmp.rename(d)\n",
        encoding="utf-8",
    )

    captured = {}
    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", _fake_popen(captured)
    )
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    executor._spawn_hermes_worker({"id": "task_868b", "title": "t"})
    spawn = executor._active_spawns["task_868b"]

    # Drive the real spawn sequence the fixed code performs: the child
    # inherits the open stdout handle, writes its deliverable, exits.
    # (Restore the real Popen first — subprocess.run goes through it.)
    monkeypatch.undo()
    handle = captured["kw"]["stdout"]
    rc = subprocess.run([sys.executable, str(child)], stdout=handle).returncode
    assert rc == 0
    spawn.process.poll = lambda: 0

    resolved = []
    executor._reap_hermes_spawn("task_868b", spawn, 5.0, resolved)
    assert resolved and resolved[0][2] == "done", resolved

    # Deliverable intact AFTER the reap closed the parent's handles...
    assert deliverable.read_text(encoding="utf-8") == "THE DELIVERABLE"
    # ...and the transcript lives in its own file, not mixed into it.
    transcript = Path(spawn.stdout_path).read_text(encoding="utf-8")
    assert "child transcript line" in transcript
    assert "THE DELIVERABLE" not in transcript


def test_reap_surfaces_stranded_hermes_tmp(tmp_path):
    """A fresh .hermes-tmp.* in the results dir within a finished delivery's
    window is a lost deliverable: the reap must surface it (loud failed
    outcome naming the file), and must NOT sweep it and NOT report done."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True)
    (results_dir / "task_868c.result.md").write_text("truncated stdout-era copy", encoding="utf-8")
    stranded = results_dir / ".hermes-tmp.STRANDED"
    stranded.write_text("Reviewer verdict: NEEDS-CHANGES", encoding="utf-8")

    spawn = ActiveSpawn(
        task_id="task_868c", task_title="t", process=MagicMock(),
        mode="hermes", result_path=str(results_dir / "task_868c.result.md"),
        started_at=time.time() - 300.0,
    )
    spawn.process.poll.return_value = 0

    resolved = []
    executor._reap_hermes_spawn("task_868c", spawn, 300.0, resolved)
    assert len(resolved) == 1
    _tid, _sp, outcome, detail = resolved[0]
    assert outcome == "lost_deliverable", (
        "#868: a finished delivery with a stranded .hermes-tmp.* must NOT "
        "resolve as a plain 'done' — silent loss is the whole bug"
    )
    assert "STRANDED" in detail
    # Surfaced, not swept: the file must still be on disk for the operator.
    assert stranded.exists()


def test_reap_ignores_tmp_older_than_this_delivery(tmp_path):
    """The check is scoped to the delivery's own window; a temp predating the
    spawn belongs to some other task and must not poison this reap."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True)
    (results_dir / "task_868d.result.md").write_text("deliverable", encoding="utf-8")
    old_tmp = results_dir / ".hermes-tmp.OTHER"
    old_tmp.write_text("x", encoding="utf-8")
    past = time.time() - 3600.0
    os.utime(old_tmp, (past, past))

    spawn = ActiveSpawn(
        task_id="task_868d", task_title="t", process=MagicMock(),
        mode="hermes", result_path=str(results_dir / "task_868d.result.md"),
        started_at=time.time() - 60.0,
    )
    spawn.process.poll.return_value = 0
    resolved = []
    executor._reap_hermes_spawn("task_868d", spawn, 60.0, resolved)
    assert resolved and resolved[0][2] == "done"


def test_transcript_promotion_is_never_a_plain_done(tmp_path):
    """rc=0, deliverable absent, stdout log full of transcript: must NOT be
    done (#868's !23 trap stands — the promotion is only allowed as the
    VISIBLE transcript_promoted outcome, never as a verdict-grade done).
    #871 supersedes the pre-fix expectation of lost_deliverable here: this is
    exactly the shape of the two diligent !280 reviewer lanes that were
    falsely FAILED; the fix promotes with a loud marker."""
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True)
    stdout_path = results_dir / "task_868e.stdout.log"
    stdout_path.write_text("some startup warnings\n**Verdict: CORRECT**\n", encoding="utf-8")

    spawn = ActiveSpawn(
        task_id="task_868e", task_title="t", process=MagicMock(),
        mode="hermes", result_path=str(results_dir / "task_868e.result.md"),
        stdout_path=str(stdout_path),
        started_at=time.time() - 60.0,
    )
    spawn.process.poll.return_value = 0
    resolved = []
    executor._reap_hermes_spawn("task_868e", spawn, 60.0, resolved)
    assert resolved
    outcome, detail = resolved[0][2], resolved[0][3]
    assert outcome != "done", (
        "#868: a promoted transcript is NOT a lane verdict — it may never "
        "resolve as plain done"
    )
    assert outcome == "transcript_promoted", (
        "#871: transcript-only + rc=0 must NOT reap as a failure either"
    )
    # The promotion is VISIBLE and machine-checkable in the deliverable itself.
    promoted = (results_dir / "task_868e.result.md").read_text(encoding="utf-8")
    assert "TRANSCRIPT-PROMOTED" in promoted.splitlines()[0]
    assert "some startup warnings" in promoted


def test_spawn_cleans_prior_delivery_files(monkeypatch, tmp_path):
    """Stale files from a previous delivery of the same task id — result.md,
    stdout log, stderr log — must be gone before the child starts, so a stale
    file can never drive a reap to a false 'done' (#851 semantics preserved
    without the pre-fix open-the-deliverable hack)."""
    results_dir = tmp_path / "hermes-results"
    results_dir.mkdir(parents=True)
    (results_dir / "task_868f.result.md").write_text("OLD deliverable", encoding="utf-8")
    (results_dir / "task_868f.stdout.log").write_text("OLD transcript", encoding="utf-8")
    (results_dir / "task_868f.stderr.log").write_text("OLD stderr", encoding="utf-8")

    captured = {}
    monkeypatch.setattr(
        "hermes_cluster.core.agent_executor.subprocess.Popen", _fake_popen(captured)
    )
    executor = _executor(worker="hermes", working_dir=str(tmp_path))
    executor._spawn_hermes_worker({"id": "task_868f", "title": "t"})
    spawn = executor._active_spawns["task_868f"]

    assert not Path(spawn.result_path).exists()
    assert Path(spawn.stdout_path).exists()  # freshly opened/truncated by us
    with open(spawn.stdout_path, encoding="utf-8") as f:
        assert "OLD" not in f.read()
    assert "OLD" not in Path(spawn.stderr_path).read_text(encoding="utf-8")
