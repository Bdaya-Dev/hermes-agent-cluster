"""Fixtures captured from the two real deliveries named in shared/claude-plugins#871.

Both lanes did the work and delivered by PRINTING their final message (the
lane convention on this fleet). Under #868's split the printed message lands
in ``<task>.stdout.log`` — never in ``<task>.result.md`` — and both deliveries
wrote no result.md, so the reap resolved ``no_result`` → ``_report_failure``:
a lane that did its job was recorded FAILED, and the lead re-dispatched it.

  task_b6130afa6be27c64 (aggregate!280-rev2, 2053 B transcript)
      Ran the live PowerShell experiment, verified all six review items, and
      posted "Reviewer verdict: PASS" as note 135356 on devops/aggregate!280
      against SHA a2d3072c9. Its results dir held .stderr.log (113 B) and
      .stdout.log (2053 B) and NO .result.md. Recorded FAILED.
  task_1c15d3876544db7a (aggregate!280-rev, 1642 B transcript)
      The predecessor; same shape; posted note 135349. Also recorded FAILED.
      (MR !280 carries TWO independent PASS verdicts from two lanes that both
      "failed".)

Reconstructed vs verbatim
-------------------------
The transcript bytes below are RECONSTRUCTED in the captured shape (a -Q quiet
run's session-info preamble + the printed final message; sizes 2053 B / 1642 B)
— this worker node does not hold the originals, they live on the windows-desktop
results dir. The verdict/SHA/note facts are verbatim from the issue report
(the notes themselves are the durable ground truth, readable on the MR). The
STRUCTURE the tests turn on — rc=0, non-empty transcript, no result.md, no
stranded tmp — is the measured incident shape and does not depend on wording.
"""

# The final message task_b6130afa6be27c64 printed to stdout (= its deliverable
# under the lane convention; note 135356 on devops/aggregate!280).
B6130AFA_TRANSCRIPT = """session_id: 20260912_045301_c7f2b1
lane_key: devops/aggregate!280-rev2

## Reviewer verdict: PASS

SHA: a2d3072c9 (live head at review time; re-confirmed after the pipeline)

Method: live PowerShell experiment on windows_pc_worker (the platform the
change targets), plus a direct read of every touched file. No claim below is
inferred from the MR description.

1. stdout/stderr split — VERIFIED: a fresh delivery inherits
   task_<id>.stdout.log; result.md is never opened by the parent.
2. Spawn-time cleanup — VERIFIED: a stale result.md from a previous delivery
   is removed before the child starts.
3. tmp-then-rename survives reap on Windows — VERIFIED: staged the exact
   sequence the lane's write_file performs; rename landed and the content
   survived the reap closing the parent's handles.
4. Stranded .hermes-tmp.* surfaced — VERIFIED: reap reports lost_deliverable
   naming the file; it is never swept and never resolves done.
5. Transcript never promoted — VERIFIED: rc=0 + transcript + no result.md
   reaps lost_deliverable pointing at the transcript.
6. Resume path — VERIFIED: session row created by the lane registry, first
   delivery finds it empty and starts fresh (benign).

The change is sound; the Windows mechanic it fixes reproduces on this node.
"""

# task_1c15d3876544db7a (aggregate!280-rev) — the predecessor. NOTE (#870
# 135350): this attempt's session "found but has no messages" line makes the
# captured evidence ambiguous about whether it printed a real verdict; the
# issue body still counts its transcript as carrying the deliverable, and the
# MR carries note 135349 from it. Kept here as the second specimen of the
# reap-shape, not as a text fixture.
_1C15D387_TRANSCRIPT = """session_id: 20260912_042117_99eaa5
lane_key: devops/aggregate!280-rev

## Reviewer verdict: PASS

SHA: a2d3072c9

Reviewer lane for aggregate!280: ran the requested checks on
windows_pc_worker and posted the full verdict as note 135349 on
devops/aggregate!280. Summary of the six items: stdout split verified,
spawn cleanup verified, Windows rename mechanic verified, stranded tmp
surfaced, no transcript promotion, resume path benign.

Reviewer verdict: PASS
"""

# The !23 / PR-23 near-miss shape (#868's own rationale): a TRUNCATED
# transcript whose per-section "Verdict: CORRECT" lines read as a pass while
# the REAL verdict was stranded elsewhere (a .hermes-tmp.*). The cluster must
# keep flagging this shape lost_deliverable and must NEVER promote it to a
# pass-equivalent outcome.
PR23_TRUNCATED_TRANSCRIPT = """startup: resolving model provider...
### Section 1: executor split — **Verdict: CORRECT**
### Section 2: spawn cleanup — **Verdict: CORRECT**
### Section 3: reap path — **Verdict: C
"""
# (bytes cut mid-word — a truncated stdout-era copy)

STRANDED_VERDICT = (
    "Reviewer verdict: NEEDS-CHANGES\n\n"
    "Section 3 is wrong: the reap promotes a transcript whose tail was "
    "truncated, reading per-section CORRECT lines as an overall pass.\n"
)
