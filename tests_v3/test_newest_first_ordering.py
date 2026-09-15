"""Newest-first queue order — owner ruling 2026-09-15, verbatim:

    "priority goes to the most recent issues first then working towards the
     older ones"

This INVERTS the previous default. Every store sorted `ORDER BY priority,
created_at` ASCENDING, which serves the OLDEST first within a band — standard
FIFO and the opposite of the ruling.

Why it is right, in the owner's estate specifically: a recent issue is filed
against the CURRENT system, an old one may rest on a premise the estate has
already moved past. Measured on the day of the ruling — a ballot built on an
in-cluster MongoDB that does not exist; a SAMA regulatory finding about a
credits system scrapped four days after the analysis; a class comment citing
"#54 still open" where #54 had closed. Working oldest-first spends the most
effort on the least-current premises.

Priority band STILL DOMINATES. Recency is only the tiebreak inside a band, so
a band-0 task always beats a newer band-3 task.

Three stores implement this and MUST agree, or the backends serve different
queue orders (the class of bug test_store_parity_backends exists to catch):
  * ClusterState      (in-memory)  - python sorted()
  * ClusterStore      (SQLite)     - ORDER BY priority, created_at DESC
  * PostgresStore     (asyncpg)    - same SQL

and the bundle PACKER decides which issues a <=max_bundle_size sitting gets,
which is where the ruling bites hardest.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from hermes_cluster.core.intake_grouping import (
    GroupingConfig, pack_repo_candidates,
)

# `_recency_key` is NEW in this change, so it is imported INSIDE the two tests
# that pin it directly (repo standard, tests_v3/test_intake_grouping_762.py:
# "on origin/main these fail on the DEFECT ... not ImportError — new modules
# are imported INSIDE the tests that pin them"). Every other test below
# exercises BEHAVIOUR through the pre-existing `pack_repo_candidates` /
# store API, so on origin/main they fail on the oldest-first defect itself.


# ---------------------------------------------------------------------------
# 1. The bundle packer takes the NEWEST issues (the decisive site)
# ---------------------------------------------------------------------------

def _iss(iid: int, *, created: str, labels=None):
    return (f"proj#{iid}", {"iid": iid, "created_at": created,
                            "labels": labels or []}, 3)


def test_packer_takes_newest_issues_when_over_cap():
    """10 candidates, cap 3 -> the THREE NEWEST are bundled, not the oldest.

    RED before the fix: the packer preserved "created order ... by the
    caller", i.e. whatever order the GitLab page happened to arrive in.
    """
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    cands = [
        _iss(700 + i, created=(base + timedelta(days=i)).isoformat())
        for i in range(10)
    ]
    # Hand them over OLDEST-first, the order the old code would have kept.
    batch, _reason = pack_repo_candidates(
        "repo#env/dev", cands, GroupingConfig(max_bundle_size=3))

    picked = sorted(int(c[1]["iid"]) for c in batch)
    assert picked == [707, 708, 709], (
        f"expected the three NEWEST (707/708/709), got {picked} — "
        "the packer is still serving oldest-first")


def test_packer_is_newest_first_even_when_input_is_newest_last():
    """Order must come from created_at, never from arrival order."""
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    cands = [
        _iss(700 + i, created=(base + timedelta(days=i)).isoformat())
        for i in range(6)
    ][::-1]  # newest FIRST on input this time
    batch, _ = pack_repo_candidates(
        "repo#env/dev", cands, GroupingConfig(max_bundle_size=2))
    assert sorted(int(c[1]["iid"]) for c in batch) == [704, 705]


def test_band_still_beats_recency():
    """A band-0 issue wins over a NEWER band-3 issue — the ruling is a
    tiebreak inside a band, never a replacement for the band."""
    old_urgent = ("proj#1", {"iid": 1, "created_at": "2026-01-01T00:00:00Z",
                             "labels": []}, 0)
    new_routine = ("proj#2", {"iid": 2, "created_at": "2026-09-15T00:00:00Z",
                              "labels": []}, 3)
    batch, _ = pack_repo_candidates(
        "repo#env/dev", [new_routine, old_urgent],
        GroupingConfig(max_bundle_size=1))
    assert [c[1]["iid"] for c in batch] == [1], (
        "band 0 must take the slot even though the band-3 issue is newer")


# ---------------------------------------------------------------------------
# 2. The recency key's edge semantics
# ---------------------------------------------------------------------------

def test_undated_issue_sorts_LAST_not_first():
    """Fail-open here means 'bad data does not JUMP THE QUEUE' — the
    opposite polarity from parse_gitlab_time's fail-open, which is about
    never STALLING intake. An undated issue is still bundled, just not
    ahead of dated work."""
    from hermes_cluster.core.intake_grouping import _recency_key

    dated = _iss(1, created="2026-01-01T00:00:00Z")
    undated = _iss(2, created="")
    assert _recency_key(dated) < _recency_key(undated)

    batch, _ = pack_repo_candidates(
        "repo#env/dev", [undated, dated], GroupingConfig(max_bundle_size=1))
    assert [c[1]["iid"] for c in batch] == [1]


def test_recency_key_is_a_total_order_so_packing_is_deterministic():
    """Two issues created in the SAME second must not reorder between
    cycles, or a bundle's membership churns and dedup thrashes."""
    from hermes_cluster.core.intake_grouping import _recency_key

    same = "2026-09-15T12:00:00Z"
    a, b = _iss(10, created=same), _iss(11, created=same)
    assert _recency_key(a) != _recency_key(b)
    assert _recency_key(a) < _recency_key(b)  # issue_id breaks the tie
    for _ in range(5):
        batch, _r = pack_repo_candidates(
            "repo#env/dev", [b, a], GroupingConfig(max_bundle_size=1))
        assert [c[1]["iid"] for c in batch] == [10]


# ---------------------------------------------------------------------------
# 3. The owner's 30 ceiling
# ---------------------------------------------------------------------------

def test_default_bundle_cap_is_thirty():
    """Owner ruling 2026-09-15: "a limit of 30 issues". Was 40 under the
    earlier "30-40" intent."""
    assert GroupingConfig().max_bundle_size == 30


# ---------------------------------------------------------------------------
# 4. Store parity — all three backends must agree on the order
# ---------------------------------------------------------------------------

def _mk_ready(store, tid, *, priority, created):
    store.create_task(tid, tid, ["tooling"], priority=priority)
    store.trigger_pending_tasks()
    t = store.get_task(tid)
    t.created_at = created
    return t


@pytest.mark.parametrize("store_name", ["memory", "sqlite"])
def test_ready_scan_serves_newest_first_within_a_band(store_name, tmp_path):
    from hermes_cluster.models import Node, NodeStatus
    if store_name == "memory":
        from hermes_cluster.state import ClusterState
        store = ClusterState()
    else:
        from hermes_cluster.state.cluster_store import ClusterStore
        store = ClusterStore(str(tmp_path / "c.db"))

    store.register_node(Node(id="n1", name="n1", capabilities=["tooling"],
                             status=NodeStatus.online, max_concurrent=1))
    base = datetime(2026, 9, 1)
    store.create_task("old", "old", ["tooling"], priority=3)
    store.create_task("new", "new", ["tooling"], priority=3)
    store.trigger_pending_tasks()

    # Force known creation times (same band).
    for tid, dt in (("old", base), ("new", base + timedelta(days=5))):
        t = store.get_task(tid)
        if t is not None:
            t.created_at = dt
        setter = getattr(store, "_set_created_at", None)
        if setter:
            setter(tid, dt)

    store.schedule_pending()
    assert store.get_task("new").assigned_to == "n1", (
        "the NEWER task must take the only slot within the same band")
    assert store.get_task("old").assigned_to is None
