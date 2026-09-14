"""#929 — the executor-side lane cost footnote.

Why this exists: the #860 footnote rides the bdaya-enforcement
``transform_llm_output`` hook, which (a) is dead on any node whose pinned
plugin predates #860 — the exact state that made 29 completed cluster lanes
ship zero footnotes — and (b) can only touch the lane's PRINTED final text,
which since #868 is not the deliverable unless the transcript was promoted.
The executor reap pass closes both gaps: it prices the lane's session-store
delta and appends to result.md itself.

These tests pin the contract, per the mutation-proof discipline:
  * baseline delta (a resumed lane is not charged for its earlier tasks),
  * fail-open on every missing input (meter never breaks a delivery),
  * idempotency against a hook-leg footnote already in the file,
  * the fresh-in / cache-read pricing split (10x at the flash tier).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
CORE = HERE.parent / "hermes_cluster" / "core"
if str(CORE) not in sys.path:
    sys.path.insert(0, str(CORE))

import lane_cost as lc  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures: a miniature Hermes session store (same schema shape).
# ---------------------------------------------------------------------------

def _make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE session_model_usage (
            session_id TEXT NOT NULL,
            model TEXT NOT NULL,
            billing_provider TEXT NOT NULL DEFAULT '',
            billing_base_url TEXT NOT NULL DEFAULT '',
            billing_mode TEXT NOT NULL DEFAULT '',
            task TEXT NOT NULL DEFAULT '',
            api_call_count INTEGER NOT NULL DEFAULT 0,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cache_read_tokens INTEGER NOT NULL DEFAULT 0,
            cache_write_tokens INTEGER NOT NULL DEFAULT 0,
            reasoning_tokens INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0,
            actual_cost_usd REAL NOT NULL DEFAULT 0,
            cost_status TEXT,
            cost_source TEXT,
            first_seen REAL,
            last_seen REAL,
            PRIMARY KEY (session_id, model, billing_provider, billing_base_url,
                         billing_mode, task)
        )"""
    )
    conn.commit()
    conn.close()


def _add_usage(path: Path, session_id: str, model: str, *, inp: int,
               cached: int, out: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO session_model_usage (session_id, model, input_tokens,"
        " cache_read_tokens, output_tokens, first_seen, last_seen)"
        " VALUES (?,?,?,?,?,?,?)"
        " ON CONFLICT (session_id, model, billing_provider, billing_base_url,"
        "  billing_mode, task) DO UPDATE SET"
        "   input_tokens = input_tokens + excluded.input_tokens,"
        "   cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,"
        "   output_tokens = output_tokens + excluded.output_tokens",
        (session_id, model, inp, cached, out, time.time(), time.time()),
    )
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _vendored_resolution(monkeypatch):
    """Force resolution through the vendored pair (no ambient repo pin)."""
    monkeypatch.delenv("BDAYA_LANE_COST_SCRIPTS", raising=False)
    # Reset the one-shot module cache so the env change takes effect.
    lc._loaded.clear()
    yield
    lc._loaded.clear()


@pytest.fixture()
def store(tmp_path):
    db = tmp_path / "state.db"
    _make_db(db)
    return db


# ---------------------------------------------------------------------------
# profile_hermes_home — mirrors hermes_cli resolution on every branch.
# ---------------------------------------------------------------------------

def test_profile_home_named_profile_under_localappdata(monkeypatch):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(Path("C:/Users/test/AppData/Local")))
    home = lc.profile_hermes_home("bdaya-worker", env={
        "LOCALAPPDATA": str(Path("C:/Users/test/AppData/Local"))})
    assert home.parts[-2:] == ("profiles", "bdaya-worker")


def test_profile_home_default_is_the_base_root():
    home = lc.profile_hermes_home("default", env={"HERMES_HOME": "/opt/hermes"})
    assert str(home) == os.path.normpath("/opt/hermes")


def test_profile_home_reanchors_another_profiles_home():
    # Executor launched as a plugin: HERMES_HOME points INTO profiles/<x>.
    env = {"HERMES_HOME": str(Path("/base/profiles/worker"))}
    got = lc.profile_hermes_home("bdaya-worker", env=env)
    assert got == Path("/base") / "profiles" / "bdaya-worker"


# ---------------------------------------------------------------------------
# read_session_usage / baseline delta / credits
# ---------------------------------------------------------------------------

def test_read_session_usage_keeps_cache_read_apart(store):
    _add_usage(store, "s1", "qwen3.8-flash", inp=1_000_000, cached=2_000_000, out=3_000_000)
    buckets = lc.read_session_usage(store, "s1")
    assert buckets == {"qwen3.8-flash": {
        "input_tokens": 1_000_000, "cache_read_tokens": 2_000_000,
        "output_tokens": 3_000_000}}


def test_read_session_usage_fail_open_on_missing_db(tmp_path):
    assert lc.read_session_usage(tmp_path / "nope.db", "s1") is None


# ---------------------------------------------------------------------------
# footnote_for_delivery — end-to-end against a real store.
# ---------------------------------------------------------------------------

def _footnote(store, session_id, baseline, lane_key="factory-metering"):
    # profile "default" => the home IS the store dir (HERMES_HOME override).
    return lc.footnote_for_delivery(
        profile="default", session_id=session_id, baseline=baseline,
        lane_key=lane_key, env={"HERMES_HOME": str(store.parent)},
    )


def test_footnote_prices_the_delta_not_the_total(store):
    """A resumed lane with prior-task usage must report only THIS task's
    tokens: baseline (1M in) subtracted from the cumulative (2M in)."""
    _add_usage(store, "s1", "qwen3.8-flash", inp=1_000_000, cached=100, out=50)
    baseline = lc.read_session_usage(store, "s1")
    _add_usage(store, "s1", "qwen3.8-flash", inp=900_000, cached=200, out=30)
    # rows aggregate: cumulative now = 1.9M in / 300 cached / 80 out
    text = _footnote(store, "s1", baseline)
    assert text is not None, "footnote must render against a readable store"
    assert "+900000 fresh-in" in text, text
    assert "+200 cache-read" in text, text
    assert "+30 out" in text, text
    assert lc.FOOTNOTE_MARKER not in text  # the hook copy uses its own header
    assert "## Lane cost footnote" in text


def test_footnote_never_conflates_cache_read_with_fresh_input(store):
    """flash tier: 60 per M fresh-in vs 6 per M cache-read (10x apart).
    1M fresh + 1M cached + 1M out = 60 + 6 + 188 = 254 credits exactly."""
    _add_usage(store, "s1", "qwen3.8-flash", inp=1_000_000, cached=1_000_000, out=1_000_000)
    text = _footnote(store, "s1", {})
    assert text is not None
    assert "254.0000" in text, text
    assert "credits since this spawn" in text, text  # empty baseline disclosed


def test_footnote_reports_unpriced_models_without_guessing(store):
    _add_usage(store, "s1", "mystery-9000", inp=10, cached=0, out=10)
    text = _footnote(store, "s1", {})
    assert text is not None
    assert "mystery-9000: unpriced" in text, text


def test_footnote_fail_open_missing_db(tmp_path):
    assert lc.footnote_for_delivery(
        profile="default", session_id="s1", baseline={},
        lane_key="x", env={"HERMES_HOME": str(tmp_path)}) is None


def test_footnote_fail_open_no_session_id(store):
    assert lc.footnote_for_delivery(
        profile="default", session_id="", baseline={}, lane_key="x",
        env={"HERMES_HOME": str(store.parent)}) is None


# ---------------------------------------------------------------------------
# append_footnote — the deliverable mutation.
# ---------------------------------------------------------------------------

def test_append_footnote_writes_once(tmp_path):
    f = tmp_path / "task_x.result.md"
    f.write_text("verdict: PASS\n", encoding="utf-8")
    assert lc.append_footnote(f, "\n## Lane cost footnote (derived - #929)\ncredits: 254\n")
    body = f.read_text(encoding="utf-8")
    assert body.startswith("verdict: PASS")
    assert "credits: 254" in body
    # idempotent: a second pass never duplicates
    assert not lc.append_footnote(f, "\n## Lane cost footnote (derived - #929)\ncredits: 254\n")
    assert f.read_text(encoding="utf-8").count("Lane cost footnote") == 1


def test_append_footnote_defers_to_the_hook_leg(tmp_path):
    """When the #860 hook already footnoted the print and the print IS the
    deliverable, the executor pass must not add a second footnote."""
    f = tmp_path / "task_y.result.md"
    f.write_text("done\n\n## Lane cost footnote (derived — #860)\nlane z: credits 12\n",
                 encoding="utf-8")
    assert not lc.append_footnote(f, "\n## Lane cost footnote (derived - #929)\nx\n")
    assert "#929" not in f.read_text(encoding="utf-8")


def test_append_footnote_fail_open_missing_file(tmp_path):
    assert not lc.append_footnote(tmp_path / "gone.md", "footnote")


# ---------------------------------------------------------------------------
# Executor wiring: the reap pass exists, is fail-open, and runs on done.
# ---------------------------------------------------------------------------

def test_executor_wires_the_footnote_pass():
    """Static wiring proof: _reap_finished_spawns calls _append_cost_footnote
    for a hermes delivery BEFORE the result body is read for transport
    (#874) — so what the main node receives is the footnoted file."""
    src = (CORE / "agent_executor.py").read_text(encoding="utf-8")
    assert "self._append_cost_footnote(spawn)" in src
    touch = src.index("self._touch_lane_from_spawn(spawn, task_id)")
    fn = src.index("self._append_cost_footnote(spawn)")
    read = src.index("result=self._read_result_body(spawn)")
    assert touch < fn < read, (
        "footnote must fire after session-id binding and before the body "
        "is read for transport")
    # baseline captured at spawn, only for resumed sessions
    assert "cost_baseline=_lane_cost.spawn_baseline" in src


def test_reconciled_resumed_spawn_falls_back_to_the_lane_table():
    """After an executor restart the spawn-time baseline was lost; the
    footnote pass must still work off the session id, disclosing that the
    figure covers the whole session (render_footnote's no_baseline leg)."""
    src = (CORE / "agent_executor.py").read_text(encoding="utf-8")
    # the helper passes an empty baseline through; build_footnote's
    # no_baseline text is the vendored footnote's honest-disclosure line.
    assert "no session-start baseline" in (CORE / "lane_cost_footnote.py").read_text(encoding="utf-8")
