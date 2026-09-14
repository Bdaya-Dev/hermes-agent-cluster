"""VENDORED COPY (shared/claude-plugins#929) of
``hermes/plugins/bdaya-enforcement/lane_cost_footnote.py`` from
shared/claude-plugins @ the #860 wire, so the cluster executor can price a
lane's spend with NO claude-plugins checkout on the node. ``lane_cost.py``
prefers the live repo copy when ``BDAYA_LANE_COST_SCRIPTS`` is set and falls
back to this vendored pair; drift is reconciled by
``tests_v3/test_lane_cost_vendored_sync.py`` against the published file.

#860 — lane cost footnote: every task result carries its lane's credit spend.

Issue #860 asks for a per-lane cost report "wired into task results". The
report script itself already exists and is unit-tested
(``hermes/scripts/lane_cost_report.py`` / ``lane_cost_report`` module); what
was missing — the actual gap — is that nothing called it, so no lane result
ever showed a number. This module is that missing wire.

Design (all of it reporting, none of it enforcement — the owner's standing
rule: a lane is never capped or refused for budget; the only sanctioned lever
is downshifting to a cheaper model):

* Registers on ``transform_llm_output`` (the same bounded hook the #859
  note-claim guard uses) and **appends** a footer to the lane's final text::

      ## Lane cost footnote (derived — #860)
      lane <key>: credits this task <D>. <fresh_in> fresh-in / <cached>
      cache-read / <out> out tokens (qwen3.8-flash @ 60/6/188 per M).
      Seat total (all lanes, to <ts>): <N> / 250000 credits (<pct>% of cap).
      DERIVED from this machine's Hermes session store — not authoritative;
      the authoritative seat figure is the ModelStudio OpenAPI
      GetSubscriptionStats. Never a gate: informational only.

  Appending (the note-claim guard *prepends* its marker) and never altering
  the lane's own words: a reporting hook must not edit the report subject.
* **Baseline delta = this task's cost.** ``session_model_usage`` counters are
  cumulative per session, and an LFP-1 lane is long-lived across tasks
  (#854), so the per-task figure is the delta between a baseline captured at
  ``on_session_start`` (fires once per brand-new session, before any usage of
  the first task is recorded) and the totals read at finalization.
* **Cache-read is priced separately, never conflated with fresh input**
  (owner rule #1 item 2; the rates differ 10x — flash 60/6/188). All maths
  delegate to ``lane_cost_report`` so there is exactly one copy of the
  accounting.
* **Session-scoped totals**, not lane-key totals. The lane *key* regex in
  ``lane_cost_report`` deliberately matches structured titles; an actual
  cluster lane title is the brief's first line (e.g. "## Hermes cluster task
  task_..."), which the key regex will not match — so the footnote must work
  from the session id the hook carries. The **seat total** line sums the
  whole store (every session's usage rows), with unpriced models counted out
  and disclosed on the line, so the figure never silently claims more
  coverage than it has.
* **Fail-open everywhere.** DB missing, unreadable, mid-write, no baseline —
  the footer is simply absent, the lane's text passes through untouched. A
  cost meter that breaks lanes is worse than no cost meter.
* **Bounded.** The read is one indexed aggregate against a local SQLite file
  opened read-only with ``busy_timeout``; it rides the dispatcher's
  post-tool budget like the #859 observer does.

## Night-window accounting is NOT claimed here — measurement limit

deepseek-v4-flash carries a 50% night discount between 22:00 and 08:00
UTC+8. Crediting it needs *per-call* timestamps. The session store aggregates
usage per ``(session_id, model, billing, task)`` with only session-level
``first_seen``/``last_seen`` — a session spanning the window boundary cannot
be split. The footnote therefore prices deepseek at the DAY rate and says so
inline; an under-claimed night discount is the conservative direction for a
budget holder. Closing this needs per-call timestamps upstream in Hermes
(tracked in #860), not a guess here.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# The single copy of the accounting maths lives in the scripts dir
# (importable module, unit-tested). The plugin is installed standalone, so
# the location is resolved from env first, then repo-relative for tests/dev.
_LANE_COST_SCRIPTS_ENV = "BDAYA_LANE_COST_SCRIPTS"


def _scripts_dir(env: Optional[Dict[str, str]] = None) -> Optional[Path]:
    env = env if env is not None else os.environ
    pinned = (env.get(_LANE_COST_SCRIPTS_ENV) or "").strip()
    if pinned:
        p = Path(pinned)
        return p if (p / "lane_cost_report.py").is_file() else None
    # this file: hermes/plugins/bdaya-enforcement/lane_cost_footnote.py
    guess = Path(__file__).resolve().parents[2] / "scripts"
    return guess if (guess / "lane_cost_report.py").is_file() else None


_lcr_cache: Dict[str, Any] = {"mod": None}
_lcr_lock = threading.Lock()


def _load_lcr() -> Optional[Any]:
    """Import (once) the lane_cost_report module the scripts dir ships."""
    with _lcr_lock:
        if _lcr_cache["mod"] is None:
            d = _scripts_dir()
            if d is None:
                return None
            if str(d) not in sys.path:
                sys.path.insert(0, str(d))
            try:
                import lane_cost_report  # noqa: PLC0415
            except Exception:
                return None
            _lcr_cache["mod"] = lane_cost_report
        return _lcr_cache["mod"]


def state_db_path(env: Optional[Dict[str, str]] = None) -> Path:
    """``HERMES_HOME/state.db`` (profile-aware, same resolution as the store)."""
    env = env if env is not None else os.environ
    home = (env.get("HERMES_HOME") or "").strip() or str(Path.home() / ".hermes")
    return Path(home) / "state.db"


# ---------------------------------------------------------------------------
# Session-scoped usage read (single aggregate query; read-only; bounded).
# ---------------------------------------------------------------------------

def read_session_usage(db: Any, session_id: str) -> Optional[Dict[str, Dict[str, int]]]:
    """This session's cumulative token buckets per model, or None on any
    read failure (fail-open). Buckets keep fresh input and cache-read apart —
    conflating them is the exact error the accounting must never make."""
    try:
        conn = sqlite3.connect(f"file:{Path(db)}?mode=ro", uri=True, timeout=1.0)
        try:
            rows = conn.execute(
                """
                SELECT model,
                       SUM(input_tokens), SUM(cache_read_tokens), SUM(output_tokens)
                FROM session_model_usage
                WHERE session_id = ?
                GROUP BY model
                """,
                (session_id,),
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return None
    out: Dict[str, Dict[str, int]] = {}
    for model, inp, cached, outp in rows:
        out[model or ""] = {
            "input_tokens": int(inp or 0),
            "cache_read_tokens": int(cached or 0),
            "output_tokens": int(outp or 0),
        }
    return out


def read_model_totals(db: Any) -> Optional[Dict[str, Dict[str, int]]]:
    """Store-wide cumulative buckets per model (the seat-total leg). Same
    fail-open shape as read_session_usage."""
    try:
        conn = sqlite3.connect(f"file:{Path(db)}?mode=ro", uri=True, timeout=1.0)
        try:
            rows = conn.execute(
                """
                SELECT model,
                       SUM(input_tokens), SUM(cache_read_tokens), SUM(output_tokens)
                FROM session_model_usage
                GROUP BY model
                """
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return None
    out: Dict[str, Dict[str, int]] = {}
    for model, inp, cached, outp in rows:
        out[model or ""] = {
            "input_tokens": int(inp or 0),
            "cache_read_tokens": int(cached or 0),
            "output_tokens": int(outp or 0),
        }
    return out


# ---------------------------------------------------------------------------
# Pure maths — everything here is unit-testable against a dict, no DB.
# ---------------------------------------------------------------------------

def lane_key_from_env(env: Optional[Dict[str, str]] = None) -> str:
    """The lane key the executor injects (same env spellings the #859 guard
    reads); empty when this session is not a keyed lane."""
    env = env if env is not None else os.environ
    for k in ("BDAYA_LANE_KEY", "BDAYA_LANE", "HERMES_LANE_KEY"):
        v = (env.get(k) or "").strip()
        if v:
            return v
    return ""


def usage_credits(lcr: Any, buckets: Dict[str, Dict[str, int]]) -> Tuple[float, List[str]]:
    """Credits for token buckets at Token Plan rates + notes for models with
    no mapped tier (reported unpriced, never guessed)."""
    total = 0.0
    notes: List[str] = []
    for model, t in buckets.items():
        credits, _rates = lcr.credits_for({"model": model, **t})
        if credits is None:
            notes.append(f"{model}: unpriced (no Token Plan tier)")
        else:
            total += credits
    return total, notes


def subtract(
    now: Dict[str, Dict[str, int]], before: Dict[str, Dict[str, int]]
) -> Dict[str, Dict[str, int]]:
    """Per-model token delta (now - before), clamped at 0 — counters only
    grow; a resumed snapshot from a re-provisioned store can make `before`
    stale-high, and a negative bucket would poison the maths."""
    models = set(now) | set(before)
    out: Dict[str, Dict[str, int]] = {}
    for m in models:
        n, b = now.get(m, {}), before.get(m, {})
        d = {
            "input_tokens": max(0, n.get("input_tokens", 0) - b.get("input_tokens", 0)),
            "cache_read_tokens": max(
                0, n.get("cache_read_tokens", 0) - b.get("cache_read_tokens", 0)
            ),
            "output_tokens": max(0, n.get("output_tokens", 0) - b.get("output_tokens", 0)),
        }
        if any(d.values()):
            out[m] = d
    return out


# One month of the seat every lane shares: 250,000 credits (~$200). Owner
# figure from #860; a console-read cap, not an API fact — the % line says
# "of cap", and no consumer may treat it as a limit to enforce against.
SEAT_CAP_CREDITS = 250_000.0

# deepseek night discount: 50% between 22:00 and 08:00 UTC+8. See module
# docstring — per-call attribution is impossible with the store's aggregate
# counters, so the footnote prices the DAY rate and discloses it. The window
# itself is pinned by tests elsewhere; do not "fix" a conversion here without
# reading #804 B2 (22:00–08:00 UTC+8 = 14:00–00:00 UTC — an earlier plan
# computed 16:00–00:00 UTC and shipped a wrong window once already).
_DEEPSEEK_NIGHT_NOTE = "night window 22:00-08:00 UTC+8 not attributable per-call; priced at day rate"


def render_footnote(
    lcr: Any,
    lane_key: str,
    task_delta: Dict[str, Dict[str, int]],
    task_credits: float,
    task_notes: List[str],
    seat_credits: Optional[float],
    seat_notes: Optional[List[str]] = None,
    no_baseline: bool = False,
    ts: Optional[float] = None,
) -> str:
    """The footer text. Never contains a refusal/blocking word — it reports."""
    ts = ts if ts is not None else time.time()
    lines: List[str] = []
    lines.append("")
    lines.append("## Lane cost footnote (derived — #860)")
    ident = lane_key if lane_key else "unkeyed lane"
    lines.append(f"lane {ident}:")
    if not task_delta:
        lines[-1] += " no usage rows attributed to this task by the session store yet."
    else:
        def _priced(kv):
            c, _r = lcr.credits_for({"model": kv[0], **kv[1]})
            return -(c if c is not None else 0.0)

        for model, t in sorted(task_delta.items(), key=_priced):
            tier = lcr._tier_for_model(model)
            rate_s = ""
            if tier:
                rate_s = f" @ {int(tier[1][0])}/{int(tier[1][1])}/{int(tier[1][2])} per M"
            if "deepseek" in model.lower():
                # day rate is what tier says; the night discount is simply
                # not attributable — disclose it on the priced line too.
                rate_s += f" ({_DEEPSEEK_NIGHT_NOTE})"
            lines.append(
                f"- {model}: +{t['input_tokens']} fresh-in / "
                f"+{t['cache_read_tokens']} cache-read / +{t['output_tokens']} out"
                f"{rate_s}"
            )
        if no_baseline:
            lines.append(
                f"  credits since this spawn: {task_credits:.4f} "
                "(no session-start baseline — resumed lane; figure covers "
                "everything this process recorded, not the full task)"
            )
        else:
            lines.append(f"  credits this task: {task_credits:.4f}")
    for n in task_notes:
        lines.append(f"- {n}")
    if seat_credits is not None:
        pct = 100.0 * seat_credits / SEAT_CAP_CREDITS
        stamp = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))
        seat_line = (
            "Seat total (all sessions in this store, to "
            + stamp + "): "
            + f"{seat_credits:.0f} / {SEAT_CAP_CREDITS:.0f} credits ({pct:.1f} percent of cap)."
        )
        if seat_notes:
            seat_line += (
                " Understated: " + str(len(seat_notes))
                + " model bucket(s) unpriced, excluded."
            )
        lines.append(seat_line)
    lines.append(
        "DERIVED from this machine's Hermes session store — not authoritative; "
        "the authoritative seat figure is the ModelStudio OpenAPI "
        "GetSubscriptionStats (SeatCredits/SeatRemainingCredits, aliyun CLI "
        "'modelstudio get-subscription-stats'). Reporting only, never a gate "
        "(owner rule: downshift models to defend the seat, never cap a lane)."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Hook plumbing
# ---------------------------------------------------------------------------

_baseline: Dict[str, Dict[str, Dict[str, int]]] = {}
_baseline_lock = threading.Lock()


def lane_cost_footnote_session_start(**payload: Any) -> None:
    """on_session_start: snapshot this session's cumulative usage as the
    baseline the first task's delta subtracts from. Fires once per brand-new
    session; a resumed lane re-baselines at its spawn (the delta then covers
    only what THIS task's process recorded — under-reporting a resumed
    lane's mid-history is the honest direction; never guess the gap)."""
    try:
        sid = str(payload.get("session_id") or "")
        if not sid:
            return
        lcr = _load_lcr()
        if lcr is None:
            return
        db = state_db_path()
        if not db.exists():
            return
        snap = read_session_usage(db, sid)
        if snap is not None:
            with _baseline_lock:
                _baseline[sid] = snap
    except Exception:
        pass  # a meter must never break a session start
    return None


def lane_cost_footnote_transform(**payload: Any) -> Any:
    """transform_llm_output: append the cost footer to the lane's final text.
    Returns None (text untouched) on any missing input or read failure."""
    try:
        final_response = payload.get("response_text")
        if not isinstance(final_response, str) or not final_response:
            return None
        sid = str(payload.get("session_id") or "")
        if not sid:
            return None
        lcr = _load_lcr()
        if lcr is None:
            return None
        db = state_db_path()
        if not db.exists():
            return None
        now = read_session_usage(db, sid)
        if now is None:
            return None
        with _baseline_lock:
            before = _baseline.get(sid)
        delta = subtract(now, before or {})
        task_credits, notes = usage_credits(lcr, delta)
        totals = read_model_totals(db)
        seat_credits: Optional[float] = None
        seat_notes: List[str] = []
        if totals is not None:
            seat_credits, seat_notes = usage_credits(lcr, totals)
        text = render_footnote(
            lcr, lane_key_from_env(), delta, task_credits, notes,
            seat_credits, seat_notes=seat_notes,
            no_baseline=before is None,
        )
        return final_response + text
    except Exception:
        return None


def reset_baselines() -> None:
    """Test seam: drop all captured baselines."""
    with _baseline_lock:
        _baseline.clear()


__all__ = [
    "SEAT_CAP_CREDITS",
    "state_db_path",
    "lane_key_from_env",
    "read_session_usage",
    "read_model_totals",
    "usage_credits",
    "subtract",
    "render_footnote",
    "lane_cost_footnote_session_start",
    "lane_cost_footnote_transform",
    "reset_baselines",
]
