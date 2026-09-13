#!/usr/bin/env python3
"""#929 — the cluster executor's per-lane cost footnote.

shared/claude-plugins#860 wired the per-lane credit footnote into lane
results via the bdaya-enforcement ``transform_llm_output`` hook (the #929
pack-repin + re-rollout gets that hook onto every node). This module is the
EXECUTOR-SIDE safety net so a cluster lane result carries its own credit
figure even when the hook leg is dead on a node:

  * pinned pack predates #860 (the fleet's exact state at the time of
    sampling — 29 completed client lanes, zero footnotes),
  * the hook installed but the scripts dir was never pinned
    (``BDAYA_LANE_COST_SCRIPTS`` absent -> the footnote fails open silently),
  * or the lane delivered via write_file while its printed final text (the
    only thing ``transform_llm_output`` can touch) never became the deliverable
    (shared/claude-plugins#868).

Contract (mirrors the #860 footnote — reporting only, never a gate):
  * one shared module, ``lane_cost_footnote.py``, so the pricing maths has
    EXACTLY ONE copy (lane_cost_report.py semantics; the hook and the
    executor both import it);
  * per-task figure = this task's session usage minus the baseline recorded
    at spawn time (the executor knows exactly when its lane started, which
    the hook does not);
  * fail-open: missing store, unreadable DB, no usage rows -> the result file
    passes through byte-identical;
  * idempotent: an already-footnoted file is never footnoted twice;
  * appends to the DELIVERABLE file at reap — the executor's own write, so
    it cannot be lost the way a print can.

Never prints a secret; reads only the local profile state.db read-only.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

FOOTNOTE_MARKER = "## Lane cost footnote (derived - #929)"

# Resolution order for the modules carrying the accounting maths:
#   1. BDAYA_LANE_COST_SCRIPTS           — the #860 env (repo hermes/scripts),
#      which must contain both lane_cost_report.py and lane_cost_footnote.py.
#   2. next to this file                 — vendored copies shipped inside the
#      cluster package, so the executor works with no claude-plugins checkout
#      on PATH at all.
_CANDIDATES_ENV = "BDAYA_LANE_COST_SCRIPTS"

_loaded: Dict[str, object] = {}


def _candidate_dirs() -> list[Path]:
    dirs: list[Path] = []
    pinned = (os.environ.get(_CANDIDATES_ENV) or "").strip()
    if pinned:
        dirs.append(Path(pinned))
    dirs.append(Path(__file__).resolve().parent)
    return dirs


def _load(name: str, marker_attr: str) -> Optional[object]:
    """Import module `name` (once) from the first candidate dir that has it."""
    if _loaded.get(name) is not None:
        return _loaded[name]  # type: ignore[return-value]
    for d in _candidate_dirs():
        f = d / f"{name}.py"
        if not f.is_file():
            continue
        s = str(d)
        if s not in sys.path:
            # Prepend: this dir wins over any same-named module elsewhere.
            # lane_cost_footnote imports lane_cost_report by name, so the
            # winning dir must be on sys.path BEFORE that lookup runs.
            sys.path.insert(0, s)
        try:
            import importlib

            mod = importlib.import_module(name)
            if hasattr(mod, marker_attr):
                _loaded[name] = mod
                return mod
        except Exception:
            return None
    return None


def profile_hermes_home(profile: str, env: Optional[Dict[str, str]] = None) -> Path:
    """The profile home a ``hermes -p <profile>`` child session writes into.

    Mirrors hermes_cli profile resolution: the base root is ``HERMES_HOME``
    when set, else ``~/.hermes`` (POSIX) / ``%LOCALAPPDATA%\\hermes`` (Windows).
    When ``HERMES_HOME`` itself points INTO a profile dir (parent named
    ``profiles`` — the state the executor inherits when launched as a Hermes
    plugin), it is re-anchored to the base root first, so a named profile
    always resolves to ``<root>/profiles/<name>``.
    """
    env = env if env is not None else os.environ
    home = (env.get("HERMES_HOME") or "").strip()
    if home:
        root = Path(home)
        if root.parent.name == "profiles":
            root = root.parent.parent  # re-anchor: another profile's home
    elif os.name == "nt":
        local = (env.get("LOCALAPPDATA") or "").strip()
        root = (Path(local) / "hermes") if local else Path.home() / ".hermes"
    else:
        root = Path.home() / ".hermes"
    if not profile or profile == "default":
        return root
    return root / "profiles" / profile


def state_db_path(profile: str, env: Optional[Dict[str, str]] = None) -> Path:
    return profile_hermes_home(profile, env) / "state.db"


def read_session_usage(db: Path, session_id: str) -> Optional[Dict[str, Dict[str, int]]]:
    """Per-model cumulative token buckets for one session, or None on failure."""
    lcf = _load("lane_cost_footnote", "read_session_usage")
    if lcf is not None:
        return lcf.read_session_usage(db, session_id)  # type: ignore[attr-defined]
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


def build_footnote(
    *,
    lane_key: str,
    session_id: str,
    baseline: Dict[str, Dict[str, int]],
    db: Path,
    now: Optional[float] = None,
) -> Optional[str]:
    """The footnote text for one cluster delivery, or None when nothing can be
    said honestly (no maths module, no DB, no rows)."""
    lcr = _load("lane_cost_report", "credits_for")
    lcf = _load("lane_cost_footnote", "render_footnote")
    if lcr is None or lcf is None:
        return None
    now = now if now is not None else time.time()
    totals = lcf.read_model_totals(db)  # type: ignore[attr-defined]
    if totals is None:
        return None
    session = read_session_usage(db, session_id)
    if session is None:
        return None
    delta = lcf.subtract(session, baseline)  # type: ignore[attr-defined]
    task_credits, notes = lcf.usage_credits(lcr, delta)  # type: ignore[attr-defined]
    seat_credits, seat_notes = lcf.usage_credits(lcr, totals)  # type: ignore[attr-defined]
    text = lcf.render_footnote(  # type: ignore[attr-defined]
        lcr,
        lane_key,
        delta,
        task_credits,
        notes,
        seat_credits,
        seat_notes=seat_notes,
        no_baseline=not baseline,
        ts=now,
    )
    return text


def append_footnote(result_path: Path, footnote: str) -> bool:
    """Append the footnote to the deliverable. Idempotent, fail-open.

    Returns True when the file was modified. Same tmp-then-rename discipline
    lanes use; on Windows a rename over an open file fails, in which case we
    fall back to an in-place append (the executor closed its own handles
    before reap reached here, #868).
    """
    try:
        text = result_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    if FOOTNOTE_MARKER in text or "## Lane cost footnote (derived" in text:
        return False  # already footnoted (hook leg or a prior reap pass)
    body = text.rstrip("\n") + "\n" + footnote.rstrip("\n") + "\n"
    tmp = result_path.with_name(f".hermes-footnote-tmp.{os.getpid()}")
    try:
        tmp.write_text(body, encoding="utf-8")
        os.replace(tmp, result_path)
        return True
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            with open(result_path, "a", encoding="utf-8") as fh:
                fh.write(footnote.rstrip("\n") + "\n")
            return True
        except OSError:
            return False


def spawn_baseline(profile: str, session_id: str, env: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, int]]:
    """Cumulative usage of `session_id` right now — recorded at spawn so the
    reap-time delta is exactly this delivery's spend. Empty dict when the
    session has no rows yet or the store is unreadable (the footnote then
    reports the full session total and says so)."""
    if not session_id:
        return {}
    db = state_db_path(profile, env)
    if not db.is_file():
        return {}
    snap = read_session_usage(db, session_id)
    return snap or {}


def footnote_for_delivery(
    *,
    profile: str,
    session_id: str,
    baseline: Dict[str, Dict[str, int]],
    lane_key: str,
    env: Optional[Dict[str, str]] = None,
    now: Optional[float] = None,
) -> Optional[str]:
    """End-to-end: the footnote for one finished delivery, or None (fail-open).

    The DB is resolved through the same env (HERMES_HOME override honored) so
    a node can point the meter at an alternate store without a code change.
    """
    lcf = _load("lane_cost_footnote", "render_footnote")
    lcr = _load("lane_cost_report", "credits_for")
    if lcf is None or lcr is None or not session_id:
        return None
    db = state_db_path(profile, env)
    if not db.is_file():
        return None
    return build_footnote(
        lane_key=lane_key, session_id=session_id, baseline=baseline, db=db, now=now
    )


__all__ = [
    "FOOTNOTE_MARKER",
    "profile_hermes_home",
    "state_db_path",
    "read_session_usage",
    "spawn_baseline",
    "build_footnote",
    "footnote_for_delivery",
    "append_footnote",
]
