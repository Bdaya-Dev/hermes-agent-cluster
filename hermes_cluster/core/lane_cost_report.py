#!/usr/bin/env python3
"""Per-lane cost report for LFP-1 Hermes lanes (owner rule #1, item 2).

Reads Hermes' per-session token usage straight from the installed session store
and prints, per lane key (the session title a stateful lane runs under):

  fresh input tokens, cache-read tokens, output tokens, and credits at the
  Token Plan rates (per million tokens):

    qwen3.8-flash .......... 60 /   6 /  188   (fresh input / cache-read / output)
    qwen3.7-plus ........... 160 /  16 /  640
    deepseek-v4-flash-0731 .  80 /   8 /  160

A "lane" is a long-lived Hermes session titled by its ``lane_key``
(``<repo>#<branch>`` author / ``<repo>!<mr>`` reviewer) -- the fork executor
spawns it with ``-c <lane_key> --create-if-missing`` and resumes it by session
id (hermes-agent-cluster PR#16). Sessions from the ``bdaya-worker`` profile
carry the lane key as their title.

Where Hermes records the numbers this script reads (all cited against the
installed hermes v0.21.1 source):

* ``hermes_state.py:157,175`` -- the session DB lives at
  ``HERMES_HOME/state.db``.
* ``hermes_state_schema.py:68`` -- the ``session_model_usage`` table carries
  ``input_tokens`` / ``output_tokens`` / ``cache_read_tokens`` (plus
  ``cache_write_tokens``, ``reasoning_tokens``) per ``(session_id, model)``.
* ``hermes_state_usage.py:17`` -- ``_TOKEN_COUNTERS`` names the same fields.
* ``hermes_cli/main.py:1323`` -- ``_create_titled_session`` titles the lane's
  session with the lane key (``set_session_title``); a plain
  ``-q "<brief>" --oneshot`` run carries no such title and is reported under
  its ``session_id`` instead.
* ``hermes_cli/_parser.py:227-241`` -- ``--resume <session_id>`` /
  ``-c <key> --create-if-missing`` are the lane continuation primes.

Usage:
  python lane-cost-report.py [--home HERMES_HOME] [--db PATH] [--json]
  python lane-cost-report.py --home ~/.hermes
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Token Plan rates per million tokens: (fresh input, cache-read, output).
# Source: the LFP-1 token plan (owner rule #1 brief; per-model tier).
TOKEN_PLAN_TIERS: List[Tuple[str, str, Tuple[float, float, float]]] = [
    ("flash", "qwen3.8-flash", (60.0, 6.0, 188.0)),
    ("plus", "qwen3.7-plus", (160.0, 16.0, 640.0)),
    ("deepseek-flash", "deepseek-v4-flash-0731", (80.0, 8.0, 160.0)),
]

# Lane-key shapes: author `<repo>#<branch>`, reviewer `<repo>!<mr>`.
AUTHOR_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-/]+#[A-Za-z0-9_.\-/]+$")
REVIEWER_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-/]+!\d+$")


def _tier_for_model(model: str) -> Optional[Tuple[str, Tuple[float, float, float]]]:
    normalized = (model or "").strip().lower()
    for tier, needle, rates in TOKEN_PLAN_TIERS:
        aliases = (needle, needle.split(".", 1)[1] if "." in needle else needle)
        if any(alias in normalized for alias in aliases) or normalized == needle:
            return tier, rates
    return None


def default_state_db(home: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> Path:
    """Resolve ``HERMES_HOME/state.db`` (env-agnostic for tests)."""
    env = env if env is not None else os.environ
    home_raw = home or (env.get("HERMES_HOME") or "").strip()
    if not home_raw:
        home_raw = str(Path.home() / ".hermes")
    return Path(home_raw) / "state.db"


def recover_lane_key(row: sqlite3.Row) -> Tuple[str, Optional[str]]:
    """Return (lane_key, role) for a session row.

    A lane session is titled by its key (``main.py:1323``). Fall back to
    composing ``<repo>#<branch>`` from the git repo/branch columns, else report
    the session by its id as an unkeyed one-off.
    """
    title = (row["title"] or "").strip()
    if AUTHOR_KEY_RE.fullmatch(title):
        return title, "author"
    if REVIEWER_KEY_RE.fullmatch(title):
        return title, "reviewer"
    repo = (row["git_repo_root"] or "").strip()
    branch = (row["git_branch"] or "").strip()
    if repo and branch:
        key = f"{Path(repo).name}#{branch}"
        return key, "author"
    return f"session:{row['id']}", None


@dataclass
class LaneUsage:
    lane_key: str
    role: Optional[str]
    # per-model token buckets
    model_tokens: List[Dict[str, Any]]

    @property
    def total_input(self) -> int:
        return sum(int(m.get("input_tokens") or 0) for m in self.model_tokens)

    @property
    def total_cache_read(self) -> int:
        return sum(int(m.get("cache_read_tokens") or 0) for m in self.model_tokens)

    @property
    def total_output(self) -> int:
        return sum(int(m.get("output_tokens") or 0) for m in self.model_tokens)


def load_usage(db_path: Any) -> List[LaneUsage]:
    """Aggregate ``session_model_usage`` by lane key (session title)."""
    db = Path(db_path)
    if not db.exists():
        raise FileNotFoundError(f"session DB not found: {db}")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        session_rows = conn.execute(
            """
            SELECT id, title, git_repo_root, git_branch
            FROM sessions
            """
        ).fetchall()
        usage_rows = conn.execute(
            """
            SELECT session_id, model, input_tokens, cache_read_tokens, output_tokens
            FROM session_model_usage
            """
        ).fetchall()
    finally:
        conn.close()

    session_by_id = {r["id"]: r for r in session_rows}
    lanes: Dict[str, LaneUsage] = {}
    for u in usage_rows:
        sess = session_by_id.get(u["session_id"])
        if sess is None:
            lane_key = f"session:{u['session_id']}"  # usage row with no session row
            role = None
        else:
            lane_key, role = recover_lane_key(sess)
        lane = lanes.setdefault(
            lane_key,
            LaneUsage(lane_key=lane_key, role=role, model_tokens=[]),
        )
        lane.model_tokens.append(
            {
                "model": u["model"],
                "input_tokens": int(u["input_tokens"] or 0),
                "cache_read_tokens": int(u["cache_read_tokens"] or 0),
                "output_tokens": int(u["output_tokens"] or 0),
            }
        )
    return sorted(lanes.values(), key=lambda l: (l.lane_key,))


def credits_for(tokens: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """Total credits for one model bucket at the Token Plan rate, or None
    when the model has no mapped tier (shown unpriced rather than guessed)."""
    tier = _tier_for_model(tokens.get("model") or "")
    if tier is None:
        return None, None
    _name, rates = tier
    inp = int(tokens.get("input_tokens") or 0)
    cached = int(tokens.get("cache_read_tokens") or 0)
    out = int(tokens.get("output_tokens") or 0)
    return (inp / 1e6 * rates[0] + cached / 1e6 * rates[1] + out / 1e6 * rates[2]), rates


def render_csv(lanes: List[LaneUsage]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for lane in lanes:
        for m in lane.model_tokens:
            credits, _rates = credits_for(m)
            out.append(
                {
                    "lane_key": lane.lane_key,
                    "role": lane.role,
                    "model": m["model"],
                    "fresh_input_tokens": m["input_tokens"],
                    "cache_read_tokens": m["cache_read_tokens"],
                    "output_tokens": m["output_tokens"],
                    "credits": round(credits, 6) if credits is not None else None,
                }
            )
    return out


def render_text(lanes: List[LaneUsage]) -> str:
    lines: List[str] = []
    lines.append(f"{'lane_key':<38} {'role':<9} {'model':<24} "
                 f"{'fresh_in':>10} {'cache_read':>10} {'output':>9} {'credits':>10}")
    lines.append("-" * 120)
    for lane in lanes:
        for m in lane.model_tokens:
            credits, _rates = credits_for(m)
            credit_s = f"{credits:.4f}" if credits is not None else "n/a"
            lines.append(
                f"{lane.lane_key:<38} {(lane.role or ''):<9} {m['model'][:24]:<24} "
                f"{m['input_tokens']:>10} {m['cache_read_tokens']:>10} "
                f"{m['output_tokens']:>9} {credit_s:>10}"
            )
        if not lane.model_tokens:
            lines.append(f"{lane.lane_key:<38} {(lane.role or ''):<9} (no usage rows)")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Per-lane token/credit report from the Hermes session DB."
    )
    parser.add_argument("--home", help="HERMES_HOME (default: $HERMES_HOME or ~/.hermes)")
    parser.add_argument("--db", help="explicit path to state.db")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else default_state_db(args.home)
    try:
        lanes = load_usage(db_path)
    except (FileNotFoundError, sqlite3.DatabaseError, OSError) as exc:
        print(f"lane-cost-report: cannot read {db_path}: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(render_csv(lanes), indent=2))
    else:
        print(render_text(lanes))
        print("\nRates (per M): flash 60/6/188, plus 160/16/640, "
              "deepseek-flash 80/8/160 (fresh input/cache-read/output).")
    return 0


if __name__ == "__main__":
    sys.exit(main())