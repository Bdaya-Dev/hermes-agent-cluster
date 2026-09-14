"""hermes_cluster/core/deliverable_guard.py — shared/claude-plugins#870.

Content-level deliverable guards for the hermes worker reap path.

main's success gate is ``contents.strip()`` — ANY non-whitespace byte in
result.md reads as a completed deliverable. Three production lanes that never
ran were reported `completed`: two wrote only a provider/transport error, one
wrote only its own dispatched brief echoed back (a REVIEWER lane — a merge
gate reading 'completed' as a pass would merge an MR nobody reviewed).

The guards here distinguish a deliverable from those shapes. They are
deliberately conservative: a FALSE POSITIVE is worse than the bug (a guard
that rejects real work re-queues good lanes and burns quota), so each rule
requires the failure shape to be the body's dominant structure, not merely
present in it.

Two rules:

  PROVIDER/TRANSPORT SHAPE — ``classify_non_deliverable`` matches the
  upstream terminal-failure sentence at the START of the body
  (``API call failed after N retries: …`` — agent/turn_recovery.py builds
  exactly ``f"API call failed after {max_retries} retries: {summary}"``).
  The word "error", or that phrase mid-prose, never matches: a real
  deliverable legitimately says things like "the MR originally errored —
  fixed". An ``is_error_response`` turn flag upgrades a merely-mid-body
  occurrence to a rejection (the caller knows the turn failed); without
  the flag the anchored shape alone fires.

  BRIEF ECHO — a body that is nothing but (a truncation of) the dispatched
  brief. Detection is whitespace/case-INSENSITIVE containment of the whole
  normalised body inside the normalised brief, allowing at most 16
  normalised chars of cut junk at each end. The observed production echo
  starts mid-word (one stray leading character, then runs to the brief's
  exact end), so an exact byte-slice test misses it and a line-by-line test
  misses the mid-line start; the trim budget covers word-sized cut artifacts
  while requiring the body to be ~entirely brief text. False-positive safety:
  ANY original prose at either end of the body defeats containment — a
  deliverable that merely QUOTES its brief sandwiches foreign text around
  the quotation, while an echo contains none. A floor of 120 normalised
  chars keeps two-word answers out of scope entirely.

  NO-TURN STDERR — ``has_no_turn_stderr`` detects hermes's
  ``Session <id> found but has no messages. Starting fresh.``
  (cli_agent_setup_mixin.py), which fires ONLY when a resumed session
  restored zero messages — the strongest cheap signal that the agent
  produced no turn. It is NOT used alone to veto a body: a resumed session
  with a real deliverable still wrote it, so the executor treats it as
  corroboration plus an operational WARNING (see _reap_hermes_spawn).
"""

from __future__ import annotations

import re
from typing import Optional

# The upstream terminal provider-failure sentence (hermes-agent
# agent/turn_recovery.py: `_final_response = f"API call failed after
# {max_retries} retries: {final_summary}"`). Anchored at body start; the
# colon form is load-bearing — mid-prose mentions ("an 'API call failed'
# branch was fixed") must not fire.
_PROVIDER_SHAPE = re.compile(
    r"^API call failed after \d+ retries:\s",
    re.IGNORECASE,
)

# hermes stderr signature: a resumed session restored zero messages
# (hermes_cli/cli_agent_setup_mixin.py:470). Exact enough to be free of
# false hits; the id is printed and must not be captured.
_NO_TURN_LINE = re.compile(
    r"Session \S+ found but has no messages\. Starting fresh\.",
)


def _flat(s: str) -> str:
    """Whitespace-free, lowered — the containment key."""
    return re.sub(r"\s+", "", s).lower()


# Bounded truncation budget (in normalised chars) allowed at each end of the
# body before containment is required. The production echo lost ONE character
# of alignment at its head (a mid-word, mid-backtick cut); 16 covers
# word-sized cut artifacts while still requiring the body to be ~entirely
# brief text.
_ECHO_TRIM_BUDGET = 16

# A body under this many normalised chars is never judged an echo: containment
# of a two-word answer ("yes", "done") inside its own brief is meaningless,
# and every observed echo is orders of magnitude longer.
_ECHO_MIN_FLAT = 120


def _is_echo_of_brief(content: str, brief_text: str) -> bool:
    """True when the body is the dispatched brief echoed back.

    The entire normalised body must sit inside the normalised brief —
    allowing at most ``_ECHO_TRIM_BUDGET`` normalised chars of truncation
    junk at each end (the observed production echo starts mid-word with one
    stray character, its remainder running exactly to the brief's end).
    Any original prose at either end defeats containment, so a deliverable
    that merely QUOTES its brief passes: quoting puts foreign text around
    the quotation; echo puts none anywhere.
    """
    cb = _flat(brief_text)
    cr = _flat(content)
    if len(cr) < _ECHO_MIN_FLAT or len(cb) < _ECHO_MIN_FLAT:
        return False
    for lead in range(_ECHO_TRIM_BUDGET + 1):
        head = cr[lead:]
        if not head:
            break
        # cheapest form: containment of some tail-trimmed slice
        for tail in range(_ECHO_TRIM_BUDGET + 1):
            core = head[: len(head) - tail] if tail else head
            if core and core in cb:
                return True
    return False


def _is_provider_shape(content: str, is_error_response: bool = False) -> bool:
    """True when the body is (headed by) an upstream provider-failure line.

    The anchored test alone is conservative: a deliverable must not START
    with the failure sentence unless it is one. With the caller's
    ``is_error_response`` flag (the turn failed upstream), a body merely
    CONTAINING the sentence also fires — the flag makes the match a fact
    about the run, not about a substring.
    """
    stripped = content.lstrip()
    if _PROVIDER_SHAPE.match(stripped):
        return True
    if is_error_response:
        return bool(_PROVIDER_SHAPE.search(content))
    return False


def classify_non_deliverable(
    content: str,
    brief_text: str,
    is_error_response: bool = False,
) -> Optional[str]:
    """Return a short failure reason when `content` is NOT a deliverable.

    Returns None for empty/whitespace bodies — that half of the bug was
    already fixed on main (`contents.strip()` → no_result); this guard only
    judges non-empty content. Reasons are stable tokens so tests and
    operators can tell which shape fired:

      'provider_error' — the body is an upstream provider/transport failure
      'brief_echo'     — the body is the dispatched brief echoed back
    """
    if not content or not content.strip():
        return None
    if _is_provider_shape(content, is_error_response=is_error_response):
        return "provider_error"
    if _is_echo_of_brief(content, brief_text):
        return "brief_echo"
    return None


def has_no_turn_stderr(stderr_text: str) -> bool:
    """True when stderr carries hermes' zero-message resume signature."""
    return bool(stderr_text and _NO_TURN_LINE.search(stderr_text))
