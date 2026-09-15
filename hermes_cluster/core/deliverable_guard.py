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


def _is_stranded_verdict(content: str) -> bool:
    """True when a REVIEWER's own result body confesses the verdict was never
    posted (shared/claude-plugins#913).

    Production specimen (task_9b8910e693ff8c1a, PR #70): a full correct PASS
    plus the sentence "Could not post verdict as PR comment — gh CLI not
    authenticated on this node... Verdict delivered via this result file
    only." The task reached `completed`; a merge gate reading PR comments saw
    nothing — indistinguishable from a review that never ran. Honesty is what
    makes this detectable: the confession is the body's OWN statement about
    THIS posting, so the rule requires a confession SHAPE, in either order,
    anchored to the verdict/comment noun — and it fires only for reviewer-role
    reaps (the caller's scope, #913's fix-2 scoping decision: an author lane
    legitimately reporting a failed post elsewhere is a real deliverable).

    False-positive safety (the #870 lesson): discussing a posting hazard, or
    reporting posting SUCCESS, must pass. Rejection therefore requires the
    self-referential pairing — a not-post verb immediately adjacent to a
    VERDICT noun (the reviewer's deliverable: "could not post verdict",
    "verdict ... delivered via this result file", "couldn't post the verdict
    comment"), or a not-post verb plus an explicit posting-noun phrase
    ("could not post it as PR comment", "failed to post the verdict note").
    A bare mention of a posting problem anywhere else — "a reviewer could not
    post verdicts if ..." as a generalization inside a PASS body whose own
    verdict evidently posted — pairs a modal/hypothetical, not a confession;
    the anchored possessive/self-reference shape cannot match it.
    """
    c = content
    _NOTPOST = (r"(?:couldn'?t|could\s*not|can'?t|cannot|was\s+not\s+able\s+to|"
                r"were\s+not\s+able\s+to|am\s+not\s+able\s+to|failed\s+to|"
                r"unable\s+to)\s+post\b")
    confession = (
        # "could not post (the|your|this|my|a|its|any) verdict ...":
        re.search(_NOTPOST + r"\s+(?:the|your|this|my|a|an|its|any)?\s*verdict\b",
                  c, re.IGNORECASE)
        # the production tail, verbatim shape:
        or re.search(r"verdict\s+(?:was\s+)?delivered\s+via\s+this\s+result\s+file",
                     c, re.IGNORECASE)
        # "could not post it as (a|this|the) PR comment":
        or re.search(_NOTPOST + r"\s+it\s+as\s+(?:a|this|the)?\s*(?:PR|MR)?\s*"
                     r"(?:comment|note)\b", c, re.IGNORECASE)
        # "posting failure: verdict never posted":
        or re.search(r"posting\s+(?:failure|failed)[^.]{0,80}\bverdict\s+never\s+posted\b",
                     c, re.IGNORECASE)
    )
    return bool(confession)


def classify_non_deliverable(
    content: str,
    brief_text: str,
    is_error_response: bool = False,
    role: str = "",
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
    # #913: reviewer-role only — a verdict the lane itself says it could not
    # post is not a deliverable to a merge gate (its whole value was the
    # posted comment). Author-role bodies are NOT judged here: an author
    # reporting a failed post elsewhere is a legitimate deliverable.
    if role.strip().lower() == "reviewer" and _is_stranded_verdict(content):
        return "stranded_verdict"
    return None


def has_no_turn_stderr(stderr_text: str) -> bool:
    """True when stderr carries hermes' zero-message resume signature."""
    return bool(stderr_text and _NO_TURN_LINE.search(stderr_text))
