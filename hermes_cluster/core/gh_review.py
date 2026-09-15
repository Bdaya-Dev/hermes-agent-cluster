"""gh_review — the reviewer lane records its GitHub verdict as the
`bdaya-lane-agent` App (shared/claude-plugins#918, STAGE 1).

Why this exists: lanes author as `ahmednfwela`, GitHub bars the author from
approving their own PR, and every lane-side merge gate so far has been
advisory (#882 — a reviewer briefed READ-ONLY merged anyway; #914 — landings
promoted on verdict-less completions). The owner ruling 2026-09-15 moves the
verdict onto the forge: a SECOND principal, the GitHub App `bdaya-lane-agent`
(app 4919916, installation 161114934, permission pull_requests:write —
capability PROVEN live on throwaway PR#75: POST returned 200, state APPROVED,
user bdaya-lane-agent[bot], type Bot — a recorded review, not the
GITHUB_TOKEN comment fallback, which is hard-barred from APPROVE).

STAGE-1 SCOPE (binding): this module submits a REVIEW only. It must NOT set
auto-merge, NOT touch branch protection, NOT merge — #882 stays: approving is
not merging, and the landing path stays where it is until the SEPARATE
protection-flip task cites this stage's live evidence.

Rules encoded here:
  * PASS at head                -> event APPROVE
  * NEEDS-CHANGES               -> event REQUEST_CHANGES
  * INCOMPLETE-ROSTER / NEEDS-HUMAN / no verdict -> NO review at all
    (the #914 rule at the forge boundary: 'completed' is never a verdict,
    so it must never mint a review either)
  * the review is sha-pinned (commit_id = head sha the verdict pinned to)
  * success is asserted only by READ-BACK: GET .../reviews must show a
    review by the App bot with the expected state at the expected sha —
    a 2xx with no recorded bot review is reported as a failure, never as
    a successful review (#913's silent-failure class, reproduced nowhere
    new).
  * failure is loud: an un-mintable token or a 4xx submission returns
    ok=False with a reason; the CLI exits non-zero.
  * secrets are never printed or returned: the installation token appears
    in results only as its byte length; the private key never enters this
    module at all (minting is delegated to lane_gh_token.py, #867, whose
    cache never yields a token with <5 min of life).

The App's `pull_requests: write` makes the review ATTRIBUTABLE to the bot —
it is not a neutral token: it posts a signed verdict under the estate's
reviewer identity. Usage is therefore restricted to reviewer-role lanes
driven by the #893 RV-1 brief, which is also why this module ships no merge
verb at all.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

from .intake_grouping import _parse_github_pr_url

API = "https://api.github.com"
CONFIG_PATH = os.path.expanduser("~/.config/bdaya/lane-gh-token.json")
RESOLVER_FALLBACK = os.path.expanduser("~/.config/bdaya/lane-gh-token/lane_gh_token.py")
USER_AGENT = "bdaya-lane-agent-review"

# The only verdicts that may reach the forge (the rest of the vocabulary —
# INCOMPLETE-ROSTER, NEEDS-HUMAN, None — means "no review", matching
# landing_gate.parse_reviewer_verdict's PASS/NEEDS-CHANGES words).
_EVENT_BY_VERDICT = {
    "PASS": "APPROVE",
    "NEEDS-CHANGES": "REQUEST_CHANGES",
}


class HttpError(Exception):
    """A non-2xx forge response, carrying the status for the loud path."""

    def __init__(self, status, body=""):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body if isinstance(body, str) else json.dumps(body)[:200]


def review_event_for_verdict(verdict):
    """APPROVE / REQUEST_CHANGES for a reviewable verdict, else None.

    None is an instruction, not a miss: the caller submits NO review and
    says why (INCOMPLETE-ROSTER / NEEDS-HUMAN / verdict-less)."""
    if not verdict:
        return None
    return _EVENT_BY_VERDICT.get(str(verdict).strip().upper().replace("_", "-"))


def _default_http():
    return _UrllibTransport()


class _UrllibTransport:
    """Minimal JSON transport over urllib (stdlib-only, CI-portable)."""

    def request(self, method, url, *, token, payload=None, timeout=30):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            snippet = ""
            try:
                snippet = e.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            raise HttpError(e.code, snippet) from None
        except OSError as e:
            raise HttpError(0, f"{type(e).__name__}: {e}") from None


def _default_token():
    """Mint/fetch the installation token through the estate resolver (#867) —
    subprocess, never env, and the value NEVER leaves this function except to
    the transport. Failure surfaces as an exception the caller turns into a
    loud ok=False."""
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)
        except (OSError, ValueError):
            cfg = {}
    resolver = cfg.get("resolver_file") or RESOLVER_FALLBACK
    if not os.path.exists(resolver):
        raise RuntimeError(f"lane-gh-token resolver not provisioned at {resolver}")
    proc = subprocess.run(
        [sys.executable, resolver, "get", "--json"],
        capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        err = (proc.stderr or "").strip().splitlines()
        # last line only, length-capped: the resolver never prints secrets,
        # but defense in depth — a token-shaped line must not ride along.
        tail = (err[-1][:200] if err else "mint failed")
        if proc.stdout.strip():
            tail = "" if _looks_like_token(proc.stdout.strip()) else tail or "mint failed"
        raise RuntimeError(f"token mint failed: {tail}")
    try:
        data = json.loads(proc.stdout)
    except ValueError:
        raise RuntimeError("token mint returned unparseable output")
    tok = data.get("token")
    if not isinstance(tok, str) or not tok:
        raise RuntimeError("token mint returned no token")
    return tok, data.get("source", "minted")


def _looks_like_token(s):
    return len(s) > 30 and not s.startswith("{")


def _readback(http, token, repo, number, sha, event, mine=None):
    """GET the reviews and confirm a RECORDED review by the App bot
    (type Bot) with the expected state at the pinned sha. Returns
    (state_or_None, detail) — detail names what was actually seen, no
    secrets."""
    try:
        page = http.request("GET", f"{API}/repos/{repo}/pulls/{number}/reviews?per_page=100",
                            token=token)
    except HttpError as e:
        return None, f"readback failed: HTTP {e.status}"
    reviews = page.get("reviews") if isinstance(page, dict) else page
    reviews = reviews if isinstance(reviews, list) else []
    want_state = {"APPROVE": "APPROVED", "REQUEST_CHANGES": "CHANGES_REQUESTED"}[event]
    for rv in reversed(reviews):
        user = rv.get("user") or {}
        login = (user.get("login") or "").lower()
        if mine and rv.get("id") is not None and rv.get("id") not in mine:
            continue
        if login.startswith("bdaya-lane-agent") and user.get("type") == "Bot" \
                and (rv.get("state") or "").upper() == want_state \
                and (rv.get("commit_id") or "").lower().startswith(str(sha)[:7].lower()):
            return want_state, "verified"
    return None, "no recorded App-bot review at the pinned sha in GET /reviews"


def submit_pr_review(mr_url, sha, verdict, *, body="", token=None,
                     http=None, token_minter=None):
    """Record the reviewer verdict on the GitHub PR as bdaya-lane-agent.

    Returns a dict SAFE to print/return in a lane result:
      {ok, submitted, event, review_state, pr_url, reason, token_len}
    ok=True + submitted=True ONLY when the review was posted AND read back
    from GET /pulls/<n>/reviews as a recorded bot review at the pinned sha.
    Never claims success on a 2xx alone, never on a comment fallback, and
    never for INCOMPLETE-ROSTER / NEEDS-HUMAN / verdict-less (submitted=False,
    reason says so). No secret value appears in the result — the token is
    present as `token_len` only.
    """
    parsed = _parse_github_pr_url(mr_url or "")
    if not parsed:
        return {"ok": False, "submitted": False, "event": None,
                "review_state": None, "pr_url": mr_url, "token_len": 0,
                "reason": "not a GitHub PR url"}
    repo, number = parsed
    http = http or _default_http()
    event = review_event_for_verdict(verdict)
    if event is None:
        return {"ok": True, "submitted": False, "event": None,
                "review_state": None, "pr_url": mr_url, "token_len": 0,
                "reason": f"verdict {verdict!r} is not review-grade: "
                          "no review submitted (only PASS and NEEDS-CHANGES "
                          "reach the forge)"}
    if not sha:
        return {"ok": False, "submitted": False, "event": event,
                "review_state": None, "pr_url": mr_url, "token_len": 0,
                "reason": "no head sha pinned: a review without its sha is "
                          "the stale-verdict hole #914 closed on the board"}
    tok_source = "caller"
    if token is None:
        minter = token_minter or _default_token
        try:
            token, tok_source = minter()
        except Exception as e:
            return {"ok": False, "submitted": False, "event": event,
                    "review_state": None, "pr_url": mr_url, "token_len": 0,
                    "reason": f"installation token unavailable, review NOT "
                              f"submitted: {e}"}
    try:
        created = http.request(
            "POST", f"{API}/repos/{repo}/pulls/{number}/reviews",
            token=token, payload={"commit_id": sha, "event": event, "body": body or ""})
    except HttpError as e:
        return {"ok": False, "submitted": False, "event": event,
                "review_state": None, "pr_url": mr_url,
                "token_len": len(token or ""),
                "reason": f"review submission failed: HTTP {e.status} "
                          f"{getattr(e, 'body', '')[:200]}"}
    mine = {created.get("id")}
    state, detail = _readback(http, token, repo, number, sha, event, mine=mine)
    ok = state is not None
    return {"ok": ok, "submitted": ok, "event": event,
            "review_state": state, "pr_url": f"{API}/repos/{repo}/pulls/{number}"
            if not str(mr_url).startswith("http") else mr_url,
            "token_len": len(token or ""), "token_source": tok_source,
            "review_id": created.get("id"),
            "reason": detail}


def main(argv=None):
    """CLI for reviewer lanes (also imported by tests). Exit 0 ONLY when the
    forge shows the recorded review (or the verdict legitimately means 'no
    review'); any mint/submit/readback failure prints a loud reason and
    exits 1. Prints no secret value."""
    import argparse
    p = argparse.ArgumentParser(prog="gh_review", description=__doc__.splitlines()[0])
    p.add_argument("--mr-url", required=True, help="the GitHub PR url under review")
    p.add_argument("--sha", required=True, help="head sha the verdict pinned to")
    p.add_argument("--verdict", required=True,
                   help="PASS | NEEDS-CHANGES | INCOMPLETE-ROSTER | NEEDS-HUMAN "
                        "(last two submit no review)")
    p.add_argument("--body", default="", help="short review body (verdict summary)")
    args = p.parse_args(argv)
    res = submit_pr_review(args.mr_url, args.sha, args.verdict, body=args.body)
    if res["ok"] and res["submitted"]:
        print(f"REVIEW {res['event']} recorded by bdaya-lane-agent[bot] at {args.sha} "
              f"— verified via GET /pulls/<n>/reviews (state {res['review_state']}, "
              f"review_id {res.get('review_id')}, token_len {res['token_len']} from "
              f"{res.get('token_source')})")
        return 0
    if res["ok"] and not res["submitted"]:
        print(f"NO-REVIEW: {res['reason']}")
        return 0
    print(f"REVIEW FAILED: {res['reason']}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
