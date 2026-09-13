#!/usr/bin/env python3
"""gh-write-doctor — fleet-doctor-style GitHub WRITE reachability check (#867, #788).

Runs per member and reports ONE verdict line (no secret values):
  GH_WRITE <node> ok      mint + authenticated write-POST reachable as the App
  GH_WRITE <node> MISSING key/config not provisioned on this node
  GH_WRITE <node> STALE   key present but mint rejected (revocation / clock / id drift)
  GH_WRITE <node> NOPR    token minted but the probe PR is not write-visible (no write perm)

Exit 0 ok / 1 MISSING / 2 STALE or NOPR. The worker's capability probe maps the
verdict onto the declared capability set (lane can `requires: [github-write]`),
so a lane for a GitHub repo can no longer be dispatched to a node that cannot
post — the #867 silent-mid-lane failure becomes a loud dispatch-time miss.

Modes:
  check        full local check (default)
  --json       machine-readable
  --url U      verify a just-posted comment URL carries the App author (proof mode)

Configuration comes from ~/.config/bdaya/lane-gh-token.json — never env vars
(owner ruling 2026-09-13). The probe repo/PR are config fields, not hardcoded.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request

CONFIG_PATH = os.path.expanduser("~/.config/bdaya/lane-gh-token.json")


def _resolver_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _node_id(cfg: dict, override: str = "") -> str:
    if override:
        return override
    if cfg.get("node_id"):
        return cfg["node_id"]
    return os.uname().nodename if hasattr(os, "uname") else os.environ.get("COMPUTERNAME", "unknown")


def local_check(cfg: dict, node_label: str = "") -> dict:
    out = {"node": _node_id(cfg, node_label), "verdict": "ok", "detail": ""}
    key_file = os.path.expanduser(cfg.get("private_key_file", "~/.config/bdaya/lane-agent-app.pem"))
    if not os.path.exists(key_file) or not os.path.exists(CONFIG_PATH):
        out.update(verdict="MISSING", detail="private key or config not provisioned on this node")
        return out
    probe_repo = cfg.get("write_probe_repo", "Bdaya-Dev/bdaya-website-infra")
    probe_pr = str(cfg.get("write_probe_pr", "288"))
    resolver = cfg.get("resolver_file") or os.path.join(_resolver_dir(), "lane_gh_token.py")
    try:
        token = subprocess.run(
            [sys.executable, resolver, "get"], capture_output=True, text=True, timeout=90
        )
    except Exception as e:  # resolver itself broken on this node
        out.update(verdict="MISSING", detail=f"resolver failed to run: {e}")
        return out
    if token.returncode != 0:
        err = (token.stderr or "").strip().splitlines()
        err = err[-1] if err else "mint failed"
        # "key not provisioned" -> MISSING; anything else mint-side -> STALE
        out.update(verdict="MISSING" if "not provisioned" in err or "empty" in err else "STALE",
                   detail=err[-200:])
        return out
    tok = token.stdout.strip()
    if cfg.get("_no_post"):
        out["mint"] = "ok"
        out["detail"] = "key present, token minted; POST skipped (--no-post)"
        return out
    url = f"https://api.github.com/repos/{probe_repo}/issues/{probe_pr}/comments"
    body = (f"[bdaya-lane-agent] #867 doctor WRITE probe from {out['node']} — "
            "no action needed.")
    req = urllib.request.Request(url, data=json.dumps({"body": body}).encode(), method="POST", headers={
        "Authorization": f"Bearer {tok}",
        "Accept": "application/vnd.github+json",
        "User-Agent": f"gh-write-doctor-{out['node']}",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            c = json.load(r)
        if c.get("user", {}).get("login", "") != cfg.get("expected_author", "bdaya-lane-agent[bot]"):
            out.update(verdict="STALE", detail=f"posted as {c.get('user', {}).get('login')} — wrong identity")
            return out
        out["post_url"] = c["html_url"]
        out["author"] = c["user"]["login"]
        if not cfg.get("_proof"):
            # Clean the probe comment back up — the write-path proof is the POST
            # verdict, not permanent noise on the PR. (--proof keeps it.)
            try:
                dele = urllib.request.Request(c["url"], method="DELETE", headers={
                    "Authorization": f"Bearer {tok}", "User-Agent": "gh-write-doctor"})
                urllib.request.urlopen(dele, timeout=30)
                out["cleanup"] = "deleted"
            except Exception as e:
                out["cleanup"] = f"FAILED ({e}) — delete manually"
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            out.update(verdict="NOPR", detail=f"write POST rejected HTTP {e.code} on {probe_repo}#{probe_pr}")
        else:
            out.update(verdict="STALE", detail=f"write POST HTTP {e.code}")
    except Exception as e:
        out.update(verdict="STALE", detail=f"write POST failed: {e}")
    return out


def verify_posted(url: str, cfg: dict) -> dict:
    """Proof mode: fetch a posted comment, assert it is the App's, without minting."""
    expected = cfg.get("expected_author", "bdaya-lane-agent[bot]")
    base, _, frag = url.partition("#")
    num = frag.replace("issuecomment-", "")
    m = __import__("re").search(r"/pull/(\d+)", base)
    if not num or not m:
        return {"verdict": "MISSING", "detail": f"cannot parse comment id from {url}"}
    api = f"https://api.github.com/repos/{base.split('github.com/')[1].split('/pull/')[0]}/issues/{m.group(1)}/comments/{num}"
    req = urllib.request.Request(api, headers={"Accept": "application/vnd.github+json", "User-Agent": "gh-write-doctor"})
    with urllib.request.urlopen(req, timeout=30) as r:
        c = json.load(r)
    login = c.get("user", {}).get("login", "")
    return {"verdict": "ok" if login == expected else "STALE", "author": login,
            "node": (c.get("body") or "").split("doctor WRITE probe from ")[-1].split(" —")[0] or "unknown",
            "detail": f"posted by {login}"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--url", help="verify a posted comment URL is authored by the App")
    ap.add_argument("--no-post", action="store_true", help="check key+mint only, do not POST")
    ap.add_argument("--proof", action="store_true",
                    help="post a PERSISTENT identity proof comment (not deleted) and verify the author")
    ap.add_argument("--node", default="", help="fleet node label recorded in the proof (e.g. windows-desktop)")
    args = ap.parse_args()
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        cfg = json.load(open(CONFIG_PATH, encoding="utf-8"))
    if args.url:
        res = verify_posted(args.url, cfg)
    elif args.no_post:
        res = local_check(dict(cfg, _no_post=True))
    elif args.proof:
        res = local_check(dict(cfg, _proof=True))
    else:
        res = local_check(cfg)
    if args.json:
        print(json.dumps(res))
    else:
        line = f"GH_WRITE {res['node']} {res['verdict']}"
        if res.get("post_url"):
            line += f" {res['post_url']}"
        if res.get("detail"):
            line += f" | {res['detail']}"
        print(line)
    return {"ok": 0, "MISSING": 1}.get(res["verdict"], 2)


if __name__ == "__main__":
    sys.exit(main())
