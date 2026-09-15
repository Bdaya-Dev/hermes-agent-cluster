#!/usr/bin/env python3
"""release-pr-917.py — open/update the release PR that moves the cluster pin (#917).

A merge to the fork main that touches hermes_cluster/** builds an image; by
cluster-image.yml's own header, "a release = bumping that SHA tag in the
ArgoCD source tree". This step performs the *noticing* automatically: it
opens (or updates) a PR against Bdaya-Dev/bdaya-website-infra bumping
`cluster-config/hermes-main/kustomization.yaml` `images.newTag` to the pushed
SHA — in the train-comment format trains 4/5/6 established, carrying the
commit range and the digest RESOLVED LIVE from Artifact Registry (never
copied from a previous train).

The release stays REVIEWED: this OPENs a PR and nothing else. It never merges,
never pushes to any repo's main, never touches kubectl. What was missing in
trains 1-6 was human attention, not human judgment — judgment stays exactly
where it was.

Credentials (fail-closed by design): opening a PR in the infra repo from this
fork needs a cross-repo identity. This step reads the fork's repository
Actions secret RELEASE_PR_TOKEN — the narrowest identity that works (see
die_missing_token for the exact grant). When it is absent the step FAILS
LOUDLY: a release owed must never pass silently — that blindness is the
defect this feature removes. Stdout never prints a token value.
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
USER_AGENT = "hermes-cluster-image/release-pr-917"
PIN_PATH = "cluster-config/hermes-main/kustomization.yaml"
TARGET_REPO = "Bdaya-Dev/bdaya-website-infra"
SOURCE_REPO = "Bdaya-Dev/hermes-agent-cluster"

FULL_SHA = re.compile(r"[0-9a-f]{40}")
# newTag line + the indented `#` train-comment lines that follow it (trains
# 4/5/6 format). Replacing the line WITHOUT consuming the block would orphan
# the previous train's comments under the new tag.
NEW_TAG_BLOCK = re.compile(
    r"(?m)^[ \t]*newTag:[ \t]*\"?[0-9a-f]{40}\"?[^\n]*(?:\n[ \t]+#[^\n]*)*")
NEW_TAG_LINE = re.compile(r"(?m)^([ \t]*)newTag:[ \t]*\"?([0-9a-f]{40})\"?[^\n]*$")


class Refuse(Exception):
    """A stop-the-train failure: loud, never a silent skip."""


def indent_comment(lines: list[str], pad: str = "      # ") -> str:
    """Every line gets the comment prefix — including multi-line bullet
    blocks (a joined-with-newlines value slipping through un-prefixed would
    emit raw YAML text inside the manifest: invalid or, worse, a silent
    comment continuation)."""
    out = []
    for ln in lines:
        for piece in str(ln).split("\n"):
            out.append(pad + piece)
    return "\n".join(out)


def format_bullets(commits: list[dict]) -> str:
    lines = []
    for c in commits:
        sha = (c.get("sha") or "")[:9]
        msg = ((c.get("commit") or {}).get("message") or "").splitlines()
        subject = msg[0] if msg else ""
        lines.append(f"  * `{sha}` {subject}")
    return "\n".join(lines) if lines else "  * (see compare link)"


def render_release_text(cur_text: str, new_sha: str, digest: str,
                        run_url: str, compare: dict) -> str:
    """The one-field bump + fresh train-comment block, in the train-5 format.

    Returns the full new file text. Raises Refuse on any shape it cannot
    prove — this never guesses at the release manifest.
    """
    if not FULL_SHA.match(new_sha or ""):
        raise Refuse(f"new sha not 40-hex: {new_sha!r}")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest or ""):
        raise Refuse("digest not sha256:<64hex> — refusing to open a release "
                     "PR without a digest RESOLVED from Artifact Registry "
                     "(acceptance #917: resolved, not copied).")
    block = NEW_TAG_BLOCK.search(cur_text)
    line = NEW_TAG_LINE.search(cur_text)
    if not block or not line:
        raise Refuse("could not locate the images.newTag block in the live "
                     "kustomization — refusing to guess the edit shape")
    pin = line.group(2)
    if pin == new_sha:
        raise Refuse("ALREADY_RELEASED")
    indent = line.group(1)
    ahead = compare.get("ahead_by", "?")
    commits = compare.get("commits") or []
    bullets = format_bullets(commits)
    run_id = run_url.rsplit("/", 1)[-1] if run_url else "?"
    comment_block = indent_comment([
        f"Ships {ahead} ALREADY-MERGED cluster commit(s) "
        f"(fork diff {pin[:9]}..{new_sha[:9]}, paths hermes_cluster/**).",
        "",
        "OPENED AUTOMATICALLY by the fork's cluster-image.yml on the push "
        "that built this image (shared/claude-plugins#917 option 1: auto-OPEN, "
        "never auto-MERGE). A human or reviewer lane still reviews and merges "
        "this PR; nothing about the GitOps or review gate changed.",
        "",
        "Merged-but-not-running commits this train carries:",
        bullets,
        "",
        f"Image published by fork run {run_url}",
        f"digest {digest}",
        "(tag -> digest resolved live against Artifact Registry before "
        "pinning, same discipline as trains 4, 5 and 6.)",
    ])
    new_tag_line = (f'{indent}newTag: "{new_sha}"  # RELEASE TRAIN '
                    f'(auto, #917) — rolls the hosted main {pin[:9]} -> '
                    f'{new_sha[:9]} (hermes-agent-cluster fork main).')
    new_text = cur_text.replace(block.group(0),
                                new_tag_line + "\n" + comment_block, 1)
    # verify the rendered file from scratch: exactly one newTag line, and it
    # carries BOTH the new sha and the digest (acceptance #917).
    tags = NEW_TAG_LINE.findall(new_text)
    if len(tags) != 1:
        raise Refuse(f"rendered kustomization has {len(tags)} newTag lines "
                     "(expected exactly 1) — refusing to commit")
    if tags[0][1] != new_sha:
        raise Refuse("rendered newTag does not carry the new sha — refusing")
    if digest not in new_text:
        raise Refuse("rendered block lost the resolved digest — refusing")
    return new_text


def render_pr_body(new_sha: str, digest: str, compare: dict,
                   run_url: str) -> str:
    pin = (compare.get("base_commit") or {}).get("sha", "")
    ahead = compare.get("ahead_by", "?")
    bullets = format_bullets(compare.get("commits") or [])
    return (
        "## Release train — auto-opened by the image build (shared/claude-plugins#917)\n\n"
        f"The fork main push `{new_sha[:9]}` built image `{digest}` — resolved "
        "live against Artifact Registry in the build run, NOT copied from a "
        "previous train. The deployed pin is the live `newTag` read from "
        f"`{PIN_PATH}` on this repo's main at open time. "
        f"**{ahead} commit(s) have merged to fork main and are not "
        "running.** This PR moves the pin — nothing else.\n\n"
        f"Range: [{pin[:9]}...{new_sha[:9]}]"
        f"(https://github.com/{SOURCE_REPO}/compare/{pin}...{new_sha})\n\n"
        f"```\n{bullets}\n```\n\n"
        "### Why this exists\n\n"
        "#917 measured ten commits sitting merged-and-not-running for ~8h "
        "(`GET /api/v1/lanes` 404 the whole time — the cluster served a stale "
        "pin while ArgoCD cheerfully reported Synced), and the gap reopening "
        "13 minutes after a hand-cut release. The missing step was attention, "
        "not judgment. **A human or reviewer lane still merges this PR — "
        "auto-OPEN only; auto-MERGE is forbidden.** The GitOps contract is "
        "unchanged: the pin moves only through a reviewed merge to this "
        "repo's main that ArgoCD reconciles.\n\n"
        "### Credential used (declared per #917)\n\n"
        f"* **Identity**: fine-grained PAT, stored as the FORK's repository "
        "Actions secret `RELEASE_PR_TOKEN` (repo-scoped, not org).\n"
        f"* **Grants**: the single repository `{TARGET_REPO}` — "
        "`Contents: Write` (create the release branch + its one-line bump "
        "commit) + `Pull requests: Write` (open the PR). Nothing else, "
        "nowhere else. Contents:Write on a single private repo is exactly "
        "the write the train has always required; review/merge authority is "
        "unchanged — this token cannot and does not merge.\n\n"
        "_Refs shared/claude-plugins#917_\n"
    )


def gh(token: str, method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{API}/{path.lstrip('/')}", data=data, method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "Content-Type": "application/json",
                 "User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
        if not raw:
            return r.status, None
        return r.status, json.loads(raw.decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads((e.read() or b"{}").decode() or "{}")
        except Exception:
            return e.code, {}
    except Exception as e:
        raise Refuse(f"{method} {path}: {e}")


TOKEN_ASK = (
    "RELEASE_PR_TOKEN is not configured on this repo. The release PR is OWED "
    "but cannot be opened. Required identity (narrowest that works, #917): a "
    "fine-grained PAT scoped to the single repository "
    f"{TARGET_REPO} with Contents:Write + Pull requests:Write ONLY "
    "(branch + bump commit + open PR — no merge authority), stored as "
    "the fork's REPOSITORY Actions secret RELEASE_PR_TOKEN. NEVER an org-wide "
    "secret, NEVER a classic PAT, NEVER a GCP service-account key (WIF stays "
    "the only GCP path — this step changes nothing GCP-side). Add it and "
    "re-run this workflow (workflow_dispatch is enough — the image already "
    "pushed). Failing closed by design: a silent no-op here would re-create "
    "the blindness this feature removes; the in-pod drift alarm "
    "(release_drift, #917 backstop) keeps screaming until a train lands.")


def main() -> int:
    token = os.environ.get("RELEASE_PR_TOKEN", "").strip()
    if not token:
        print(f"::error::{TOKEN_ASK}", file=sys.stderr)
        return 1
    sha = os.environ.get("GITHUB_SHA", "").strip().lower()
    digest = os.environ.get("RESOLVED_DIGEST", "").strip().lower()
    run_url = (os.environ.get("GITHUB_SERVER_URL", "https://github.com")
               + "/" + os.environ.get("GITHUB_REPOSITORY", SOURCE_REPO)
               + "/actions/runs/" + os.environ.get("GITHUB_RUN_ID", ""))
    print(f"release-pr: token present (len={len(token)} bytes; value never printed)")

    try:
        # live pin from the infra repo (NOT assumed from the event payload)
        status, cur = gh(token, "GET",
                         f"repos/{TARGET_REPO}/contents/{PIN_PATH}?ref=main")
        if status != 200 or not cur or "content" not in cur:
            raise Refuse(f"cannot read {TARGET_REPO}/{PIN_PATH}@main "
                         f"(HTTP {status}) — token needs Contents:Read there")
        cur_text = base64.b64decode(cur["content"]).decode()

        status, cmp_data = gh(token, "GET",
                              f"repos/{SOURCE_REPO}/commits/main")
        if status != 200 or not cmp_data:
            raise Refuse(f"cannot read fork main (HTTP {status})")
        main_sha = cmp_data["sha"]
        if main_sha != sha:
            print(f"::notice::fork main moved to {main_sha[:9]} after this "
                  f"run's build of {sha[:9]}; opening the train for the "
                  "built image (a newer train will supersede if it lands "
                  "unmerged)")

        # range = live pin .. built sha (both full 40-hex; pin read from the
        # manifest above, sha from the workflow trigger)
        m = NEW_TAG_LINE.search(cur_text)
        if not m:
            raise Refuse("no newTag line found in the live kustomization")
        pin = m.group(2)
        if pin == sha:
            print("::notice::live pin already equals the built sha — "
                  "nothing owed")
            return 0
        status, cmp_data = gh(token, "GET",
                              f"repos/{SOURCE_REPO}/compare/{pin}...{sha}")
        if status != 200 or not cmp_data or "ahead_by" not in cmp_data:
            raise Refuse(f"compare {pin[:9]}...{sha[:9]} failed (HTTP {status})")

        new_text = render_release_text(cur_text, sha, digest, run_url,
                                       cmp_data)
    except Refuse as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1

    branch = f"release/hermes-train-auto-{sha[:7]}"
    title = (f"release(hermes-main): roll {pin[:9]} -> {sha[:9]} — "
             f"{cmp_data.get('ahead_by')} merged commit(s) not running "
             f"(auto-opened, #917)")

    try:
        # Branch per built-sha (the name carries sha[:7], so an "update" can
        # only ever be the SAME image re-run — same content, review state
        # preserved). A branch that already exists WITH an open PR must be
        # updated in place, never deleted: deleting a PR head branch
        # auto-closes the PR (worse: kills a reviewer's in-flight work).
        # A branch WITHOUT an open PR is an orphan of a previous run: safe to
        # re-create from current infra main, but ONLY if it was last written
        # by this workflow's identity — never clobber a human branch
        # (estate rule: never rebase/force-push a shared branch).
        status, ref = gh(token, "GET",
                         f"repos/{TARGET_REPO}/git/ref/heads/{branch}")
        branch_exists = status == 200 and bool(ref)
        status, prs = gh(token, "GET",
                         f"repos/{TARGET_REPO}/pulls?state=open"
                         f"&head={TARGET_REPO.split('/')[0]}:{branch}")
        if status != 200:
            raise Refuse(f"cannot list open PRs (HTTP {status})")
        existing = next((p for p in (prs or [])
                         if p["head"]["ref"] == branch), None)

        if branch_exists and existing is None and ref:
            head_sha = (ref.get("object") or {}).get("sha", "")
            if not head_sha:
                raise Refuse(f"branch {branch} ref has no object sha — refusing")
            status2, ch = gh(token, "GET",
                             f"repos/{TARGET_REPO}/commits/{head_sha}")
            actor = os.environ.get("GITHUB_ACTOR", "")
            author = ((ch or {}).get("author") or {}).get("login", "")
            committer = ((ch or {}).get("committer") or {}).get("login", "")
            if actor and author in ("", actor) and committer in ("", actor):
                gh(token, "DELETE",
                   f"repos/{TARGET_REPO}/git/refs/heads/{branch}")
                branch_exists = False
            else:
                raise Refuse(
                    f"branch {branch} exists without an open PR and was last "
                    f"written by '{author or committer or '?'}', not this "
                    f"workflow's identity '{actor}' — refusing to overwrite "
                    "a human branch. Close or rename it and re-run.")

        if not branch_exists:
            status, bh = gh(token, "GET", f"repos/{TARGET_REPO}/commits/main")
            if status != 200 or not bh:
                raise Refuse(f"cannot read infra main (HTTP {status})")
            status, _ = gh(token, "POST", f"repos/{TARGET_REPO}/git/refs",
                           {"ref": f"refs/heads/{branch}", "sha": bh["sha"]})
            if status not in (201, 422):
                raise Refuse(f"cannot create branch {branch} (HTTP {status})")

        # the bump commit: one file, one field (RELEASE MECHANICS header,
        # trains 4-6 shape)
        target_ref = branch
        status2, cur_branch = gh(
            token, "GET",
            f"repos/{TARGET_REPO}/contents/{PIN_PATH}?ref={target_ref}")
        blob_sha = (cur_branch or {}).get("sha") or cur.get("sha")
        upd = {"message": (f"release(hermes-main): roll the pin {pin[:9]} -> "
                           f"{sha[:9]} (auto, #917)\n\n"
                           f"digest {digest} resolved live against Artifact "
                           f"Registry; {cmp_data.get('ahead_by')} merged "
                           "commit(s) never released. Refs "
                           "shared/claude-plugins#917."),
               "content": base64.b64encode(new_text.encode()).decode(),
               "branch": target_ref}
        if blob_sha:
            upd["sha"] = blob_sha
        status, res = gh(token, "PUT",
                         f"repos/{TARGET_REPO}/contents/{PIN_PATH}", upd)
        if status not in (200, 201):
            raise Refuse(f"commit to {branch} failed (HTTP {status}): "
                         f"{json.dumps(res)[:300]}")

        # open OR update the PR — auto-OPEN only, never merge
        body_md = render_pr_body(sha, digest, cmp_data, run_url)
        if existing:
            status, _ = gh(token, "PATCH",
                           f"repos/{TARGET_REPO}/pulls/{existing['number']}",
                           {"title": title, "body": body_md})
            if status != 200:
                raise Refuse(f"updating PR #{existing['number']} failed "
                             f"(HTTP {status})")
            print(f"updated release PR #{existing['number']}: "
                  f"https://github.com/{TARGET_REPO}/pull/"
                  f"{existing['number']}")
        else:
            status, pr = gh(token, "POST", f"repos/{TARGET_REPO}/pulls",
                            {"title": title, "head": branch, "base": "main",
                             "body": body_md})
            if status != 201 or not pr:
                raise Refuse(f"opening the release PR failed (HTTP "
                             f"{status}): {json.dumps(res)[:300]}")
            print(f"opened release PR #{pr['number']}: {pr['html_url']}")
    except Refuse as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
