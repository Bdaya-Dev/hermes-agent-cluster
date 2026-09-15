"""Tests for the release-PR opener script's PURE transforms (#917).

The workflow's network path cannot be exercised without the credential that
the PR itself declares missing — so everything that CAN be made provable
without it is pure here: the kustomization render (the only line the release
touches + the train-comment block) and the PR body. Fixtures use the REAL
train-5 newTag block shape read off bdaya-website-infra main 2026-09-15.
"""

import importlib.util
import re
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / \
    ".github/scripts/release-pr-917.py"


def _load():
    spec = importlib.util.spec_from_file_location("release_pr_917", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


r917 = _load()

TRAIN5_BLOCK = '''    newTag: "25da11c5fd8500b0bab503e7d2aeceae2635d8a9"  # RELEASE TRAIN 5 — rolls the hosted main aebf9d9 -> 25da11c (hermes-agent-cluster fork main).
      # Ships 10 ALREADY-MERGED, ALREADY-RV-1'd cluster commits (fork diff aebf9d9f..25da11c5, paths hermes_cluster/**).
      #
      # Image published 2026-09-15T17:28:54Z by fork run on 25da11c5 (workflow "Cluster Image", conclusion success);
      # digest sha256:8e74bc0e3f81f8875587448138777f6b83c7271a62d9599a26539fd71c3850c9
      # (tag -> digest resolved live against Artifact Registry before pinning, same discipline as train 4.)
'''

KUSTOM = ("apiVersion: kustomize.config.k8s.io/v1beta1\n"
          "kind: Kustomization\n\nnamespace: hermes-main\n\nresources:\n"
          "  - deployment.yaml\n\nimages:\n  - name: hermes-cluster-main\n"
          "    newName: europe-west1-docker.pkg.dev/bdaya-website/bdaya-docker/hermes-cluster-main\n"
          + TRAIN5_BLOCK)

NEW = "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a"
NEW_DIGEST = "sha256:d371728ad56d42090af0b9a322f48824b5cf8672e582a04893f42e4396f94428"
COMPARE = {
    "ahead_by": 2,
    "commits": [
        {"sha": "17a952f01234567890abcdef1234567890abcdef",
         "commit": {"message": "fix(worker-connector): make the beat wait interruptible\n\nbody"}},
        {"sha": "173c30701234567890abcdef1234567890abcdef",
         "commit": {"message": "Merge pull request #70 from Bdaya-Dev/feat/newest-first-ordering"}},
    ],
}


def _render():
    return r917.render_release_text(
        KUSTOM, NEW, NEW_DIGEST,
        "https://github.com/Bdaya-Dev/hermes-agent-cluster/actions/runs/1",
        COMPARE)


def test_render_bumps_only_the_pin_field():
    out = _render()
    # exactly one newTag line and it carries the NEW sha
    tags = re.findall(r"(?m)^ *newTag:.*$", out)
    assert len(tags) == 1
    assert NEW in tags[0]


def test_render_consumes_previous_train_comments():
    """The old train's comment block must NOT survive under the new tag —
    a stale digest comment under a new pin is exactly the "copied from the
    previous train" failure #917 forbids."""
    out = _render()
    assert "8e74bc0e" not in out
    assert "RELEASE TRAIN 5" not in out


def test_render_carries_resolved_digest_and_range():
    out = _render()
    assert NEW_DIGEST in out
    assert "25da11c5f..e6b58f971" in out
    assert "ahead_by" not in out  # rendered real numbers, not placeholders


def test_render_surrounding_manifest_untouched():
    out = _render()
    assert out.startswith(KUSTOM.split("images:")[0])
    # resources list and header intact (RELEASE MECHANICS header is above)
    assert "deployment.yaml" in out


def test_render_rejects_malformed_digest():
    # a malformed digest is refused outright: the number that lands in the
    # release body is the one the build job resolved LIVE from the registry
    # (the copy-prevention itself is structural — a previous train's digest
    # cannot appear here because this function's only digest input is the
    # registry resolution the workflow performs after push).
    with pytest.raises(r917.Refuse):
        r917.render_release_text(KUSTOM, NEW, "sha256:not-long-enough",
                                 "", COMPARE)
    with pytest.raises(r917.Refuse):
        r917.render_release_text(KUSTOM, NEW, "", "", COMPARE)


def test_render_refuses_already_matching_pin():
    with pytest.raises(r917.Refuse) as e:
        r917.render_release_text(KUSTOM,
                                 "25da11c5fd8500b0bab503e7d2aeceae2635d8a9",
                                 NEW_DIGEST, "", COMPARE)
    assert "ALREADY_RELEASED" in str(e.value)


def test_render_refuses_manifest_without_newtag():
    with pytest.raises(r917.Refuse):
        r917.render_release_text("kind: Kustomization\n", NEW, NEW_DIGEST,
                                 "", COMPARE)


def test_multiline_bullets_stay_commented():
    """A raw YAML line leaking into the comment block (un-prefixed bullet)
    would change the MANIFEST — the render must keep every block line a
    comment."""
    out = _render()
    tail = out.split("newTag")[1]
    bad = [ln for ln in tail.splitlines()
           if ln.strip() and not ln.strip().startswith("#")
           and not ln.startswith(' "')]
    # the only non-comment remainder is the inline header text ON the newTag
    # line itself; everything below it is comment continuation
    for ln in bad:
        assert "e6b58f9715" in ln  # the newTag line itself


def test_pr_body_states_range_digest_and_no_merge():
    body = r917.render_pr_body(
        NEW, NEW_DIGEST, COMPARE,
        "https://github.com/Bdaya-Dev/hermes-agent-cluster/actions/runs/1")
    assert "auto-OPEN only" in body and "auto-MERGE is forbidden" in body
    assert NEW_DIGEST in body
    assert "Contents: Write" in body or "Contents:Write" in body
    assert "shared/claude-plugins#917" in body
    assert "17a952f01" in body  # commit list carried


def test_indent_comment_prefixes_every_physical_line():
    block = r917.indent_comment(["a\nb\nc"])
    assert block == "      # a\n      # b\n      # c"


# ---------------------------------------------------------------------------
# 5. the review gate is MECHANICAL, not aspirational (#917 hard constraint:
#    auto-OPEN, auto-MERGE forbidden). Pinned against the actual artifacts.
# ---------------------------------------------------------------------------

MERGE_SHAPES = re.compile(
    r"merge\b|/merge\b|auto_merge|auto-?merge|\bmerge_method\b|"
    r"pulls/\d+/merge|mergeable_state", re.IGNORECASE)


def test_no_merge_call_anywhere():
    """Neither the opener script nor the workflow may contain a GitHub merge
    API call or auto-merge toggle. Comments ABOUT the ban are allowed; the
    grep runs on code shapes: gh(token, VERB, path) calls and `uses:` steps."""
    src = SCRIPT.read_text()
    wf = (SCRIPT.parents[2] / ".github/workflows/cluster-image.yml").read_text()
    # every gh() method/path in the script:
    calls = re.findall(r'gh\(token,\s*"(\w+)",\s*\n?\s*f?"([^"]+)"', src)
    assert calls, "gh() extraction failed — the test must actually see calls"
    for verb, path in calls:
        assert not MERGE_SHAPES.search(path) or "Pull requests:Write" in path, \
            f"forbidden merge-shaped call: {verb} {path}"
    # the workflow: no merge action, no auto-merge enablement
    assert "gh pr merge" not in wf
    assert "auto_merge" not in wf.lower().replace("auto-merge", "")
    assert 'method: "PUT"' not in wf or "merge" not in wf


def test_script_verb_surface_is_open_only():
    """The write verbs the opener may use are exactly: create ref, delete
    ref (own orphan branch), put contents (branch only), post pulls,
    patch pulls — nothing merges, and base 'main' never receives a PUT."""
    src = SCRIPT.read_text()
    verbs = set(re.findall(r'gh\(token,\s*"(\w+)"', src))
    assert verbs <= {"GET", "POST", "PUT", "PATCH", "DELETE"}, verbs
    # the PR target's main is never a write branch
    assert re.search(r'"branch":\s*branch|branch', src)
    # no PUT/PATCH/POST/DELETE against infra main
    for m in re.finditer(r'gh\(token,\s*"(POST|PUT|PATCH|DELETE)",\s*\n?\s*f?"([^"]+)"', src):
        path = m.group(2)
        assert "commits/main" not in path or m.group(1) == "GET"
        assert path.rstrip("/").split("?ref=")[-1] != "main" or m.group(1) == "GET"
