"""Release drift detection for the hosted cluster main (shared/claude-plugins#917).

A merge to the fork's ``main`` builds and pushes an image
(``.github/workflows/cluster-image.yml``) and then NOTHING moves the release
pin — ``bdaya-website-infra`` ``cluster-config/hermes-main/kustomization.yaml``
``images: newTag`` — so merged code sits undeployed, and invisibly: the live
main answers ``/health`` 200, pods are Running, ArgoCD reports Synced, because
it IS reconciled, to the stale pin. Measured on 2026-09-15: ten commits sat
merged-and-not-running for nearly eight hours, and the gap reopened 13 minutes
after the hand-cut release that closed it.

This module compares the DEPLOYED pin to the fork ``main`` head and reports the
drift; ``evaluate_drift_alert`` decides when to scream. The scream rides the
cluster's EXISTING webhook fan-out (the same ``task_failed``-hook surface the
metering poller's ``emit_alert`` uses — the Telegram relay subscribes to it),
and the GitHub workflow additionally files an Issue.

The release itself stays exactly what it is: a GitOps manifest bump, opened as
a REVIEWED PR, merged by a human or reviewer lane. This detector can never move
a pin — it holds no infra-repo write credential and writes only notes.

Pure logic is dependency-free (stdlib only), mirroring the signing helpers of
``core.peer_auth`` so it is importable from stdlib-only contexts.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("hermes_cluster.release_drift")

FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# The pin and the image, stated once — the values cluster-image.yml's header
# and the infra kustomization already document in prose.
PIN_FILE = "cluster-config/hermes-main/kustomization.yaml"
INFRA_REPO = "Bdaya-Dev/bdaya-website-infra"
FORK_REPO = "Bdaya-Dev/hermes-agent-cluster"
IMAGE_BASE = "europe-west1-docker.pkg.dev"
IMAGE_PROJECT = "bdaya-website"
IMAGE_REPO = "bdaya-docker"
IMAGE_NAME = "hermes-cluster-main"


class DriftConfigError(ValueError):
    """The release_drift config section is malformed (fail-loud shape of
    parse_metering_settings: the poller must run NO cycle on bad config)."""


@dataclass
class DriftConfig:
    enabled: bool = False
    interval_s: int = 900
    # grace period: drift must persist this long before the alarm fires —
    # covers the normal image-build + ArgoCD-sync window.
    grace_s: int = 1800
    # absolute backstop: alarm at latest this long after the merge, whatever
    # the commit count.
    max_age_s: int = 3600
    # commit-count threshold: > N commits of drift alarms immediately
    # (grace applies only up to this many commits).
    commit_threshold: int = 2
    image: str = f"{IMAGE_BASE}/{IMAGE_PROJECT}/{IMAGE_REPO}/{IMAGE_NAME}"
    pin_repo: str = INFRA_REPO
    pin_file: str = PIN_FILE
    main_repo: str = FORK_REPO
    main_branch: str = "main"
    http_timeout: int = 15
    adc_token_file: str = ""
    # Acknowledgement seam: the 40-char fork-main head an OPEN release PR
    # already covers. The pod cannot read the private infra repo's PR list
    # (no GitHub credential there — WIF is Secret-Manager-only), so a human
    # or reviewer lane that HAS confirmed a PR is in flight sets it via
    # PUT /api/v1/config; while it matches the drifted head the "owed" alarm
    # suppresses (stalled-escalation backstop still applies). Empty = never
    # suppress: staying silent was the original defect (#917).
    in_flight_head: str = ""


def _require_int(section: Dict[str, Any], key: str, default: int) -> int:
    raw = section.get(key, default)
    try:
        val = int(raw)
    except (TypeError, ValueError):
        raise DriftConfigError(f"release_drift.{key} is not an integer")
    return val


def _require_bool(section: Dict[str, Any], key: str, default: bool) -> bool:
    raw = section.get(key, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and raw.lower() in ("true", "false"):
        return raw.lower() == "true"
    raise DriftConfigError(f"release_drift.{key} is not a boolean")


def parse_drift_settings(config: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[DriftConfig]]:
    """(valid, parsed) from a runtime/yaml config dict's ``release_drift``
    section.

    valid=False means the section is malformed — the poller refuses to run a
    cycle and records the parse error instead of spinning on bad config
    (same contract the metering section's parse enforces).
    """
    section = (config or {}).get("release_drift") if isinstance(config, dict) else None
    if section is None:
        return True, DriftConfig()
    if not isinstance(section, dict):
        return False, None
    try:
        cfg = DriftConfig(
            enabled=_require_bool(section, "enabled", False),
            interval_s=_require_int(section, "interval_s", 900),
            grace_s=_require_int(section, "grace_s", 1800),
            max_age_s=_require_int(section, "max_age_s", 3600),
            commit_threshold=_require_int(section, "commit_threshold", 2),
            image=str(section.get("image", DriftConfig.image)),
            pin_repo=str(section.get("pin_repo", INFRA_REPO)),
            pin_file=str(section.get("pin_file", PIN_FILE)),
            main_repo=str(section.get("main_repo", FORK_REPO)),
            main_branch=str(section.get("main_branch", "main")),
            http_timeout=_require_int(section, "http_timeout", 15),
            adc_token_file=str(section.get("adc_token_file", "")),
            in_flight_head=str(section.get("in_flight_head", "")).strip().lower(),
        )
    except DriftConfigError:
        return False, None
    if cfg.interval_s < 30 or cfg.grace_s < 0 or cfg.max_age_s <= 0 \
            or cfg.commit_threshold < 1 or cfg.http_timeout <= 0:
        return False, None
    return True, cfg


def extract_pinned_sha(kustomization_text: str) -> Optional[str]:
    """The ``newTag`` of the hermes-cluster-main image block — the deployed pin.

    Regex over the manifest, NOT a yaml parse: cluster-config has its own
    schema and kustomize quirks; the pin shape here is fixed by the release
    contract ("a release = one commit bumping newTag here") and train comments
    carry inline text this must tolerate.
    """
    block = re.search(
        r"^\s*-\s*name:\s*hermes-cluster-main\s*$(?:[ \t]*\n[ \t]+\S.*)*?[ \t]*\n[ \t]*newTag:\s*\"?([0-9a-fA-F]+)\"?",
        kustomization_text, re.MULTILINE)
    if not block:
        return None
    sha = block.group(1).lower()
    return sha if FULL_SHA_RE.match(sha) else None


def compute_drift(pin_sha: str, main_sha: str,
                  commits_ahead: Optional[int] = None) -> Dict[str, Any]:
    """Compare the deployed pin to the fork main head.

    commits_ahead: number of commits in ``pin..main`` (touching the release
    path). When None it is derived cheaply as same/not-same — a detector that
    merely diffs two strings still ALARMS on a stale pin, which is the
    acceptance property; the ancestry count only sharpens the message.
    """
    for name, sha in (("pin_sha", pin_sha), ("main_sha", main_sha)):
        if not isinstance(sha, str) or not FULL_SHA_RE.match(sha):
            raise ValueError(f"{name} must be a 40-char hex sha, got {sha!r}")
    pin, main = pin_sha.lower(), main_sha.lower()
    drifted = pin != main
    if commits_ahead is None:
        commits_ahead = 1 if drifted else 0
    else:
        commits_ahead = int(commits_ahead)
    if not drifted:
        commits_ahead = 0
    return {"pin_sha": pin, "main_sha": main, "drifted": drifted,
            "commits_ahead": commits_ahead}


def evaluate_drift_alert(drift: Dict[str, Any], state: Dict[str, Any],
                         cfg: DriftConfig, now: float) -> Optional[Dict[str, Any]]:
    """Decide whether THIS poll cycle should fire a drift alert, updating
    ``state`` (the poller's alert bookkeeping) in place.

    Semantics:
      * pin == main  -> clear streak and suppression, no alert.
      * drifted      -> track the main head the same way the #879 health
            self-report's consecutive_failures counter works (reset only on
            recovery, never mid-drift), so the age basis is the FIRST sighting
            of this drifted state; a poller restart re-establishes the streak
            (a restart is not a recovery — same rule as #879's
            consecutive_saturated).
      * fire when: commits_ahead > cfg.commit_threshold, OR the streak age has
            passed cfg.grace_s, OR it has passed cfg.max_age_s (the absolute
            backstop when grace is configured 0 or drift creeps under the
            threshold one commit per interval).
      * suppression: one alert per (main head, threshold class) — identical
            alerts don't re-fire, a NEW drifted head does (the gap reopened 13
            minutes after train 5: a fresh head must scream again).
      * unknown (detection itself failed) -> alert kind='unknown' past the
            threshold cycles: "cannot see" is exactly the blind spot this
            feature exists to close, so it must not pass silently.

    Returns the alert dict (kind, message, ...) or None. Pure function —
    clock and state injected.
    """
    now = float(now)
    if not drift.get("drifted"):
        state["first_seen_ts"] = None
        state["cycles"] = 0
        state["head"] = None
        state["last_alert_key"] = None
        return None

    head = drift.get("main_sha")
    if not state.get("first_seen_ts"):
        # age basis = FIRST sighting of this drifted EPISODE. A head change
        # mid-drift must NOT renew it: drift creeping one commit per interval
        # would then sit forever inside a re-armed grace window (the max_age
        # backstop exists precisely because of that shape).
        state["first_seen_ts"] = now
    if state.get("head") != head:
        state["head"] = head
        state["last_alert_key"] = None  # a fresh head is a fresh alarm
    state["cycles"] = int(state.get("cycles", 0)) + 1

    age = now - float(state.get("first_seen_ts", now))
    commits = int(drift.get("commits_ahead", 1))
    in_flight = drift.get("release_in_flight")
    if in_flight:
        # A release PR IS open for this head — noticed, awaiting review/merge.
        # That is the healthy pipeline mid-flight, not the defect. Suppress
        # the "owed" scream; the backstop still rules: at 3x max_age even an
        # open PR is stalled (train 6 sat open 24h; a PR that never merges
        # must re-escalate as a PERSON, not silence).
        if age >= 3 * cfg.max_age_s:
            reason = "stalled"
        else:
            return None
    else:
        reason = None
        if commits > cfg.commit_threshold:
            reason = "commits"
        elif age >= cfg.max_age_s:
            reason = "max_age"
        elif age >= cfg.grace_s:
            reason = "grace"
        if reason is None:
            return None

    key = f"drift:{head}:{reason}" if head else f"drift:{reason}"
    if state.get("last_alert_key") == key:
        return None
    state["last_alert_key"] = key
    n_commits = f"{commits} commit(s) " if commits else ""
    if reason == "stalled":
        message = (f"hermes-cluster-main drift: a release PR for fork main "
                   f"{(head or '')[:9]} has been open for {int(age)}s without "
                   f"merging (deployed build is {(drift.get('pin_sha') or '')[:9]}). "
                   f"A review/merge decision is owed — never kubectl.")
    else:
        message = (f"hermes-cluster-main image deployed from fork commit "
                   f"{(drift.get('pin_sha') or '')[:9]} but {n_commits}"
                   f"merged to {FORK_REPO}@{cfg.main_branch} have not been "
                   f"released (main head {(head or '')[:9]}, drift age "
                   f"{int(age)}s). A release PR is owed: bump "
                   f"{cfg.pin_file} newTag to the main head — never kubectl. "
                   f"If a release PR for {(head or '')[:9]} is already open, "
                   f"acknowledge via PUT /api/v1/config "
                   f"release_drift.in_flight_head={(head or '')[:40]}")
    return {
        "kind": "release_drift",
        "trigger": reason,
        "pin_sha": drift.get("pin_sha"),
        "main_sha": head,
        "commits_ahead": commits,
        "drift_age_s": int(age),
        "release_in_flight": bool(in_flight),
        "message": message,
    }


def build_digest_url(cfg: DriftConfig, tag: str) -> str:
    """GAR v2 manifests URL for an image tag. The digest is read from the
    ``Docker-Content-Digest`` response HEADER (HEAD request) — never by
    parsing manifest bytes.

    Derived from cfg.image (the full docker URL the pin carries), NOT from the
    constants, so an image override changes the lookup too: <host>/v2/projects/
    <project>/repos/<repo>/images/<name>/manifests/<tag>.
    """
    if not re.fullmatch(r"[0-9a-zA-Z._-]{1,128}", tag):
        raise ValueError(f"invalid image tag for digest lookup: {tag!r}")
    parts = cfg.image.split("/")
    if len(parts) != 4 or not all(parts):
        raise ValueError(f"release_drift.image is not <host>/<project>/<repo>/"
                         f"<name>: {cfg.image!r}")
    host, project, repo, name = parts
    return (f"https://{host}/v2/projects/{project}/repos/{repo}"
            f"/images/{name}/manifests/{tag}")


def _gh_api(cfg: DriftConfig, path: str, token: Optional[str]) -> Any:
    req = urllib.request.Request(
        f"https://api.github.com/{path.lstrip('/')}",
        headers={"Accept": "application/vnd.github+json",
                 "User-Agent": "hermes-cluster-main/release-drift",
                 **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req, timeout=cfg.http_timeout) as r:
        return json.load(r)


def _http_head_digest(url: str, token: Optional[str], timeout: int = 15) -> Optional[str]:
    """HEAD the manifests URL, return the raw Docker-Content-Digest header.

    Seam for tests + the only place that touches the network in this module.
    """
    headers = {"Accept": "application/vnd.docker.distribution.manifest.list.v2+json, "
                         "application/vnd.oci.image.index.v1+json, */*",
               "User-Agent": "hermes-cluster-main/release-drift",
               **({"Authorization": f"Bearer {token}"} if token else {})}
    req = urllib.request.Request(url, headers=headers, method="HEAD")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return (r.headers.get("Docker-Content-Digest") or "").lower()


def _resolve_one(cfg: DriftConfig, tag: str,
                 token: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Resolve one tag to a digest; (digest, error). Errors are values, never
    raised: the drift ALARM must survive a broken digest lookup."""
    try:
        raw = _http_head_digest(build_digest_url(cfg, tag), token,
                                timeout=cfg.http_timeout)
    except urllib.error.HTTPError as exc:
        return None, f"registry HTTP {exc.code} for tag {tag[:12]}"
    except Exception as exc:
        return None, str(exc)[:200]
    if raw and DIGEST_RE.match(raw):
        return raw, None
    return None, f"registry returned no usable digest for tag {tag[:12]}"


def resolve_release_digests(cfg: DriftConfig, pin_sha: str, main_sha: str,
                            gh_token: Optional[str] = None,
                            adc_token: Optional[str] = None) -> Dict[str, Any]:
    """Resolve both image tags to digests against the registry — RESOLVED, not
    copied from a previous train (acceptance #917: the digest in the release
    body must come from Artifact Registry).

    The docker-v2 manifests endpoint is the same API gcloud uses; under the
    estate's WIF discipline the image build SA holds ``artifactregistry.reader``
    on bdaya-docker, so a short-lived ADC token authenticates the read. Without
    one, the endpoint may still answer anonymously for this repo — if it does,
    good; if not, digest=None with a reason: a missing digest must not sink the
    drift ALARM (the alarm is the safety property; the digest is PR-body
    decoration).
    """
    out: Dict[str, Any] = {"pin_digest": None, "main_digest": None,
                           "pin_error": None, "main_error": None}
    if gh_token:
        try:
            # the cluster-image build run for the main head — proves the image
            # exists and when it was published (train-comment format).
            runs = _gh_api(cfg, f"repos/{cfg.main_repo}/actions/runs"
                           f"?event=push&head_sha={main_sha}", gh_token)
            for run in runs.get("workflow_runs", []):
                if run.get("name") == "Cluster Image":
                    out["image_run_id"] = run.get("id")
                    out["image_published_at"] = run.get("updated_at")
                    break
        except Exception as exc:
            logger.warning("release_drift: image run lookup failed: %s", exc)
    for key, sha in (("pin", pin_sha), ("main", main_sha)):
        digest, err = _resolve_one(cfg, sha, adc_token)
        out[f"{key}_digest"] = digest
        out[f"{key}_error"] = err
    return out
