"""Grouping oracle over the REAL corpus: run the lane branch's candidate
filter + packer over every open issue in the configured scopes; report the
lane/bundle counts vs the 872 per-issue anti-pattern; verify caps + band-0.
Auth: `glab api` with GITLAB_HOST pinned. Never prints token values."""
import json, os, re, subprocess, sys
from collections import Counter

sys.path.insert(0, ".")

HOST = "gitlab.bdaya-dev.com"

def gl(path, **params):
    q = "&".join(f"{k}={v}" for k, v in params.items())
    url = path + (("?" + q) if q else "")
    env = dict(os.environ)
    env["GITLAB_HOST"] = HOST
    out = subprocess.run(["glab", "api", url], capture_output=True,
                         text=True, timeout=90, env=env)
    if out.returncode != 0:
        raise RuntimeError(f"glab api {url} failed: {out.stderr[:160]}")
    return json.loads(out.stdout)

scopes = [("group", "invora"), ("project", "shared/claude-plugins"),
          ("group", "metaphor/bayader"), ("group", "metaphor/morshdy")]

corpus = {}
for kind, path in scopes:
    quoted = re.sub(r"/", "%2F", path)
    base = f"{kind}s/{quoted}/issues"
    page = 1
    while True:
        try:
            batch = gl(f"/{base}", state="opened", per_page=100, page=page,
                       order_by="created_at", sort="asc")
        except Exception as e:
            print(f"scope {path} p{page}: {e}")
            batch = []
        for iss in batch:
            full = (iss.get("references") or {}).get("full", path + "#?")
            proj = full.rsplit("#", 1)[0] if "#" in full else path
            corpus[f"{proj}#{iss['iid']}"] = iss
        if len(batch) < 100 or page >= 25:
            break
        page += 1
print(f"corpus: {len(corpus)} open issues")

from hermes_cluster.core.intake_grouping import (
    GroupingConfig, LaneView, bundle_plan_for_repo, filter_candidates,
    is_doomed_label, lane_key_for,
)

cfg = GroupingConfig(enabled=True)   # production defaults incl. skip patterns
AUTHOR_IDS = {12: 0, 8: 0}

def band_for(iss):
    return AUTHOR_IDS.get((iss.get("author") or {}).get("id"), 3)

doomed = Counter()
per_project = Counter()
for key, iss in corpus.items():
    proj = key.rsplit("#", 1)[0]
    hit = [l for l in (iss.get("labels") or []) if is_doomed_label(l, cfg)]
    if hit:
        for l in hit:
            doomed[l] += 1
    else:
        per_project[proj] += 1
print("\ndoomed-label census:")
for l, c in doomed.most_common(10):
    print(f"  {l}: {c}")
print(f"\nrepos with candidates: {len(per_project)}  (candidate issues: {sum(per_project.values())})")
for p, c in per_project.most_common(12):
    print(f"  {p}: {c}")

# Bundle plan per repo with the REAL planner (no GitLab DAG/MR census here:
# conservative upper bound of the first sitting per repo).
view = LaneView()
total_bundles = 0
first_sitting_sizes = {}
for proj, count in per_project.items():
    pairs = [(k, corpus[k]) for k in corpus if k.rsplit("#", 1)[0] == proj]
    cands = filter_candidates(pairs, config=cfg, band_for=band_for)
    lane = lane_key_for(proj, cfg)
    plan = bundle_plan_for_repo(lane, cands, cfg, view=view,
                                project_of=lambda pid: pid.rsplit("#", 1)[0])
    if plan is None:
        continue
    total_bundles += 1
    first_sitting_sizes[lane] = (len(cands), len(plan.iids))
    assert len(plan.iids) <= cfg.max_bundle_size, "cap violated"

print(f"\nFIRST-SITTING view: {total_bundles} lane tasks (one per active repo) "
      f"vs 872 per-issue tasks")
big = sorted(first_sitting_sizes.items(), key=lambda kv: -kv[1][0])[:12]
for lane, (avail, picked) in big:
    sittings = -(-avail // cfg.max_bundle_size)
    print(f"  {lane}: {picked} bundled now (cap {cfg.max_bundle_size}); "
          f"{avail} ready -> ~{sittings} sittings total")
tot_sit = sum(-(-a // cfg.max_bundle_size) for a, _ in first_sitting_sizes.values())
print(f"\nESTIMATE: ~{tot_sit} lane-sitting tasks vs 872 per-issue tasks "
      f"= {872 / max(tot_sit,1):.1f}x fewer review gates at worst")
json.dump({k: v for k, v in first_sitting_sizes.items()},
          open("/tmp/oracle_70fa.json", "w"), indent=1)
