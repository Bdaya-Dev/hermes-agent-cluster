"""Live proof for #917: the drift detector against REAL current state.

No fakes: real GitHub public-API reads of the fork, the real measured
train-5 pin as 'deployed', and the real Artifact Registry v2 endpoint for
the digest (anonymous HEAD; if the registry requires auth from here, that
failure is reported honestly — the alarm must not depend on it).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hermes_cluster.core.release_drift_poller import ReleaseDriftPoller

DEPLOYED = "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a"  # train 6 MERGED — infra main current pin
# real infra main right now:
os.environ["HERMES_CLUSTER_BUILD_COMMIT"] = DEPLOYED


class _State:
    def get_config(self):
        return {"release_drift": {"enabled": True, "interval_s": 300,
                                  "grace_s": 0, "max_age_s": 3600,
                                  "commit_threshold": 2}}


emitted = []
p = ReleaseDriftPoller(state=_State(), hook_manager=None)
p.emit_alert = lambda a: emitted.append(a)
r = p.poll_once()
print("=== LIVE DRIFT CHECK (real GitHub + real GAR) ===")
print("poll result:", {k: r.get(k) for k in ("ok", "status", "drifted")})
st = p.status()
s = st.get("sample") or {}
print("deployed  :", s.get("deployed_commit"))
print("main head :", s.get("main_head"))
print("drift     :", s.get("drift"))
print("pin_digest:", s.get("pin_digest"), s.get("pin_error") or "")
print("head_digest:", s.get("main_digest"), s.get("main_error") or "")
print("range     :", s.get("range"))
print("recent commits:")
for c in (s.get("recent_commits") or [])[:6]:
    print("   ", c["sha"][:9], c["message"].splitlines()[0][:70])
print("ALARM FIRED:", bool(emitted))
if emitted:
    print("alert kind:", emitted[0]["kind"], "trigger:", emitted[0]["trigger"])
    print("message:", emitted[0]["message"])
print("last_errors:", st.get("last_errors"))
