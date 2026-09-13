#!/usr/bin/env python3
"""Proof: k8s_gateway (the token ESO gives the gateway) is ACCEPTED by a real
cluster main, and k8s_not_registered is rejected. Spins create_app() locally
exactly the way the hermes-main container does (same middleware, same
PEER_TOKEN/PEER_TOKENS env contract), with k8s_gateway in the peer map — the
state hermes-main reaches once SM map v2 + a pod restart land.

Run from a worktree of Bdaya-Dev/hermes-agent-cluster with the repo venv:
  env -u PEER_TOKENS -u PEER_TOKEN .venv/bin/python /path/proof_892_gateway_accepted.py
"""
import os
import sys

os.environ["PEER_TOKEN"] = "s" * 64
os.environ["PEER_TOKENS"] = "k8s_main:" + "s" * 64 + ",k8s_gateway:" + "s" * 64
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hermes_cluster.app import create_app  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

app = create_app(cluster_id="proof", node_id="k8s_main", node_role="main",
                 config_path="", fed_token="", static_dir=None)
client = TestClient(app)

def signed(node_id, token, path="/api/v1/tasks"):
    import hashlib, hmac, time
    ts = int(time.time())
    msg = f"{node_id}:{ts}:GET:{path}:{hashlib.sha256(b'').hexdigest()}"
    sig = hmac.new(token.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {"X-Peer-Node": node_id, "X-Peer-Timestamp": str(ts), "X-Peer-Signature": sig}

# 1) the blindness: unsigned 401
r = client.get("/api/v1/tasks")
print("unsigned            ->", r.status_code, "(expect 401)")
assert r.status_code == 401

# 2) gateway env (PEER_TOKEN from ESO, node_id k8s_gateway from deployment env) -> 200
r = client.get("/api/v1/tasks", headers=signed("k8s_gateway", "s" * 64))
print("k8s_gateway signed  ->", r.status_code, "(expect 200)")
assert r.status_code == 200

# 3) an unregistered node with the SAME token -> 401 (proves registration is
#    what grants visibility — the k8s_gateway map entry is load-bearing)
r = client.get("/api/v1/tasks", headers=signed("k8s_not_registered", "s" * 64))
print("unregistered signed ->", r.status_code, "(expect 401)")
assert r.status_code == 401

print("PROOF-892-ACCEPT: gateway token+node_id is a first-class peer of main")
