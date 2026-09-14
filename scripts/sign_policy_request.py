#!/usr/bin/env python3
"""Sign-and-send a policy WRITE to the cluster main node (PR#36 finding 1).

The policy PUT accepts EITHER a plain X-Gitlab-Token carrying
GITLAB_INTAKE_POLICY_SECRET (the operator path — no signing needed) OR a
peer-HMAC signature (the cluster-node path). This helper covers the second:
it builds the exact signature auth_middleware verifies
(node:ts:METHOD:path?query:sha256(body)) and issues the PUT.

Usage (from any machine that holds a peer token):

    python scripts/sign_policy_request.py \
        --url https://brain.example.internal/api/v1/intake/gitlab/policy \
        --node-id operator-cli --token-env PEER_TOKEN \
        --file policy.json

The token is read from the environment (default: PEER_TOKEN); it is never
printed. --print-only emits the header dict (still without the signature?
no — the signature IS the secret-equivalent for one request, so it prints
only with --i-know-what-im-doing).
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request


def sign_headers(node_id: str, token: str, method: str, url: str,
                 body: bytes, timestamp: int | None = None) -> dict:
    """Mirror of core/peer_auth.py::PeerAuthState.sign_request.

    Kept self-contained on purpose: this script must run on an operator
    laptop without the cluster package installed.
    """
    parsed = urllib.parse.urlsplit(url)
    path = parsed.path
    if parsed.query:
        path = f"{path}?{parsed.query}"
    ts = timestamp or int(time.time())
    body_hash = hashlib.sha256(body).hexdigest()
    message = f"{node_id}:{ts}:{method}:{path}:{body_hash}"
    signature = hmac.new(token.encode(), message.encode(), hashlib.sha256).hexdigest()
    return {
        "X-Peer-Node": node_id,
        "X-Peer-Timestamp": str(ts),
        "X-Peer-Signature": signature,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", required=True,
                    help="full policy URL, e.g. https://host/api/v1/intake/gitlab/policy")
    ap.add_argument("--node-id", default="operator-cli")
    ap.add_argument("--token-env", default="PEER_TOKEN",
                    help="environment variable holding the peer token (never printed)")
    ap.add_argument("--file", help="JSON policy body file; default: read stdin")
    ap.add_argument("--method", default="PUT")
    args = ap.parse_args(argv)

    token = os.environ.get(args.token_env, "")
    if not token:
        print(f"error: {args.token_env} is unset — a peer token is required",
              file=sys.stderr)
        return 2

    body = (open(args.file, "rb").read() if args.file else sys.stdin.buffer.read())
    # Validate the JSON locally before spending a request on it.
    try:
        json.loads(body or b"{}")
    except json.JSONDecodeError as e:
        print(f"error: policy body is not valid JSON: {e}", file=sys.stderr)
        return 2

    headers = sign_headers(args.node_id, token, args.method, args.url, body)
    headers["Content-Type"] = "application/json"
    req = urllib.request.Request(args.url, data=body, headers=headers,
                                 method=args.method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(resp.read().decode())
            return 0
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode(errors='replace')}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
