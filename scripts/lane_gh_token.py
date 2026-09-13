#!/usr/bin/env python3
"""lane-gh-token — mint (and cache) a bdaya-lane-agent GitHub App installation
token for worker lanes (shared/claude-plugins#867).

Design (chosen: option (a)+(b) hybrid — per-node on-demand mint from a
fleet-synced, node-local private-key file):
  * The App private key lives in GCP Secret Manager
    (bdaya-website/lane-agent-github-app-private-key) and is hydrated by
    fleet-apply to a node-local file with tight perms. The key NEVER goes
    into an env var and is NEVER printed.
  * This resolver reads the mint config from a config file
    (~/.config/bdaya/lane-gh-token.json), per the owner ruling that nothing
    should be configured from env vars.
  * Tokens are minted on demand and cached at
    $XDG_CACHE or ~/.cache/bdaya/lane-gh-token.json with a skew; a caller
    always gets a token with > MIN_REMAINING seconds of life left, refreshing
    transparently when the cached one is older/older-than-an-hour.

Usage:
  lane_gh_token.py get             -> print token to stdout (only output path)
  lane_gh_token.py get --json      -> {"token":..,"expires_at":..,"source":"cache|minted"}
  lane_gh_token.py status          -> non-secret health line (cache state, byte
                                      lengths, crc32 of key — never values)
  lane_gh_token.py hydrate-key     -> pull the private key file from GCP SM via
                                      ADC (fleet-apply hook; idempotent)
  lane_gh_token.py mint-test       -> force a fresh mint and report expiry only

Never prints a secret value. Secret material asserts byte length / crc32 only.
"""
from __future__ import annotations

import base64
import binascii
import json
import os
import stat
import sys
import time
import urllib.request

CONFIG_PATH = os.path.expanduser("~/.config/bdaya/lane-gh-token.json")
CACHE_PATH = os.path.expanduser("~/.cache/bdaya/lane-gh-token.json")
MIN_REMAINING = 300  # seconds of validity we insist on before refreshing

DEFAULTS = {
    "project": "bdaya-website",
    "app_id_secret": "lane-agent-github-app-id",
    "installation_id_secret": "lane-agent-github-app-installation-id",
    "private_key_secret": "lane-agent-github-app-private-key",
    # node-local key file, hydrated from SM; NOT an env var, NOT a literal repo file
    "private_key_file": os.path.expanduser("~/.config/bdaya/lane-agent-app.pem"),
    "api": "https://api.github.com",
}


def load_config() -> dict:
    cfg = dict(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg.update(json.load(f))
    return cfg


def _sm_access(cfg: dict, name: str) -> bytes:
    """Read latest version of a SM secret via ADC token. Never logs the value."""
    token = None
    for tok_path in (cfg.get("adc_token_file"), os.path.expanduser("~/.config/gcloud/adc_token")):
        if tok_path and os.path.exists(tok_path):
            with open(tok_path) as f:
                token = f.read().strip()
            if token:
                break
    if not token:
        import shutil
        import subprocess
        gcloud = (shutil.which("gcloud") or shutil.which("gcloud.exe")
                  or next((p for p in (
                      os.path.expanduser("~") + "/AppData/Local/google-cloud-sdk/bin/gcloud",
                      "/opt/homebrew/bin/gcloud", "/usr/local/bin/gcloud")
                      if os.path.exists(p)), "gcloud"))
        token = subprocess.run(
            [gcloud, "auth", "application-default", "print-access-token"],
            capture_output=True, text=True, check=True).stdout.strip()
    url = (f"https://secretmanager.googleapis.com/v1/projects/{cfg['project']}"
           f"/secrets/{name}/versions/latest:access")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.load(r)
    return base64.b64decode(payload["payload"]["data"])


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def mint_token(cfg: dict, now: float | None = None) -> tuple[str, float]:
    """RS256 JWT -> POST /app/installations/<id>/access_tokens. Returns (token, expires_at epoch)."""
    key_file = cfg["private_key_file"]
    if not os.path.exists(key_file):
        raise SystemExit(f"FATAL: App private key not provisioned at {key_file} "
                         "(run hydrate-key; fleet provisioning owns this file per #867/#788)")
    key_pem = open(key_file, "rb").read()
    if not key_pem:
        raise SystemExit("FATAL: private key file is empty")
    app_id = str(cfg.get("app_id", "")).strip()
    installation_id = str(cfg.get("installation_id", "")).strip()
    if not app_id or not installation_id:
        raise SystemExit("FATAL: lane-gh-token.json missing app_id/installation_id")

    t = int(now if now is not None else time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = _b64url(json.dumps({"iat": t - 60, "exp": t + 540, "iss": app_id}).encode())
    signing_input = f"{header}.{claims}".encode()
    sig = _sign_rs256(key_file, key_pem, signing_input)
    jwt = f"{signing_input.decode()}.{_b64url(sig)}"

    url = f"{cfg['api']}/app/installations/{installation_id}/access_tokens"
    req = urllib.request.Request(url, data=b"{}", method="POST", headers={
        "Authorization": f"Bearer {jwt}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "lane-gh-token",
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.load(r)
    # 201 Created; expires_at is ISO8601 UTC — parse timezone-aware, never
    # via time.mktime (which would read UTC as local time).
    from datetime import datetime, timezone
    expires = datetime.strptime(out["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp()
    return out["token"], expires


def _sign_rs256(key_file: str, key_pem: bytes, signing_input: bytes) -> bytes:
    """Sign with `cryptography` if present (Windows/pc), else shell out to
    openssl (stock macOS python3 has no pip cryptography; LibreSSL's
    `dgst -sha256 -sign` accepts the same PKCS#1 RSA PEM)."""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        key = serialization.load_pem_private_key(key_pem, password=None)
        return key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except ImportError:
        pass
    import shutil
    import subprocess
    import tempfile
    openssl = shutil.which("openssl")
    if not openssl:
        raise SystemExit("FATAL: neither python 'cryptography' nor openssl available to sign the App JWT")
    with tempfile.TemporaryDirectory() as td:
        bin_path = os.path.join(td, "si.bin")
        sig_path = os.path.join(td, "si.sig")
        with open(bin_path, "wb") as f:
            f.write(signing_input)
        subprocess.run([openssl, "dgst", "-sha256", "-sign", key_file,
                        "-out", sig_path, bin_path], check=True,
                       capture_output=True)
        return open(sig_path, "rb").read()


def read_cache(cfg: dict):
    if not os.path.exists(CACHE_PATH):
        return None
    try:
        with open(CACHE_PATH, encoding="utf-8") as f:
            c = json.load(f)
        if float(c["expires_at"]) - time.time() > MIN_REMAINING:
            return c
    except (ValueError, KeyError, OSError):
        pass
    return None


def write_cache(token: str, expires: float) -> None:
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    tmp = CACHE_PATH + f".tmp{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"token": token, "expires_at": expires,
                   "minted_at": time.time()}, f)
    os.replace(tmp, CACHE_PATH)
    try:
        os.chmod(CACHE_PATH, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def cmd_get(as_json: bool, force: bool) -> int:
    cfg = load_config()
    # Self-provisioning (#867): if the key/config are absent but ADC+SM are
    # reachable, hydrate now. Provisioning = "this node can already read
    # bdaya-website SM" — the estate's existing credential-sync mechanism
    # (same ADC->SM chain as the alibaba key and run-worker-macbook.sh) —
    # so a new member needs ZERO hand-placed files, and a node without SM
    # access fails LOUDLY here (the probe maps it to MISSING at dispatch).
    kf = cfg["private_key_file"]
    if not (os.path.exists(kf) and cfg.get("app_id") and cfg.get("installation_id")):
        try:
            cmd_hydrate_key(force=False)
        except SystemExit:
            raise
        except Exception:
            pass  # fall through — mint_token raises the precise MISSING error
        cfg = load_config()
    src = "cache"
    c = None if force else read_cache(cfg)
    if c is None:
        token, expires = mint_token(cfg)
        write_cache(token, expires)
        src = "minted"
    else:
        token, expires = c["token"], float(c["expires_at"])
    if as_json:
        print(json.dumps({"token": token, "expires_at": expires, "source": src}))
    else:
        sys.stdout.write(token)
    return 0


def cmd_status() -> int:
    cfg = load_config()
    kf = cfg["private_key_file"]
    if os.path.exists(kf):
        raw = open(kf, "rb").read()
        key_line = f"key=present bytes={len(raw)} crc32={binascii.crc32(raw) & 0xffffffff:08x}"
    else:
        key_line = "key=MISSING"
    c = read_cache(cfg)
    if c:
        rem = int(float(c["expires_at"]) - time.time())
        cache_line = f"cache=fresh remaining={rem}s"
    else:
        cache_line = "cache=none-or-expired"
    print(f"lane-gh-token status: {key_line}; {cache_line}; "
          f"min_remaining={MIN_REMAINING}s cfg={CONFIG_PATH}")
    return 0 if "present" in key_line else 1


def cmd_hydrate_key(force: bool = True) -> int:
    cfg = load_config()
    kf = cfg["private_key_file"]
    if not force and os.path.exists(kf) and os.path.getsize(kf) > 100 \
            and cfg.get("app_id") and cfg.get("installation_id"):
        print("key already provisioned (idempotent --if-missing); nothing to do")
        return 0
    os.makedirs(os.path.dirname(kf), exist_ok=True)
    data = _sm_access(cfg, cfg["private_key_secret"])
    # never compare against a literal in logs; just write bytes
    tmp = kf + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, kf)
    try:
        os.chmod(kf, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    # pull the small ids too, fold them into the config file (they are ids, not secrets,
    # but still keep the chain reproducible: write if absent)
    app_id = _sm_access(cfg, cfg["app_id_secret"]).decode().strip()
    inst_id = _sm_access(cfg, cfg["installation_id_secret"]).decode().strip()
    cfg["app_id"] = app_id
    cfg["installation_id"] = inst_id
    cfg["resolver_file"] = os.path.abspath(__file__)
    cfg["node_id"] = os.uname().nodename if hasattr(os, "uname") else os.environ.get("COMPUTERNAME", "unknown")
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    print(f"hydrated key bytes={len(data)} crc32={binascii.crc32(data) & 0xffffffff:08x} "
          f"app_id_len={len(app_id)} installation_id_len={len(inst_id)}")
    return 0


def cmd_mint_test() -> int:
    cfg = load_config()
    token, expires = mint_token(cfg)
    print(f"minted ok token_len={len(token)} expires_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(expires))}")
    return 0


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    cmd = args[0]
    if cmd == "get":
        return cmd_get("--json" in args, "--force" in args)
    if cmd == "status":
        return cmd_status()
    if cmd == "hydrate-key":
        return cmd_hydrate_key(force="--if-missing" not in args)
    if cmd == "mint-test":
        return cmd_mint_test()
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
