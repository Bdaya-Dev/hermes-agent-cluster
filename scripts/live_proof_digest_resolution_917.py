"""Live digest-resolution proof (#917 acceptance: RESOLVED from Artifact
Registry). Prints byte lengths / digests (registry content hashes are public
integrity metadata, not secrets) — never any token value."""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hermes_cluster.core.release_drift import (DriftConfig,
                                               resolve_release_digests,
                                               build_digest_url)

# ADC token via gcloud (the same resolution chain the pod's metadata-server
# path mirrors); byte length asserted, value never printed or written.
out = subprocess.run(
    ["gcloud", "auth", "application-default", "print-access-token", "--quiet"],
    capture_output=True, text=True, timeout=60)
tok = out.stdout.strip()
print(f"ADC token minted: {len(tok)} bytes (value never printed)")
if len(tok) < 20:
    print("stderr:", out.stderr[-300:])
    sys.exit(1)

cfg = DriftConfig()
PIN = "e6b58f9715b9f4dd90e7f1210975463ae12bdc2a"   # deployed (train 6)
MAIN = "e22ad09288aa44c2ef1949cea81275ccf2121ec4"  # fork main head, live

# 1) anonymous must 401 (proves the endpoint is private -> auth matters)
try:
    req = urllib.request.Request(build_digest_url(cfg, PIN),
                                 headers={"Accept": "*/*"}, method="HEAD")
    with urllib.request.urlopen(req, timeout=15) as r:
        print("anonymous HEAD ->", r.status)
except urllib.error.HTTPError as e:
    print("anonymous HEAD ->", e.code, "(private endpoint confirmed)")

# 2) authenticated resolves real digests for BOTH tags
res = resolve_release_digests(cfg, PIN, MAIN, adc_token=tok)
print("pin  digest:", res["pin_digest"], res["pin_error"] or "")
print("head digest:", res["main_digest"], res["main_error"] or "")
# 3) train-6's recorded digest was 5b26…? compare against the manifest claim
print("train6 comment claims: sha256:d371728ad56d42090af0b9a322f48824b5cf8672e582a04893f42e4396f94428")
print("MATCHES live resolution" if res["pin_digest"] ==
      "sha256:d371728ad56d42090af0b9a322f48824b5cf8672e582a04893f42e4396f94428"
      else "MISMATCH — flag it")
