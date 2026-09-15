#!/usr/bin/env python3
"""Capability probes — prove a capability before the node advertises it (#907).

``worker_connector`` declares a probe-gated capability ONLY while its probe
command exits 0 (#867). This script is the registry of those commands, so
adding a capability to the fleet is a config line naming it here rather than a
bespoke shell command pasted into three node YAMLs — which is how the fleet
ended up with no vocabulary at all and lanes inventing names (`merge`, `land`,
`flutter`, `invora`) for needs nobody had written down.

    python scripts/capability_probe.py flutter     # exit 0 = advertise it

Every probe answers "can a lane on THIS machine actually do the thing", not
"is a binary on PATH". A binary that exists but cannot authenticate produces
exactly the failure probes exist to prevent: a task dispatched to a node that
accepts it and then fails mid-lane, which is worse than never being dispatched
because the lane has already burned its budget.

``--list`` prints the registry; ``--all`` runs every probe and reports each,
which is what you want when bringing a new node into the fleet.
"""

import argparse
import hashlib
import shutil
import subprocess
import sys

# Probe timeout. Generous: an auth-touching probe makes a network call, and a
# probe that times out is read as "capability absent", so a too-tight bound
# silently removes a node from the fleet rather than erroring.
TIMEOUT_S = 90


def _run(cmd, timeout=TIMEOUT_S, redact=False):
    """True iff *cmd* exits 0. A missing binary is a clean False, not a crash.

    ``redact=True`` for any probe whose SUCCESS output is a credential. Those
    probes succeed by minting a token, and the obvious implementation — echo
    the first line of stdout as evidence — writes a live bearer token into the
    worker log, the probe's own console, and every transcript that captured
    it. Measured 2026-09-15 while writing this file: the first run of
    ``--all`` printed a full GCP access token. The rule the estate already
    had ("never print a secret value, assert a length or checksum") is
    enforced here in code so a future probe cannot re-learn it the same way.

    Failure output is never redacted: a failing credential command prints an
    error, not a credential, and that error is the whole diagnostic value.
    """
    exe = shutil.which(cmd[0])
    if not exe:
        return False, f"{cmd[0]}: not on PATH"
    try:
        proc = subprocess.run([exe, *cmd[1:]], capture_output=True, text=True,
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"{cmd[0]}: timed out after {timeout}s"
    except Exception as e:  # pragma: no cover - defensive
        return False, f"{cmd[0]}: {e}"
    if proc.returncode == 0:
        out = (proc.stdout or "").strip()
        if redact:
            return True, f"ok ({len(out)} bytes, sha256:{hashlib.sha256(out.encode()).hexdigest()[:12]})"
        first = out.splitlines()[:1]
        return True, (first[0] if first else "ok")
    tail = ((proc.stderr or proc.stdout or "").strip() or "(no output)")[-200:]
    return False, f"{cmd[0]}: rc={proc.returncode} {tail}"


def probe_flutter():
    """A lane can build/test Flutter here.

    `flutter --version` and not `flutter doctor`: doctor is slow (tens of
    seconds) and reports non-zero for unrelated toolchain gaps such as a
    missing Android licence, which would drop the node from web/desktop work
    it can do perfectly well.
    """
    return _run(["flutter", "--version"])


def probe_dotnet():
    """A lane can build/test .NET here."""
    return _run(["dotnet", "--version"])


def probe_gcp():
    """A lane can reach GCP as this machine.

    Deliberately mints an ADC token rather than reading `gcloud auth list`:
    the CLI credential store and Application Default Credentials are separate,
    and every tool in this estate that touches GCP uses ADC. A node with a
    logged-in CLI and dead ADC would advertise a capability nothing can use.
    """
    return _run(["gcloud", "auth", "application-default", "print-access-token"],
                redact=True)


def probe_k8s():
    """A lane can talk to the cluster's API server as this machine.

    `auth can-i` exits non-zero when the answer is "no", so a rc=0 here means
    both reachable AND authorized — which is the thing a lane needs.
    """
    return _run(["kubectl", "auth", "can-i", "get", "pods", "--all-namespaces"])


PROBES = {
    "flutter": probe_flutter,
    "dotnet": probe_dotnet,
    "gcp": probe_gcp,
    "k8s": probe_k8s,
}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("capability", nargs="?", help="capability to probe")
    ap.add_argument("--list", action="store_true", help="print the registry")
    ap.add_argument("--all", action="store_true", help="run every probe")
    args = ap.parse_args(argv)

    if args.list:
        for name in sorted(PROBES):
            print(name)
        return 0

    if args.all:
        worst = 0
        for name in sorted(PROBES):
            ok, detail = PROBES[name]()
            print(f"{'PASS' if ok else 'FAIL'}  {name:10} {detail}")
            if not ok:
                worst = 1
        return worst

    if not args.capability:
        ap.error("give a capability, --list or --all")

    fn = PROBES.get(args.capability)
    if fn is None:
        # An UNKNOWN capability fails closed and says so. Failing open would
        # let a typo advertise a capability the node cannot serve, recreating
        # the forever-queue this whole mechanism exists to prevent.
        print(f"unknown capability {args.capability!r}; known: "
              f"{', '.join(sorted(PROBES))}", file=sys.stderr)
        return 2

    ok, detail = fn()
    print(f"{'PASS' if ok else 'FAIL'}  {args.capability}: {detail}",
          file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
