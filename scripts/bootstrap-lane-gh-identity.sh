#!/bin/bash
# bootstrap-lane-gh-identity.sh — per-node one-call provisioning of the
# bdaya-lane-agent GitHub identity (shared/claude-plugins#867), idempotent.
#
# The estate's existing credential-sync pattern is ADC -> GCP Secret Manager
# (run-worker-macbook.sh, install-worker-profile.sh — both hydrate from SM at
# launch and never print values). This script is that same pattern for the
# GitHub App identity: hydrate-key pulls the App private key + ids from SM and
# writes them node-local, then status proves it. Safe to re-run; safe at boot.
#
# Usage: bootstrap-lane-gh-identity.sh [path-to-lane_gh_token.py]
# Prereq: ADC with secretmanager access on bdaya-website (same as the alibaba key).
set -euo pipefail
RESOLVER="${1:-$(dirname "$0")/lane_gh_token.py}"
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null || PY=python
"$PY" "$RESOLVER" hydrate-key --if-missing
"$PY" "$RESOLVER" status
