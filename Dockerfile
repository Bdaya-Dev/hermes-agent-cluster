# =============================================================================
# Hermes cluster — containerized main node (shared/claude-plugins#856)
# =============================================================================
# Runs the exact entrypoint the desktop runs today:
#
#   python -m hermes_cluster.serve --node-role main ...
#
# Desktop today (run-main-authed.cmd): uvicorn(FastAPI) on :8787, fed by
# cluster.yaml + flags. The container starts the same app through the same
# serve.py CLI; only the host changes.
#
# This image is built by CI (.github/workflows/cluster-image.yml) and pushed
# to Artifact Registry on bdaya-website. NO SECRETS ARE BAKED IN:
#   - peer auth tokens arrive as env (PEER_TOKEN / PEER_TOKENS — read from
#     the environment by hermes_cluster/app.py), synced by External Secrets
#     from GCP Secret Manager;
#   - the Postgres DSN — once GKE-1's ClusterStore lands — arrives as
#     HERMES_CLUSTER_PG_DSN through External Secrets. serve.py rejects a
#     literal DSN in config unconditionally.
# asyncpg is installed here deliberately AHEAD of GKE-1 so that flipping
# store.backend: postgres is a pure config change (cluster.yaml) requiring no
# Dockerfile rebuild decision — see the MR's migration shape section.
# =============================================================================

# Digest-pinned base (pinned base, per #856 hard constraint): python:3.12-slim
# resolved from Docker Hub 2026-09-11. Bump by re-resolving
# `docker manifest inspect python:3.12-slim`, never by moving a floating tag.
FROM python:3.12-slim@sha256:2fe5997d249a808b8eeea52c58a1dbffbba28754dc11699ef5c029f2d818ce79

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Package + metadata only (see .dockerignore — tests, dashboards, db files and
# every dev script stay out of the build context).
COPY pyproject.toml README.md ./
COPY hermes_cluster ./hermes_cluster

# Runtime deps from pyproject (fastapi, uvicorn[standard], pydantic, httpx)
# + pyyaml (cluster.yaml parsing — installed the same way the repo CI does)
# + asyncpg (config-gated Postgres store, see header comment).
RUN pip install --upgrade pip \
 && pip install -e . pyyaml asyncpg

# Non-root. /data holds the SQLite store file (mounted PVC); the app tree is
# owned by root and only read at runtime.
RUN groupadd --gid 10001 hermes \
 && useradd --uid 10001 --gid hermes --shell /usr/sbin/nologin hermes \
 && mkdir -p /data && chown hermes:hermes /data \
 && rm -rf /root/.cache
USER hermes:hermes

# Writable HOME for the runtime user (Python caches, temp files) — /data is
# the mounted volume; nothing secret lands there.
ENV HOME=/data

EXPOSE 8787

# Health: the app serves GET /health (hermes_cluster/app.py). stdlib urllib —
# no curl/wget added to the runtime image.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8787/health', timeout=4).status==200 else 1)"

# The desktop entrypoint, exec-form. Flags here mirror run-main-authed.cmd;
# cluster-id/node-id/config are supplied by the Kubernetes Deployment args so
# one image serves any main-node config. Static dashboard ships in the wheel
# tree at /app/hermes_cluster/static.
#
# IMAGE CONTRACT (F1, reviewer infra PR#261): this image deliberately sets
# CMD only and NO ENTRYPOINT — `docker run <image>` and the desktop behave
# identically. Consequence for orchestrators: Kubernetes REPLACES CMD with
# container.args (it does not append when command is unset), so any Pod spec
# overriding args MUST also set `command: ["python"]` — as
# infra-github cluster-config/hermes-main/deployment.yaml now does. Consumers
# that prepend their own program (e.g. `docker run --entrypoint python <img>
# -m hermes_cluster.serve --help`) are the proven-safe form.
CMD ["python", "-m", "hermes_cluster.serve", \
     "--node-role", "main", \
     "--host", "0.0.0.0", \
     "--port", "8787", \
     "--static-dir", "/app/hermes_cluster/static"]
