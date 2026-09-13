"""FastAPI middleware that enforces peer-token auth on cross-node endpoints.

Deny-by-default when peer auth is enabled (M2 fix): only an explicit PUBLIC
set is accessible without authentication. Everything else requires a valid
peer-token HMAC-SHA256 signature.

Public paths (no auth required):
  - /health
  - /metrics
  - /dashboard/* (static files)
  - /docs, /redoc, /openapi.json (OpenAPI UI)
"""

from __future__ import annotations

import logging
from typing import Callable, FrozenSet

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from .core.peer_auth import PeerAuthState

logger = logging.getLogger("hermes_cluster.auth_middleware")

# M2: deny-by-default — only these path prefixes/exact paths are public.
# Everything else requires peer auth when enabled.
PUBLIC_PATHS: FrozenSet[str] = frozenset({
    "/health",
    "/metrics",
    "/docs",
    "/redoc",
    "/openapi.json",
    # GitLab webhook — has its own X-Gitlab-Token auth (PR#2).
    # GitLab (external) can never sign peer-HMAC, so this MUST be public.
    "/api/v1/intake/gitlab/webhook",
    # Runtime intake POLICY read/write (hermes-factory-intake): the business
    # team's 2am control surface. Peer-HMAC signing is impossible from a
    # browser/plain curl, which would force the config through env-var/
    # ConfigMap redeploys — exactly what the requirement forbids. Same auth
    # contract as the webhook: the handlers enforce X-Gitlab-Token when
    # GITLAB_INTAKE_WEBHOOK_SECRET is set (and log a warning at boot when it
    # is not). Method-restricted to GET/PUT by the route itself.
    "/api/v1/intake/gitlab/policy",
})

PUBLIC_PREFIXES: tuple = (
    "/dashboard/",
    "/dashboard/static/",
)


def _is_public(path: str) -> bool:
    """Check if a path is in the public allowlist."""
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES)


class PeerAuthMiddleware(BaseHTTPMiddleware):
    """Verify peer-token HMAC-SHA256 signature on all non-public endpoints.

    Deny-by-default: when enabled, only PUBLIC_PATHS/PUBLIC_PREFIXES pass through.
    All other paths require a valid peer auth signature.

    H2 fix: takes a PeerAuthState instance — not module globals — so two apps
    in one process have isolated trust domains.
    """

    def __init__(self, app, state: PeerAuthState = None, enabled: bool = False):
        super().__init__(app)
        self.enabled = enabled
        # H2: bind state to this middleware instance, not module globals
        self.state = state

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        if not self.enabled:
            return await call_next(request)

        # M2: deny-by-default — public paths pass through, everything else gated
        if _is_public(path):
            return await call_next(request)

        # Read body for signature verification
        body = await request.body()

        # M1: include query string in the path for verification
        if request.url.query:
            signed_path = f"{path}?{request.url.query}"
        else:
            signed_path = path

        # Extract headers
        headers = {
            "X-Peer-Node": request.headers.get("X-Peer-Node", ""),
            "X-Peer-Timestamp": request.headers.get("X-Peer-Timestamp", ""),
            "X-Peer-Signature": request.headers.get("X-Peer-Signature", ""),
        }

        # H2: use per-instance state, not module globals
        state = self.state
        if state is None:
            # Fallback to module default for backward compat
            from .core import peer_auth
            state = peer_auth.get_default_state()

        ok, err = state.verify_request(
            method=request.method,
            path=signed_path,
            body=body,
            headers=headers,
        )

        if not ok:
            logger.warning("Peer auth failed for %s %s: %s", request.method, path, err)
            return JSONResponse(
                status_code=401,
                content={"error": "peer_auth_failed", "detail": "peer_auth_failed"},
            )

        # L3 fix: removed dead request._receive assignment.
        # Body replay works through Starlette's _CachedRequest — proved by
        # signed POSTs reaching handlers (tasks created, sync applied).
        return await call_next(request)
