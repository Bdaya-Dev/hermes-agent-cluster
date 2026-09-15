"""Release-drift API router — /api/v1/release/drift (shared/claude-plugins#917).

Read surface + manual trigger for the release-drift poller
(core/release_drift_poller.py), mirroring routers/metering.py exactly:
peer-auth applies like every non-public endpoint (the alarm state carries no
secrets; nothing here writes to any system).

  GET  /api/v1/release/drift        — poller status + last sample
  POST /api/v1/release/drift/check  — force one cycle now (operator/test path,
                                      mirrors POST /api/v1/intake/gitlab/poll)

This surface READS drift only. It cannot move a pin: the release stays a
reviewed GitOps PR against bdaya-website-infra, merged by a human or reviewer
lane (#917 hard constraint — detection must never become a deploy path).
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/release", tags=["release"])

# Set by app.py init (same pattern as the other routers' module globals).
_poller = None


def set_poller(poller) -> None:
    global _poller
    _poller = poller


@router.get("/drift")
async def release_drift() -> Dict[str, Any]:
    """Current release-drift status as stored by the last poll cycle."""
    if _poller is None:
        raise HTTPException(status_code=503,
                            detail="release drift poller not initialized "
                                   "on this node")
    return _poller.status()


@router.post("/drift/check")
async def release_drift_check() -> Dict[str, Any]:
    """Force one drift check immediately (does not disturb the interval loop).

    poll_once is synchronous by design (it owns its own fetch loop); run it
    in a worker thread so the app's event loop never blocks on GitHub/registry
    latency.
    """
    if _poller is None:
        raise HTTPException(status_code=503,
                            detail="release drift poller not initialized "
                                   "on this node")
    import asyncio
    result = await asyncio.to_thread(_poller.poll_once)
    return {"result": result, "status": _poller.status()}
