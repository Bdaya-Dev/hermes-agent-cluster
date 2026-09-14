"""Metering API router — /api/v1/metering/alibaba

Read surface for the Alibaba Token Plan metering poller
(hermes_cluster/core/metering.py). The poller runs in this same process as
the intake poller; this router exposes its last sample.

  GET  /api/v1/metering/alibaba        — last sample: per seat
       total/surplus/cycle_end, fetched_at, last_error, alert state,
       enabled/interval/alert_below (the metering section, post-seed).
  POST /api/v1/metering/alibaba/poll   — force one cycle now (operator/test
       path, mirrors POST /api/v1/intake/gitlab/poll).

No secrets: the payload is totals + seat IDs only; credential material never
enters the response or the poller's status map. Peer-auth applies like every
non-public /api/v1 surface (auth_middleware deny-by-default) — lanes and the
Telegram bot talk to the main through the plugin, which signs.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/metering", tags=["metering"])

# Set by app.py init (same pattern as the other routers' module globals).
_poller = None


def set_poller(poller) -> None:
    global _poller
    _poller = poller


@router.get("/alibaba")
async def alibaba_metering() -> Dict[str, Any]:
    """Current metering summary as stored by the last poll cycle."""
    if _poller is None:
        raise HTTPException(status_code=503,
                            detail="metering poller not initialized on this node")
    return _poller.status()


@router.post("/alibaba/poll")
async def alibaba_metering_poll() -> Dict[str, Any]:
    """Force one poll cycle immediately (does not disturb the interval loop).

    poll_once is synchronous by design (it owns its own fetch loop); run it
    in a worker thread so the app's event loop never blocks on Alibaba/SM
    latency.
    """
    if _poller is None:
        raise HTTPException(status_code=503,
                            detail="metering poller not initialized on this node")
    import asyncio
    result = await asyncio.to_thread(_poller.poll_once)
    return {"result": result, "status": _poller.status()}
