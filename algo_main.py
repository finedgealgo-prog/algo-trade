"""
Algo-only entrypoint for the split backend.

`api.py` here is a trimmed copy of the original monolith: every
simulator/scanner/signal_builder import, router mount, and @sim_router
endpoint has been removed from the source itself (not just hidden), and the
simulator/scanner/signal_builder/signals/common packages are not present in
this folder at all. What's left only serves the "Algo Trade" frontend menu
(Forward Test, Live Trade, Backtest, Broker Login, Live Option Chain, MTM
Graph, Group MTM Graph) plus its supporting websockets.

Central-tick mode
─────────────────
This process does NOT open its own broker WebSocket connection. Instead it
connects to algo.websocket (/ws/internal-ticks) via CentralTickClient, which
keeps a local ltp_map updated in-process (sub-ms dict read for SL/TP checks)
and calls live_tick_dispatcher.dispatch_tick() on every tick — exactly as the
real dhan_ticker would. algo.trade gets first priority in this process:
  tick arrives → ltp_map updated → dispatch_tick() → SL/TP worker
  then listeners are called (display/monitoring).

Run (from /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.trade):
    uvicorn algo_main:app --reload --port 8000
"""

import asyncio
import logging
import threading

import requests

from api import app  # noqa: E402

log = logging.getLogger(__name__)

# Skip: alert_checker (chart domain, not algo trade)
# Skip: _auto_start_ticker (this service uses central-tick mode — algo.websocket
#        owns the ONE broker WS connection; we start CentralTickClient below)
SKIP_STARTUP_FUNCS = {"_auto_start_alert_checker", "_auto_start_ticker"}

_DROP_ROUTE_PREFIXES = ("/ws/live-quotes", "/live-quotes")

app.router.on_startup = [
    f for f in app.router.on_startup
    if f.__name__ not in SKIP_STARTUP_FUNCS
]
app.router.routes = [
    r for r in app.router.routes
    if not str(getattr(r, "path", "")).startswith(_DROP_ROUTE_PREFIXES)
]


# ── Central tick client startup ───────────────────────────────────────────────

def _start_central_ticker() -> None:
    """
    Connect to algo.websocket's /ws/internal-ticks.
    Retried inside CentralTickClient automatically on disconnect.
    """
    from features.central_tick_client import CentralTickClient
    from features.broker_gateway import broker_ticker_manager
    from features.mongo_data import MongoData

    client = CentralTickClient("http://localhost:8003")
    broker_ticker_manager.set_central_client(client)

    db = MongoData()
    try:
        client.start(db._db)
    finally:
        db.close()

    log.info("[algo_main] CentralTickClient started — broker WS owned by algo.websocket")


async def _wait_for_websocket_ready(
    url: str = "http://localhost:8003/health",
    max_wait: float = 30.0,
    poll_interval: float = 0.3,
) -> bool:
    """
    Poll algo.websocket's /health instead of a blind fixed sleep — connects
    the moment it's actually ready (often well under 3s) instead of always
    waiting the full guess window, and keeps polling past 3s if it's slow
    (Mongo/broker-token validation) instead of firing one connect attempt
    that fails and falls into CentralTickClient's own (slower) backoff.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + max_wait
    while loop.time() < deadline:
        try:
            resp = await asyncio.to_thread(requests.get, url, timeout=1.0)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return True
        except Exception:
            pass
        await asyncio.sleep(poll_interval)
    return False


@app.on_event("startup")
async def _auto_start_central_ticker() -> None:
    async def _bg() -> None:
        ready = await _wait_for_websocket_ready()
        if ready:
            log.info("[algo_main] algo.websocket /health ok — starting CentralTickClient")
        else:
            log.warning(
                "[algo_main] algo.websocket not ready after 30s — starting "
                "CentralTickClient anyway (it retries internally on disconnect)"
            )
        try:
            threading.Thread(target=_start_central_ticker, daemon=True).start()
        except Exception:
            log.exception("[STARTUP] CentralTickClient start failed")
    asyncio.create_task(_bg())


# ── Chart domain (TradingView chart-state/alerts/symbol search+history) ──────
# chart_api.py is symlinked in from ../shared/chart_api.py, same mount as
# algo.scanner and algo.simulator — its data layer (features/chart_data.py)
# only depends on shared/features/, so it's importable from any service.
# Deliberately NOT calling start_chart_background_loops() here: that starts
# the price/trendline + indicator alert-checker polling loop, which
# algo.scanner's process already runs — running it again here would
# double-evaluate every alert and fire each webhook twice. This mount is
# REST-only (chart-state/alerts CRUD, symbol_search, symbol_historical_chart).
from chart_api import router as chart_router  # noqa: E402

app.include_router(chart_router)
