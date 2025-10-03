# common/poly_tools.py
from __future__ import annotations
from datetime import datetime, date
from typing import Iterable, Optional
from .config import POLY_PARAMS
from .app_logging import get_logger

log = get_logger("Polygon")

# This module intentionally delegates to your custom polygon_api/* interface
# so the rest of the system never uses the SDK directly.
# We lazy-import to avoid circulars and keep optional deps out of import time.

def _rest():
    try:
        # your custom interface (already part of the codebase)
        from polygon_api import rest  # noqa: WPS433
        return rest
    except Exception as ex:
        raise ImportError(
            "polygon_api.rest module is required but not importable."
        ) from ex

def require_api_key():
    if not POLY_PARAMS.api_key:
        raise RuntimeError(
            "POLYGON_API_KEY not set. Populate environment or .env."
        )

def fetch_daily_bars(symbol: str, start: date, end: date):
    """Return iterable of daily bars as dicts."""
    require_api_key()
    rest = _rest()
    # Expect your rest module to expose an explicit function; try common names:
    if hasattr(rest, "fetch_daily_bars"):
        return rest.fetch_daily_bars(symbol, start, end, api_key=POLY_PARAMS.api_key)
    if hasattr(rest, "get_daily_bars"):
        return rest.get_daily_bars(symbol, start, end, api_key=POLY_PARAMS.api_key)
    raise AttributeError("polygon_api.rest must expose fetch/get_daily_bars")

def fetch_minute_bars(symbol: str, start: datetime, end: datetime, timespan: str = "minute"):
    require_api_key()
    rest = _rest()
    fn = getattr(rest, "fetch_minute_bars", None) or getattr(rest, "get_minute_bars", None)
    if not fn:
        raise AttributeError("polygon_api.rest must expose fetch/get_minute_bars")
    return fn(symbol, start, end, api_key=POLY_PARAMS.api_key, timespan=timespan)

def fetch_ticks(symbol: str, start: datetime, end: datetime):
    require_api_key()
    rest = _rest()
    fn = getattr(rest, "fetch_ticks", None) or getattr(rest, "get_ticks", None)
    if not fn:
        raise AttributeError("polygon_api.rest must expose fetch/get_ticks")
    return fn(symbol, start, end, api_key=POLY_PARAMS.api_key)
