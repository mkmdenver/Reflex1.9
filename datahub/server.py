# datahub/server.py
# FastAPI DataHub: local-first, then backfill-from-Polygon, then requery from DB
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, date
from typing import List
import pandas as pd

from common.app_logging import setup_logger
from common.utils import to_utc_datetime  # if you have one; otherwise inline parse
from common.db_access import (
    get_daily_bars_db, get_minute_bars_db, get_ticks_db,
    insert_daily_bars_db, insert_minute_bars_db, insert_ticks_db
)
from polygon_api.rest import (
    fetch_daily_bars, fetch_minute_bars, fetch_ticks
)

log = setup_logger("datahub.api", level="INFO")
app = FastAPI(title="Reflex DataHub", version="1.0")

def _ensure_daily(symbol: str, day: date) -> pd.DataFrame:
    start = datetime.combine(day, datetime.min.time())
    end   = datetime.combine(day, datetime.max.time())
    df = get_daily_bars_db(symbol, start, end)
    if not df.empty:
        return df
    live = fetch_daily_bars(symbol, day.strftime("%Y-%m-%d"))
    if live is not None and not live.empty:
        insert_daily_bars_db(symbol, live)
    return get_daily_bars_db(symbol, start, end)

def _ensure_minute(symbol: str, day: date) -> pd.DataFrame:
    start = datetime.combine(day, datetime.min.time())
    end   = datetime.combine(day, datetime.max.time())
    df = get_minute_bars_db(symbol, start, end)
    if not df.empty:
        return df
    live = fetch_minute_bars(symbol, day.strftime("%Y-%m-%d"))
    if not live.empty:
        insert_minute_bars_db(live.assign(symbol=symbol))
    return get_minute_bars_db(symbol, start, end)

def _ensure_ticks(symbol: str, day: date) -> pd.DataFrame:
    start = datetime.combine(day, datetime.min.time())
    end   = datetime.combine(day, datetime.max.time())
    df = get_ticks_db(symbol, start, end)
    if not df.empty:
        return df
    live = fetch_ticks(symbol, day.strftime("%Y-%m-%d"))
    if not live.empty:
        # reshape to match your insert schema if needed
        live = live.assign(symbol=symbol)
        insert_ticks_db(symbol, live)
    return get_ticks_db(symbol, start, end)

@app.get("/v1/daily")
def daily(symbol: str, day: str):
    try:
        day_dt = datetime.strptime(day, "%Y-%m-%d").date()
        df = _ensure_daily(symbol, day_dt)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        log.exception("daily error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/minute")
def minute(symbol: str, day: str):
    try:
        day_dt = datetime.strptime(day, "%Y-%m-%d").date()
        df = _ensure_minute(symbol, day_dt)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        log.exception("minute error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/ticks")
def ticks(symbol: str, day: str):
    try:
        day_dt = datetime.strptime(day, "%Y-%m-%d").date()
        df = _ensure_ticks(symbol, day_dt)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        log.exception("ticks error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# Quotes: memory-only policy — served elsewhere (e.g., cockpit) from live buffers.
@app.get("/v1/quotes")
def quotes(symbol: str, limit: int = 100):
    try:
        from shared_mem.registry import registry
        from shared_mem.buffers import symbol_buffers
        if symbol not in registry:
            raise HTTPException(status_code=404, detail="Symbol not found")
        buffer = symbol_buffers.get(symbol, {}).get("quotes", [])
        recent = buffer[-limit:] if limit > 0 else buffer
        return JSONResponse(content=recent)
    except Exception as e:
        log.exception("quotes error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))     