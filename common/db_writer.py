# common/db_writer.py
"""
High-level DB writers for Reflex using psycopg v3.

- Depends on common.dbutils (psycopg v3) and its bulk_upsert().
- Provides production-grade upsert helpers for daily, minute, tick, and quote data.
- Accepts Polygon-style rows or DB-style rows; maps to DB columns.
- Adapts to schema via environment variables (no code edits for naming differences).

Environment overrides (optional):
  # tables
  REFLEX_TABLE_DAILY            default "daily"
  REFLEX_TABLE_MINUTE           default "minute"
  REFLEX_TABLE_TICK             default "tick"
  REFLEX_TABLE_QUOTE            default "quote"

  # shared
  REFLEX_COL_SYMBOL             default "symbol"

  # daily bars
  REFLEX_COL_DAILY_TS           default "day"         (DATE)
  REFLEX_COL_OPEN               default "open"
  REFLEX_COL_HIGH               default "high"
  REFLEX_COL_LOW                default "low"
  REFLEX_COL_CLOSE              default "close"
  REFLEX_COL_VOLUME             default "volume"
  REFLEX_COL_VWAP               default "vwap"

  # minute bars
  REFLEX_COL_MINUTE_TS          default "ts"          (TIMESTAMPTZ)

  # ticks
  REFLEX_COL_TICK_TS            default "ts"          (TIMESTAMPTZ)
  REFLEX_COL_PRICE              default "price"
  REFLEX_COL_SIZE               default "size"
  REFLEX_COL_EXCHANGE           default "exchange"
  REFLEX_COL_CONDITIONS         default "conditions"  (array/json/text acceptable)

  # quotes
  REFLEX_COL_QUOTE_TS           default "ts"          (TIMESTAMPTZ)
  REFLEX_COL_BID_PRICE          default "bid_price"
  REFLEX_COL_BID_SIZE           default "bid_size"
  REFLEX_COL_ASK_PRICE          default "ask_price"
  REFLEX_COL_ASK_SIZE           default "ask_size"
  REFLEX_COL_BID_EXCHANGE       default "bid_exchange"
  REFLEX_COL_ASK_EXCHANGE       default "ask_exchange"
  REFLEX_COL_QUOTE_CONDITIONS   default "conditions"  (array/json/text acceptable)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .dbutils import bulk_upsert

# --------------------------------------------------------------------------------------
# Table / Column config with safe defaults to match hypertable names:
# daily, minute, tick, quote (you can override with env vars)
# --------------------------------------------------------------------------------------
TABLE_DAILY  = os.getenv("REFLEX_TABLE_DAILY",  "daily")
TABLE_MINUTE = os.getenv("REFLEX_TABLE_MINUTE", "minute")
TABLE_TICK   = os.getenv("REFLEX_TABLE_TICK",   "tick")
TABLE_QUOTE  = os.getenv("REFLEX_TABLE_QUOTE",  "quote")

COL_SYMBOL   = os.getenv("REFLEX_COL_SYMBOL",   "symbol")

# Bars: date & timestamp
COL_DAILY_TS  = os.getenv("REFLEX_COL_DAILY_TS",  "day")   # DATE
COL_MINUTE_TS = os.getenv("REFLEX_COL_MINUTE_TS", "ts")    # TIMESTAMPTZ

# OHLCV columns
COL_OPEN   = os.getenv("REFLEX_COL_OPEN",   "open")
COL_HIGH   = os.getenv("REFLEX_COL_HIGH",   "high")
COL_LOW    = os.getenv("REFLEX_COL_LOW",    "low")
COL_CLOSE  = os.getenv("REFLEX_COL_CLOSE",  "close")
COL_VOLUME = os.getenv("REFLEX_COL_VOLUME", "volume")
COL_VWAP   = os.getenv("REFLEX_COL_VWAP",   "vwap")

# Tick columns
COL_TICK_TS   = os.getenv("REFLEX_COL_TICK_TS",   "ts")
COL_PRICE     = os.getenv("REFLEX_COL_PRICE",     "price")
COL_SIZE      = os.getenv("REFLEX_COL_SIZE",      "size")
COL_EXCHANGE  = os.getenv("REFLEX_COL_EXCHANGE",  "exchange")
COL_TK_COND   = os.getenv("REFLEX_COL_CONDITIONS","conditions")

# Quote columns
COL_QUOTE_TS  = os.getenv("REFLEX_COL_QUOTE_TS",  "ts")
COL_BID_PRICE = os.getenv("REFLEX_COL_BID_PRICE", "bid_price")
COL_BID_SIZE  = os.getenv("REFLEX_COL_BID_SIZE",  "bid_size")
COL_ASK_PRICE = os.getenv("REFLEX_COL_ASK_PRICE", "ask_price")
COL_ASK_SIZE  = os.getenv("REFLEX_COL_ASK_SIZE",  "ask_size")
COL_BID_EXCH  = os.getenv("REFLEX_COL_BID_EXCHANGE", "bid_exchange")
COL_ASK_EXCH  = os.getenv("REFLEX_COL_ASK_EXCHANGE", "ask_exchange")
COL_QT_COND   = os.getenv("REFLEX_COL_QUOTE_CONDITIONS","conditions")


# --------------------------------------------------------------------------------------
# Time helpers
# --------------------------------------------------------------------------------------
def _epoch_to_dt_utc(t: int | float) -> datetime:
    """
    Convert epoch seconds/milliseconds/nanoseconds to UTC datetime.
    """
    if t is None:
        raise ValueError("Missing epoch timestamp value")
    if t > 1e14:    # ns
        seconds = t / 1e9
    elif t > 1e11:  # ms
        seconds = t / 1e3
    else:           # s
        seconds = t
    return datetime.fromtimestamp(seconds, tz=timezone.utc)

def _minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


# --------------------------------------------------------------------------------------
# Mapping helpers (accept Polygon-style or DB-native rows)
# --------------------------------------------------------------------------------------
def _map_daily_row(row: Mapping[str, Any], *, symbol: str) -> Dict[str, Any]:
    o = row.get(COL_OPEN,  row.get("o"))
    h = row.get(COL_HIGH,  row.get("h"))
    l = row.get(COL_LOW,   row.get("l"))
    c = row.get(COL_CLOSE, row.get("c"))
    v = row.get(COL_VOLUME,row.get("v"))
    vw = row.get(COL_VWAP, row.get("vw", row.get("vwap")))
    if None in (o, h, l, c, v):
        missing = [k for k, v_ in [(COL_OPEN,o),(COL_HIGH,h),(COL_LOW,l),(COL_CLOSE,c),(COL_VOLUME,v)] if v_ is None]
        raise ValueError(f"Daily bar row missing fields: {missing}")

    if COL_DAILY_TS in row:
        day_val = row[COL_DAILY_TS]
    elif "day" in row:
        day_val = row["day"]
    elif "t" in row:
        day_val = _epoch_to_dt_utc(row["t"]).date()
    else:
        raise ValueError(f"Daily bar row missing {COL_DAILY_TS}/'day' or epoch 't'")

    out = {
        COL_SYMBOL: symbol,
        COL_DAILY_TS: day_val,
        COL_OPEN: o, COL_HIGH: h, COL_LOW: l, COL_CLOSE: c, COL_VOLUME: v
    }
    if vw is not None:
        out[COL_VWAP] = vw
    return out


def _map_minute_row(row: Mapping[str, Any], *, symbol: str) -> Dict[str, Any]:
    o = row.get(COL_OPEN,  row.get("o"))
    h = row.get(COL_HIGH,  row.get("h"))
    l = row.get(COL_LOW,   row.get("l"))
    c = row.get(COL_CLOSE, row.get("c"))
    v = row.get(COL_VOLUME,row.get("v"))
    vw = row.get(COL_VWAP, row.get("vw", row.get("vwap")))
    if None in (o, h, l, c, v):
        missing = [k for k, v_ in [(COL_OPEN,o),(COL_HIGH,h),(COL_LOW,l),(COL_CLOSE,c),(COL_VOLUME,v)] if v_ is None]
        raise ValueError(f"Minute bar row missing fields: {missing}")

    if COL_MINUTE_TS in row:
        ts_val = row[COL_MINUTE_TS]
    elif "ts" in row:
        ts_val = row["ts"]
    elif "t" in row:
        ts_val = _minute_floor(_epoch_to_dt_utc(row["t"]))
    else:
        raise ValueError(f"Minute bar row missing {COL_MINUTE_TS}/'ts' or epoch 't'")

    out = {
        COL_SYMBOL: symbol,
        COL_MINUTE_TS: ts_val,
        COL_OPEN: o, COL_HIGH: h, COL_LOW: l, COL_CLOSE: c, COL_VOLUME: v
    }
    if vw is not None:
        out[COL_VWAP] = vw
    return out


def _map_tick_row(row: Mapping[str, Any], *, symbol: str) -> Dict[str, Any]:
    price = row.get(COL_PRICE, row.get("p"))
    size  = row.get(COL_SIZE,  row.get("s"))
    exch  = row.get(COL_EXCHANGE, row.get("x"))
    cond  = row.get(COL_TK_COND,  row.get("c"))
    if price is None or size is None:
        missing = [k for k, v_ in [(COL_PRICE,price),(COL_SIZE,size)] if v_ is None]
        raise ValueError(f"Tick row missing fields: {missing}")

    if COL_TICK_TS in row:
        ts_val = row[COL_TICK_TS]
    elif "ts" in row:
        ts_val = row["ts"]
    elif "t" in row:
        ts_val = _epoch_to_dt_utc(row["t"])
    else:
        raise ValueError(f"Tick row missing {COL_TICK_TS}/'ts' or epoch 't'")

    out = {
        COL_SYMBOL: symbol,
        COL_TICK_TS: ts_val,
        COL_PRICE: price,
        COL_SIZE: size
    }
    if exch is not None:
        out[COL_EXCHANGE] = exch
    if cond is not None:
        out[COL_TK_COND] = cond
    return out


def _map_quote_row(row: Mapping[str, Any], *, symbol: str) -> Dict[str, Any]:
    bp = row.get(COL_BID_PRICE, row.get("bp"))
    bs = row.get(COL_BID_SIZE,  row.get("bs"))
    ap = row.get(COL_ASK_PRICE, row.get("ap"))
    asz= row.get(COL_ASK_SIZE,  row.get("as"))
    bx = row.get(COL_BID_EXCH,  row.get("bx"))
    ax = row.get(COL_ASK_EXCH,  row.get("ax"))
    cond = row.get(COL_QT_COND, row.get("c"))

    # bid/ask prices and sizes are typically required
    if bp is None or bs is None or ap is None or asz is None:
        missing = [k for k, v_ in [(COL_BID_PRICE,bp),(COL_BID_SIZE,bs),(COL_ASK_PRICE,ap),(COL_ASK_SIZE,asz)] if v_ is None]
        raise ValueError(f"Quote row missing fields: {missing}")

    if COL_QUOTE_TS in row:
        ts_val = row[COL_QUOTE_TS]
    elif "ts" in row:
        ts_val = row["ts"]
    elif "t" in row:
        ts_val = _epoch_to_dt_utc(row["t"])
    else:
        raise ValueError(f"Quote row missing {COL_QUOTE_TS}/'ts' or epoch 't'")

    out = {
        COL_SYMBOL: symbol,
        COL_QUOTE_TS: ts_val,
        COL_BID_PRICE: bp,
        COL_BID_SIZE: bs,
        COL_ASK_PRICE: ap,
        COL_ASK_SIZE: asz,
    }
    if bx is not None:
        out[COL_BID_EXCH] = bx
    if ax is not None:
        out[COL_ASK_EXCH] = ax
    if cond is not None:
        out[COL_QT_COND] = cond
    return out


# --------------------------------------------------------------------------------------
# Normalizers
# --------------------------------------------------------------------------------------
def _normalize(rows: Iterable[Mapping[str, Any]], mapper, *, symbol: str) -> List[Dict[str, Any]]:
    return [mapper(r, symbol=symbol) for r in rows]


# --------------------------------------------------------------------------------------
# Public API used by db_backfill.py (and elsewhere)
# --------------------------------------------------------------------------------------
def upsert_daily_bars_for_symbol(
    symbol: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    dsn: Optional[str] = None,
    update_cols: Optional[Sequence[str]] = None,
    chunk_size: int = 1000,
) -> int:
    norm = _normalize(rows, _map_daily_row, symbol=symbol)
    if not norm:
        return 0
    cols = list(norm[0].keys())
    conflict = [COL_SYMBOL, COL_DAILY_TS]
    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict]
    return bulk_upsert(
        TABLE_DAILY, cols, norm,
        conflict_cols=conflict, update_cols=update_cols,
        dsn=dsn, chunk_size=chunk_size
    )


def upsert_minute_bars_for_symbol(
    symbol: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    dsn: Optional[str] = None,
    update_cols: Optional[Sequence[str]] = None,
    chunk_size: int = 2000,
) -> int:
    norm = _normalize(rows, _map_minute_row, symbol=symbol)
    if not norm:
        return 0
    cols = list(norm[0].keys())
    conflict = [COL_SYMBOL, COL_MINUTE_TS]
    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict]
    return bulk_upsert(
        TABLE_MINUTE, cols, norm,
        conflict_cols=conflict, update_cols=update_cols,
        dsn=dsn, chunk_size=chunk_size
    )


def upsert_ticks_for_symbol(
    symbol: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    dsn: Optional[str] = None,
    update_cols: Optional[Sequence[str]] = None,
    chunk_size: int = 5000,
) -> int:
    norm = _normalize(rows, _map_tick_row, symbol=symbol)
    if not norm:
        return 0
    cols = list(norm[0].keys())
    conflict = [COL_SYMBOL, COL_TICK_TS]
    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict]
    return bulk_upsert(
        TABLE_TICK, cols, norm,
        conflict_cols=conflict, update_cols=update_cols,
        dsn=dsn, chunk_size=chunk_size
    )


def upsert_quotes_for_symbol(
    symbol: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    dsn: Optional[str] = None,
    update_cols: Optional[Sequence[str]] = None,
    chunk_size: int = 5000,
) -> int:
    norm = _normalize(rows, _map_quote_row, symbol=symbol)
    if not norm:
        return 0
    cols = list(norm[0].keys())
    conflict = [COL_SYMBOL, COL_QUOTE_TS]
    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict]
    return bulk_upsert(
        TABLE_QUOTE, cols, norm,
        conflict_cols=conflict, update_cols=update_cols,
        dsn=dsn, chunk_size=chunk_size
    )


__all__ = [
    "upsert_daily_bars_for_symbol",
    "upsert_minute_bars_for_symbol",
    "upsert_ticks_for_symbol",
    "upsert_quotes_for_symbol",
]