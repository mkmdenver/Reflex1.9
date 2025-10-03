# ---------- db_backfill.py ----------
"""
db_backfill.py
Handles tiered backfill routines for historical market data:
- Full: all available history
- Moderate: recent month
- Recent: last 2 days (with destructive refresh)

Uses poly_tools for data fetch and db_writer for upsert.
"""
import subprocess
import sys

# Ensure required modules are installed
required_packages = ["matplotlib", "pandas", "pandas_market_calendars"]

for package in required_packages:
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

from datetime import datetime, time, timedelta, timezone
from matplotlib.pyplot import table

from common.poly_tools import (
    poly_fetch_daily_bars,
    poly_fetch_minute_bars,
    poly_fetch_ticks,

)
from common.db_writer import (
    upsert_daily_bars_for_symbol,
    upsert_minute_bars_for_symbol,
    upsert_ticks_for_symbol
)
from common.dbutils import get_connection
from common.app_logging import setup_logger

log = setup_logger("db_backfill", level="DEBUG")

import pandas as pd
import pandas_market_calendars as mcal


def last_market_day_check(reference_dt: datetime = None) -> datetime:
    """
    Returns the most recent valid US market day before or on reference_dt.
    Defaults to now if not provided.
    """
    print("Last market day request")
    if reference_dt is None:
        print("No reference date provided, using current time")
        dt = datetime.utcnow()
    print("step 1")
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.valid_days(
        start_date=(dt - timedelta(days=10)).date().isoformat(),
        end_date=dt.date().isoformat()
    )

    if schedule.empty:
        raise ValueError("No valid market days found in range")
    print(f"step 2 {schedule}")

    last_valid = schedule[-1].to_pydatetime()
    print(f"last_valid: {last_valid}, type: {type(last_valid)}")
    resolved = datetime.combine(last_valid.date(), time(23, 59), tzinfo=timezone.utc)
    
    print("step 3")
    log.info(f"[📅] Resolved last market day: {resolved}")
    return resolved




def ingest_daily(symbols, start, end):
    log.info(f"[🟦] Ingesting daily bars: {start} → {end}")
    errors = []
    for symbol in symbols:
        try:

            df = poly_fetch_daily_bars(symbol, start, end)
            if df is None or df.empty:
                raise ValueError("Empty DataFrame returned")
            upsert_daily_bars_for_symbol(df, symbol)
        except Exception as e:
            log.error(f"[❌] Daily bars failed for {symbol}: {e}")
            errors.append((symbol, "daily", str(e)))
    return errors

def ingest_minute(symbols, start, end):
    log.info(f"[🟨] Ingesting minute bars: {start} → {end}")
    errors = []
    for symbol in symbols:
        try:
            log.debug(f"[🔸] Minute fetch for {symbol}")
            df = poly_fetch_minute_bars(symbol, start, end)
            if df is None or df.empty:
                raise ValueError("Empty DataFrame returned")
            upsert_minute_bars_for_symbol(df, symbol)
        except Exception as e:
            log.error(f"[❌] Minute bars failed for {symbol}: {e}")
            errors.append((symbol, "minute", str(e)))
    return errors

def ingest_tick(symbols, start, end):
    log.info(f"[🟥] Ingesting tick data: {start} → {end}")
    errors = []
    for symbol in symbols:
        try:
            log.debug(f"[🔻] Tick fetch for {symbol}")
            df = poly_fetch_ticks(symbol, start, end)
            if df is None or df.empty:
                raise ValueError("Empty DataFrame returned")
            upsert_ticks_for_symbol(df, symbol)
        except Exception as e:
            log.error(f"[❌] Tick data failed for {symbol}: {e}")
            errors.append((symbol, "tick", str(e)))
    return errors

def refresh_recent(symbols):
    now = last_market_day_check()
    cutoff = now - timedelta(days=5)
    log.info(f"[♻️] Refreshing recent data: {cutoff} → {now}")

    for table in ['daily_bars', 'minute_bars', 'ticks']:
        try:
            log.debug(f"[🧹] Deleting recent rows from {table}")
            conn = get_connection()
            cur = conn.cursor()
            delete_query = f"DELETE FROM {table} WHERE timestamp >= %s AND symbol = ANY(%s)"
            cur.execute(delete_query, (cutoff, symbols))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            log.error(f"[❌] Failed to delete from {table}: {e}")

    errors = []
    errors += ingest_daily(symbols, None, now)
    errors += ingest_minute(symbols, cutoff, now)
    errors += ingest_tick(symbols, cutoff, now)
    summarize_errors(errors)

def prime_moderate(symbols):
    print("\nModerate backfilly: ")
    try:
        now = last_market_day_check()
    except Exception as e:
        print(f"[❌] Failed to get last market day: {e}")
        return
    print(f"[📦] Moderate backfill ending at {now}")
    
    errors = []
    #errors += ingest_daily(symbols, None, now)
    #errors += ingest_minute(symbols, now - timedelta(days=30), now)
    errors += ingest_tick(symbols, now - timedelta(days=2), now)
    summarize_errors(errors)

def prime_full(symbols):
    print("Full backfill: ")
    now = last_market_day_check(datetime.utcnow())
    log.info(f"[📦] Full backfill ending at {now}")
    errors = []
    errors += ingest_daily(symbols, None, now)
    errors += ingest_minute(symbols, None, now)
    errors += ingest_tick(symbols, now - timedelta(days=5), now)
    summarize_errors(errors)

def run_backfill(symbols, mode="recent"):
    mode = mode.lower()
    log.info(f"[🚀] Starting backfiller mode: {mode}")
    if mode == "recent":
        refresh_recent(symbols)
    elif mode == "moderate":
        prime_moderate(symbols)
    elif mode == "full":
        prime_full(symbols)
    else:
        raise ValueError(f"Unknown backfill mode: {mode}")
    print(f"run backfill finish")

def summarize_errors(errors):
    if not errors:
        log.info("[✅] Backfill completed with no errors.")
        return
    log.warning(f"[⚠️] Backfill completed with {len(errors)} errors:")
    for symbol, stage, err in errors:
        log.warning(f"    • {symbol} [{stage}]: {err}")
    # Optional: write to file
    with open("backfill_errors.log", "a") as f:
        for symbol, stage, err in errors:
            f.write(f"{datetime.utcnow().isoformat()} {symbol} [{stage}]: {err}\n")