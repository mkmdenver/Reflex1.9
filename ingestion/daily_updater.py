# --- daily_updater.py ---
# Daily updater for fetching and storing daily bars for active symbols

import time
import os
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from polygon_api.rest import fetch_daily_bars
from common.db_writer import upsert_daily_bars_for_symbol
from shared_mem.registry import registry
from diagnostics.model_logger import log_model_decision

REPLAY_MODE = os.getenv("REFLEXION_REPLAY", "false").lower() == "true"

def get_last_market_day(reference_date=None):
    if reference_date is None:
        reference_date = datetime.utcnow().date()

    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(
        start_date=reference_date - timedelta(days=7),
        end_date=reference_date
    )
    last_open = schedule.index[-1].date()
    return last_open.strftime("%Y-%m-%d")

def start_daily_bar_updater():
    print("[📅] Daily bar updater started...")
    interval = 3600 if not REPLAY_MODE else 86400  # 1h live, 1d replay

    while True:
        try:
            active_date = get_last_market_day()
            print(f"[📅] Fetching daily bars for {active_date}...")

            for symbol in registry:
                try:
                    df = fetch_daily_bars(symbol, date=active_date)
                    if df is not None and not df.empty:
                        upsert_daily_bars_for_symbol(df, symbol)
                        registry[symbol]['daily_bars_updated'] = True

                        # Log for diagnostics
                        log_model_decision(
                            symbol,
                            "daily_bar_update",
                            registry[symbol].get("model", {}),
                            registry[symbol].get("snapshot", {}),
                            registry[symbol].get("flags", {})
                        )
                        print(f"[✅] {symbol} daily bars updated.")
                    else:
                        print(f"[⚠️] No daily bars returned for {symbol}.")

                except Exception as e:
                    print(f"[❌] Error updating {symbol} daily bars: {e}")

            if REPLAY_MODE:
                print("[⏪] Replay mode: daily updater sleeping until next simulated day.")
                break  # In replay, run once per simulated day
            else:
                time.sleep(interval)

        except Exception as e:
            print(f"[❌] Daily updater loop error: {e}")
            time.sleep(interval)