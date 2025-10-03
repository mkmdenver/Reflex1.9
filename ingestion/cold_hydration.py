import os
from datetime import datetime
from shared_mem.registry import registry
from shared_mem.buffers import symbol_buffers
from common.timeutils import get_date_range, get_session_anchor
from fundamentals.api import fetch_fundamentals
from market_data.api import fetch_daily_bars, fetch_minute_bars, fetch_ticks
from snapshots.snapshot_metrics import precalc_indicators

REPLAY_MODE = os.getenv("REFLEXION_REPLAY", "false").lower() == "true"

def load_fundamentals(symbol):
    """Load fundamentals into registry for a symbol."""
    fundamentals = fetch_fundamentals(symbol)
    if fundamentals:
        registry[symbol]["fundamentals"] = fundamentals
        print(f"[📊] Fundamentals loaded for {symbol}")
    else:
        print(f"[⚠️] No fundamentals found for {symbol}")

def load_indicator_precals(symbol):
    """
    Load enough historical data to compute all model filter fields.
    This primes the snapshot engine so filters can run immediately.
    """
    model = registry[symbol].get("model", {})
    lookbacks = _extract_max_lookbacks(model)

    # Pull minute bars for the largest lookback needed
    start_date, end_date = get_date_range(lookbacks or 5)
    df = fetch_minute_bars(symbol, start_date, end_date)
    if not df.empty:
        for _, row in df.iterrows():
            tick = {
                "timestamp": row["timestamp"],
                "price": row["close"],
                "size": row["volume"]
            }
            symbol_buffers[symbol]["trades"].append(tick)
        registry[symbol]["last_bar"] = df.iloc[-1].to_dict()

    # Precompute indicators into snapshot
    registry[symbol]["snapshot"] = precalc_indicators(symbol)

def load_daily_bars(symbol, lookback_days=252):
    """Load daily bars into registry for a symbol."""
    start_date, end_date = get_date_range(lookback_days)
    df = fetch_daily_bars(symbol, start_date, end_date)
    if df is not None and not df.empty:
        registry[symbol]["daily_bars"] = df.to_dict(orient="records")
    print(f"[📅] Loaded {lookback_days} daily bars for {symbol} ({start_date} → {end_date})")

def load_minute_bars(symbol, enough_for_indicators=True):
    """Load minute bars for indicator lookbacks."""
    lookback_days = 30 if enough_for_indicators else 5
    start_date, end_date = get_date_range(lookback_days)
    df = fetch_minute_bars(symbol, start_date, end_date)
    if not df.empty:
        for _, row in df.iterrows():
            tick = {
                "timestamp": row["timestamp"],
                "price": row["close"],
                "size": row["volume"]
            }
            symbol_buffers[symbol]["trades"].append(tick)
        registry[symbol]["last_bar"] = df.iloc[-1].to_dict()
    print(f"[⏱️] Minute bars loaded for {symbol} ({start_date} → {end_date})")

def load_ticks(symbol, days=2, up_to_now=True):
    """Load recent tick data into trade buffer."""
    start_date, end_date = get_date_range(days)
    if up_to_now and not REPLAY_MODE:
        end_date = datetime.utcnow().date()
    df = fetch_ticks(symbol, start_date, end_date)
    if not df.empty:
        for _, row in df.iterrows():
            tick = {
                "timestamp": row["timestamp"],
                "price": row["price"],
                "size": row["size"]
            }
            symbol_buffers[symbol]["trades"].append(tick)
    print(f"[💹] Loaded {days} days of ticks for {symbol} ({start_date} → {end_date})")

def _extract_max_lookbacks(model):
    """Helper to find the largest lookback needed from model config."""
    max_lookback = 0
    for section in ("entry_model", "exit_models", "add_model"):
        if isinstance(model.get(section), dict):
            lb = model[section].get("params", {}).get("lookback", 0)
            max_lookback = max(max_lookback, lb)
        elif isinstance(model.get(section), list):
            for m in model[section]:
                lb = m.get("params", {}).get("lookback", 0)
                max_lookback = max(max_lookback, lb)
    return max_lookback