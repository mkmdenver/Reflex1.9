# --- bar_builder.py ---
# Builds and stores 1-minute bars from tick buffers

import time
import os
import pandas as pd
from shared_mem.buffers import symbol_buffers
from shared_mem.registry import registry
from common.db_writer import upsert_minute_bars_for_symbol
from diagnostics.model_logger import log_model_decision

REPLAY_MODE = os.getenv("REFLEXION_REPLAY", "false").lower() == "true"

def build_minute_bar(symbol):
    ticks = symbol_buffers[symbol]["trades"].get_short()  # explicitly use trade buffer
    if not ticks:
        return None

    prices = [t['price'] for t in ticks]
    volumes = [t['size'] for t in ticks]

    return {
        'symbol': symbol,
        'timestamp': ticks[-1]['timestamp'],
        'open': prices[0],
        'high': max(prices),
        'low': min(prices),
        'close': prices[-1],
        'volume': sum(volumes)
    }

def start_bar_builder():
    print("[📊] Bar builder started...")
    interval = 60 if not REPLAY_MODE else 0.01  # accelerate in replay mode

    while True:
        try:
            for symbol in symbol_buffers:
                bar = build_minute_bar(symbol)
                if bar:
                    # Store in DB
                    df = pd.DataFrame([bar])
                    upsert_minute_bars_for_symbol(df, symbol)

                    # Store in registry for cockpit/snapshot use
                    registry[symbol]['last_bar'] = bar

                    # Log for diagnostics
                    log_model_decision(
                        symbol,
                        "minute_bar_built",
                        registry[symbol].get("model", {}),
                        registry[symbol].get("snapshot", {}),
                        registry[symbol].get("flags", {})
                    )

                    print(f"[✅] {symbol} 1-min bar: O={bar['open']} H={bar['high']} L={bar['low']} C={bar['close']} V={bar['volume']}")

        except Exception as e:
            print(f"[❌] Bar builder error: {e}")

        time.sleep(interval)