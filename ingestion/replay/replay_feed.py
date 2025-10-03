# --- replay_feed.py ---
# Provides historical tick and quote data for Reflexion replay mode

import csv
import os
import time
from datetime import datetime

REPLAY_DATA_PATH = os.path.abspath("replay_data")  # Directory with CSV files

def replay_ticks():
    """
    Generator yielding (symbol, tick) from historical tick data.
    Expected CSV format: timestamp,symbol,price,size,side
    """
    tick_file = os.path.join(REPLAY_DATA_PATH, "ticks.csv")
    with open(tick_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tick = {
                "timestamp": row["timestamp"],
                "price": float(row["price"]),
                "size": int(row["size"]),
                "side": row.get("side", "buy")
            }
            yield row["symbol"], tick

def replay_quotes():
    """
    Generator yielding (symbol, quote) from historical quote data.
    Expected CSV format: timestamp,symbol,bid,ask,bid_size,ask_size
    """
    quote_file = os.path.join(REPLAY_DATA_PATH, "quotes.csv")
    with open(quote_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            quote = {
                "timestamp": row["timestamp"],
                "bid": float(row["bid"]),
                "ask": float(row["ask"]),
                "bid_size": int(row["bid_size"]),
                "ask_size": int(row["ask_size"])
            }
            yield row["symbol"], quote