# -*- coding: utf-8 -*-
"""
Snapshot hydrator: updates per-symbol, in-memory quote-driven snapshots.

Computes:
  - spread, mid
  - aggregate sizes (bid_sz, ask_sz)
  - simple imbalance and pressure signals

Accepts quote event dict with fields:
  timestamp, bid_price, bid_size, ask_price, ask_size
(from quote_stream)
"""

from __future__ import annotations
from typing import Dict, Any
from .registry import registry

def _compute_features(bid_px: float, bid_sz: float, ask_px: float, ask_sz: float) -> Dict[str, float]:
    spread = max(0.0, float(ask_px) - float(bid_px))
    mid = (float(ask_px) + float(bid_px)) / 2.0 if spread > 0 else float(bid_px)
    sum_bid = float(bid_sz)
    sum_ask = float(ask_sz)
    tot = sum_bid + sum_ask
    imbalance = ((sum_bid - sum_ask) / tot) if tot > 1e-12 else 0.0
    pressure = ((bid_sz - ask_sz) / max(bid_sz + ask_sz, 1e-12))
    return {
        "spread": spread, "mid": mid,
        "bid_sz": sum_bid, "ask_sz": sum_ask,
        "imbalance": imbalance, "pressure": pressure
    }

def hydrate_snapshot(symbol: str, quote: Dict[str, Any]) -> None:
    """
    Update in-memory snapshot fields for a symbol based on a single quote tick.
    Does not persist. Quotes are memory-only by policy.
    """
    sreg = registry[symbol]
    bid = float(quote.get("bid", quote.get("bid_price", 0.0)))
    ask = float(quote.get("ask", quote.get("ask_price", 0.0)))
    bid_sz = float(quote.get("buy_volume", quote.get("bid_size", 0.0)))
    ask_sz = float(quote.get("sell_volume", quote.get("ask_size", 0.0)))
    ts = quote.get("timestamp")
    feats = _compute_features(bid, bid_sz, ask, ask_sz)
    snap = sreg.get("snapshot", {})
    snap.update(feats)
    sreg["snapshot"] = snap
    sreg["last_update"] = ts
    # Keep last_price aligned if mid moves
    sreg["last_price"] = snap["mid"]
