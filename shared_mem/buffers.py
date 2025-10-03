# -*- coding: utf-8 -*-
"""
Double ring buffers per symbol for high-throughput ingestion.

- Quotes remain memory-only (policy).
- Trades are persisted to DB by writers; buffers provide recent slices for bar building.
- get_short(): drain-and-return recent events (used by bar_builder as a minute bucket).

Design:
  Each buffer is a lock-protected double deque. Writers append to 'active'.
  Readers call drain() which atomically swaps active<->drain and returns the drained items.
"""

from __future__ import annotations
from collections import deque
from threading import RLock
from typing import Dict, List, Any, DefaultDict
from collections import defaultdict
import os

# Capacities (can be tuned via env)
trade_buffer_capacity = int(os.getenv("REFLEX_TRADE_BUFFER_CAP", "200000"))
quote_buffer_capacity = int(os.getenv("REFLEX_QUOTE_BUFFER_CAP", "300000"))

class DoubleRingBuffer:
    __slots__ = ("_active", "_drain", "_lock", "_maxlen")

    def __init__(self, capacity: int):
        self._active: deque = deque(maxlen=capacity)
        self._drain: deque = deque(maxlen=capacity)
        self._lock = RLock()
        self._maxlen = capacity

    def append(self, item: Any) -> None:
        with self._lock:
            self._active.append(item)

    def drain(self) -> List[Any]:
        """
        Atomically swap active and drain deques, return drained items as list.
        Consuming read—used for minute bar aggregation.
        """
        with self._lock:
            self._active, self._drain = self._drain, self._active
            items = list(self._drain)
            self._drain.clear()
            return items

    # Back-compat for older references
    def get_short(self) -> List[Any]:
        return self.drain()

    def snapshot(self, max_items: int | None = None) -> List[Any]:
        with self._lock:
            items = list(self._active)
        return items[-max_items:] if (max_items and max_items > 0) else items

    def __len__(self) -> int:
        with self._lock:
            return len(self._active)

# Global container: { "SYMBOL": { "trades": DoubleRingBuffer, "quotes": DoubleRingBuffer } }
symbol_buffers: Dict[str, Dict[str, DoubleRingBuffer]] = {}
_symbol_lock = RLock()

def ensure_symbol_buffers(symbol: str) -> Dict[str, DoubleRingBuffer]:
    s = symbol.upper()
    with _symbol_lock:
        if s not in symbol_buffers:
            symbol_buffers[s] = {
                "trades": DoubleRingBuffer(trade_buffer_capacity),
                "quotes": DoubleRingBuffer(quote_buffer_capacity),
            }
        return symbol_buffers[s]
