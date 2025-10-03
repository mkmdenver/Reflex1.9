# -*- coding: utf-8 -*-
"""
Thread-safe trade signal queue for evaluator -> trader messaging.

Signals are simple dicts with:
  - symbol
  - action: 'BUY' | 'SELL' | 'CANCEL' | 'FLATTEN'
  - qty
  - meta (optional): dict for model, rationale, risk, etc.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from collections import deque
from threading import RLock
from typing import Optional, Deque, Dict, Any, List

@dataclass
class TradeSignal:
    symbol: str
    action: str
    qty: int
    meta: Dict[str, Any] = field(default_factory=dict)

class TradeSignalQueue:
    def __init__(self, maxlen: int = 5000):
        self._dq: Deque[TradeSignal] = deque(maxlen=maxlen)
        self._lock = RLock()

    def put(self, signal: TradeSignal) -> None:
        with self._lock:
            self._dq.append(signal)

    def get(self) -> Optional[TradeSignal]:
        with self._lock:
            if self._dq:
                return self._dq.popleft()
            return None

    def drain(self, max_items: Optional[int] = None) -> List[TradeSignal]:
        out: List[TradeSignal] = []
        with self._lock:
            n = len(self._dq) if max_items is None else min(max_items, len(self._dq))
            for _ in range(n):
                out.append(self._dq.popleft())
        return out

    def __len__(self) -> int:
        with self._lock:
            return len(self._dq)
