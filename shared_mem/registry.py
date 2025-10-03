# -*- coding: utf-8 -*-
"""
Symbol registry: thread-safe dict-of-dicts with sensible defaults.

Usage:
  registry["AAPL"]["last_price"] = 123.45
  set_mode("AAPL", MODE_WARM)
  get_mode("AAPL") -> "WARM"
"""

from __future__ import annotations
from threading import RLock
from typing import Dict, Any

MODE_COLD  = "COLD"
MODE_WATCH = "WATCH"
MODE_WARM  = "WARM"
MODE_HOT   = "HOT"

_default_fields = {
    "mode": MODE_COLD,
    "flags": {},
    "last_update": None,
    "last_price": None,
    "last_quote": None,
    "snapshot": {  # real-time computed fields used by evaluator
        "spread": 0.0, "mid": 0.0,
        "bid_sz": 0.0, "ask_sz": 0.0,
        "imbalance": 0.0, "pressure": 0.0,
    },
}

class _Registry:
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}
        self._lock = RLock()

    def _ensure(self, symbol: str) -> Dict[str, Any]:
        s = symbol.upper()
        with self._lock:
            if s not in self._data:
                # deep copy defaults
                self._data[s] = {
                    "mode": _default_fields["mode"],
                    "flags": {},
                    "last_update": None,
                    "last_price": None,
                    "last_quote": None,
                    "snapshot": dict(_default_fields["snapshot"]),
                }
            return self._data[s]

    def __getitem__(self, symbol: str) -> Dict[str, Any]:
        return self._ensure(symbol)

    def __setitem__(self, symbol: str, value: Dict[str, Any]) -> None:
        s = symbol.upper()
        with self._lock:
            self._data[s] = value

    def __contains__(self, symbol: str) -> bool:
        s = symbol.upper()
        with self._lock:
            return s in self._data

    def set_mode(self, symbol: str, mode: str) -> None:
        s = symbol.upper()
        with self._lock:
            self._ensure(s)
            self._data[s]["mode"] = mode.upper()

    def get_mode(self, symbol: str) -> str:
        s = symbol.upper()
        with self._lock:
            self._ensure(s)
            return self._data[s]["mode"]

    def get_all_modes(self) -> Dict[str, str]:
        with self._lock:
            return {k: v.get("mode", MODE_COLD) for k, v in self._data.items()}

registry = _Registry()

# Convenience wrappers
def set_mode(symbol: str, mode: str) -> None:
    registry.set_mode(symbol, mode)

def get_mode(symbol: str) -> str:
    return registry.get_mode(symbol)

def get_all_modes() -> Dict[str, str]:
    return registry.get_all_modes()
