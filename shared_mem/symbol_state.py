# -*- coding: utf-8 -*-
"""
Lightweight SymbolState model for bookkeeping and potential state transitions.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from .registry import MODE_COLD, MODE_WATCH, MODE_WARM, MODE_HOT

@dataclass
class SymbolState:
    symbol: str
    mode: str = MODE_COLD
    flags: Dict[str, Any] = field(default_factory=dict)
    last_update: Optional[str] = None
    last_price: Optional[float] = None

    def promote(self, target: str) -> None:
        self.mode = target

    def demote(self, target: str) -> None:
        self.mode = target
