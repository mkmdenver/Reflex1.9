from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime, timezone
from .buffers import DoubleRingBuffer

STATES = ("COLD","WATCH","WARM","HOT")

@dataclass
class SymbolInfo:
    state: str = 'COLD'
    last_state_change: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_price: float | None = None
    snapshot: dict = field(default_factory=dict)
    buffers: Dict[str, DoubleRingBuffer] = field(default_factory=lambda: {
        'default': DoubleRingBuffer()
    })

class Registry:
    def __init__(self):
        self._symbols: Dict[str, SymbolInfo] = {}

    def upsert(self, symbol: str) -> SymbolInfo:
        if symbol not in self._symbols:
            self._symbols[symbol] = SymbolInfo()
        return self._symbols[symbol]

    def set_state(self, symbol: str, new_state: str):
        si = self.upsert(symbol)
        if new_state != si.state:
            si.state = new_state
            si.last_state_change = datetime.now(timezone.utc)

    def symbols(self):
        return self._symbols

REGISTRY = Registry()

# seed a couple of symbols for demo
for sym in ("AAPL","MSFT","TSLA"):
    REGISTRY.upsert(sym)
