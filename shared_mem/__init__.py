# -*- coding: utf-8 -*-
"""
shared_mem: Thread-safe in-memory structures for Reflex

Provides:
- symbol_buffers: per-symbol double ring buffers for "trades" and "quotes"
- registry: lazy-initialized symbol registry with default fields and modes
- hydrate_snapshot(): quote-driven snapshot updater with basic microstructure features
- SymbolState model and a small trade queue
"""
from .buffers import symbol_buffers, ensure_symbol_buffers, trade_buffer_capacity, quote_buffer_capacity
from .registry import registry, set_mode, get_mode, get_all_modes, MODE_COLD, MODE_WATCH, MODE_WARM, MODE_HOT
from .hydrator import hydrate_snapshot
from .symbol_state import SymbolState
from .trade_queue import TradeSignalQueue, TradeSignal
