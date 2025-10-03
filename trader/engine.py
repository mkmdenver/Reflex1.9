# trader/engine.py
from __future__ import annotations
import csv
import json
import os
import time
import threading
import requests

from common.logger import setup_logger
from common.config import Config
# BUS can live under 'pubsub.bus' or flat as 'bus'
try:
    from pubsub.bus import BUS  # type: ignore
except Exception:
    from bus import BUS  # type: ignore

# Broker import can be packaged or flat
try:
    from trader.brokers.alpaca import Alpaca  # type: ignore
except Exception:
    from alpaca import Alpaca  # type: ignore

log = setup_logger("trader")

# --- Load JSON model (same file as Evaluator) -------------------------------
HERE = os.path.dirname(__file__)
MODELS_DIR = os.path.normpath(os.path.join(HERE, "..", "models"))
MODEL_PATH = os.path.join(MODELS_DIR, "momentum_filter_v1.json")
with open(MODEL_PATH, "r", encoding="utf-8") as f:
    MODEL_CFG = json.load(f)

# Exit params
def _exit_param(model_type: str, key: str, default_val):
    for m in MODEL_CFG.get("exit_models", []):
        if (m.get("type") or "").lower() == model_type:
            return m.get("params", {}).get(key, default_val)
    return default_val

TARGET_POINTS = float(_exit_param("fixed_gain", "target_points", 10))
STOP_POINTS   = float(_exit_param("fixed_stop", "stop_points", 5))
TAPE_THRESH   = float(_exit_param("orderflow_exit", "tape_pressure_threshold", 0.3))
MAX_HOLD_S    = int(_exit_param("orderflow_exit", "max_hold_seconds", 300))

# Add model params
ADD_PARAMS = (MODEL_CFG.get("add_model") or {}).get("params", {})
ADD_SIZE   = int(ADD_PARAMS.get("add_size", 1))         # multiplier of base qty per add
MAX_ADDS   = int(ADD_PARAMS.get("max_adds", 0))
MIN_MOM    = float(ADD_PARAMS.get("min_momentum", 0.01))

# Paper trade log
TRADE_LOG = os.path.join(os.path.dirname(__file__), "trade_log.csv")
if not os.path.exists(TRADE_LOG):
    with open(TRADE_LOG, "w", newline="") as f:
        csv.writer(f).writerow(["ts", "symbol", "side", "qty", "price", "mode", "note"])

DATAHUB = f"http://{Config.DATAHUB_HOST}:{Config.DATAHUB_PORT}"

def _get_snapshot(symbol: str):
    """
    Minimal snapshot fetcher; expects DataHub /symbols to return a JSON array of dicts.
    """
    try:
        r = requests.get(f"{DATAHUB}/symbols", timeout=2)
        r.raise_for_status()
        for row in r.json():
            if row.get("symbol") == symbol:
                return row
    except Exception as e:
        log.error(f"snapshot error: {e}")
    return {}

class Trader:
    def __init__(self):
        self.alpaca = Alpaca()
        self.base_qty = 10
        self.positions: dict[str, dict] = {}  # sym -> dict(entry_price, qty, ts, adds, last_add_ts)

    # --- Order helpers -------------------------------------------------------
    def _log_trade(self, symbol, side, qty, price, mode, note):
        with open(TRADE_LOG, "a", newline="") as f:
            csv.writer(f).writerow([time.strftime('%Y-%m-%dT%H:%M:%S'), symbol, side, qty, price, mode, note])
        log.info(f"Trade: {symbol} {side} x{qty} @ {price} mode={mode} note={note}")

    def _paper_fill(self, symbol: str, side: str, qty: int):
        snap = _get_snapshot(symbol)
        price = float(snap.get("last_price") or 0.0)
        note = "paper"
        self._log_trade(symbol, side, qty, price, "paper", note)
        return price, note

    def _alpaca_buy(self, symbol: str, qty: int):
        ok, price, note = self.alpaca.market_buy(symbol, qty)
        mode = "alpaca" if ok else "paper"
        if not ok:
            price, note = self._paper_fill(symbol, "buy", qty)
        else:
            self._log_trade(symbol, "buy", qty, price, mode, note)
        return price, note

    def _alpaca_sell(self, symbol: str, qty: int):
        ok, price, note = self.alpaca.market_sell(symbol, qty)
        mode = "alpaca" if ok else "paper"
        if not ok:
            price, note = self._paper_fill(symbol, "sell", qty)
        else:
            self._log_trade(symbol, "sell", qty, price, mode, note)
        return price, note

    # --- Signal handler ------------------------------------------------------
    def on_signal(self, msg: dict):
        if msg.get("type") != "signal.entry":
            return
        sym = msg["symbol"]
        if sym in self.positions:
            return  # already holding

        qty = self.base_qty
        if self.alpaca.is_configured:
            price, note = self._alpaca_buy(sym, qty)
        else:
            price, note = self._paper_fill(sym, "buy", qty)

        self.positions[sym] = {
            "entry_price": float(price),
            "qty": qty,
            "ts": time.time(),
            "adds": 0,
            "last_add_ts": 0.0,
        }

    # --- Background risk/exit & add loop ------------------------------------
    def _risk_loop(self):
        while True:
            remove: list[str] = []
            for sym, pos in list(self.positions.items()):
                snap = _get_snapshot(sym)
                last_price = float(snap.get("last_price") or 0.0)
                momentum = float(snap.get("momentum") or 0.0)
                age = time.time() - pos["ts"]
                gain = last_price - pos["entry_price"]

                # Fixed stop / gain exits / time / tape
                if gain >= TARGET_POINTS or -gain >= STOP_POINTS or age >= MAX_HOLD_S or abs(momentum) < TAPE_THRESH:
                    qty = pos["qty"]
                    if self.alpaca.is_configured:
                        self._alpaca_sell(sym, qty)
                    else:
                        self._paper_fill(sym, "sell", qty)
                    remove.append(sym)
                    continue
                # Adds (liquidity absorption approx): momentum positive & adds left
                if pos["adds"] < MAX_ADDS and momentum >= MIN_MOM and (time.time() - pos["last_add_ts"]) > 10:
                    add_qty = self.base_qty * max(1, ADD_SIZE)
                    if self.alpaca.is_configured:
                        self._alpaca_buy(sym, add_qty)
                    else:
                        self._paper_fill(sym, "buy", add_qty)
                    pos["qty"] += add_qty
                    pos["adds"] += 1
                    pos["last_add_ts"] = time.time()

            for sym in remove:
                self.positions.pop(sym, None)
            time.sleep(1)

    def run(self):
        BUS.subscribe("signals", self.on_signal)
        log.info("Trader running, enforcing exits & adds...")
        t = threading.Thread(target=self._risk_loop, daemon=True)
        t.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            pass
if __name__ == "__main__":
    Trader().run()
