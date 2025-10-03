# ingestion/tick_stream_proc.py
# Standalone process for Polygon 'T' (trades). Config-first secrets via common/creds.py

from __future__ import annotations

import json
import logging
import queue
import signal
import threading
import time
from typing import Dict, Optional

from polygon_api.websocket import PolygonWebSocketClient
from common.creds import get_polygon_api_key, get_redis_url

try:
    import redis  # redis-py (Garnet compatible)
except Exception as e:
    raise RuntimeError("Missing dependency 'redis'. Install with: pip install redis") from e

Json = Dict[str, object]


class TradeProcess:
    def __init__(
        self,
        *,
        control_channel: str = "reflex:wsctl:ticks",
        out_channel: str = "reflex:bus:trades",
        health_key: str = "reflex:health:tick_proc",
        max_queue: int = 500_000,
        workers: int = 2,
        log_level: str = "INFO",
    ) -> None:
        # Logging
        self.log = logging.getLogger("TickProcess")
        if not self.log.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("[%(asctime)s] %(name)s %(levelname)s: %(message)s")
            h.setFormatter(fmt)
            self.log.addHandler(h)
        self.log.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Secrets & endpoints (from config.py)
        api_key = get_polygon_api_key()
        redis_url = get_redis_url()

        # Redis
        self.r = redis.from_url(redis_url, decode_responses=True)
        self.pub = self.r

        # Channels / keys
        self.control_channel = control_channel
        self.out_channel = out_channel
        self.health_key = health_key

        # WS client (this process owns its socket)
        self.ws = PolygonWebSocketClient(api_key, name="PolygonWS-T")
        self.ws.register_handler("T", self._on_trade_event)

        # Pipeline
        self._q: "queue.Queue[Json]" = queue.Queue(maxsize=max_queue)
        self._workers = max(1, workers)
        self._threads: list[threading.Thread] = []
        self._stop_evt = threading.Event()

        # Metrics
        self._processed = 0
        self._dropped = 0
        self._last_proc_ns = 0

    # ----- lifecycle -----
    def start(self) -> None:
        self._install_signals()
        self.ws.start()
        self._start_workers()
        self._start_control_listener()
        self._start_health_loop()
        self.log.info("TickProcess started. control=%s out=%s", self.control_channel, self.out_channel)

        try:
            while not self._stop_evt.is_set():
                time.sleep(0.5)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop_evt.set()
        self.ws.stop()
        for _ in self._threads:
            try:
                self._q.put_nowait(None)  # type: ignore
            except Exception:
                pass
        for t in self._threads:
            t.join(timeout=5.0)
        self._threads.clear()
        self.log.info("TickProcess stopped. processed=%d dropped=%d", self._processed, self._dropped)

    def _install_signals(self) -> None:
        def _sig(_signo, _frame):
            self.log.info("Signal received; shutting down.")
            self._stop_evt.set()
        try:
            signal.signal(signal.SIGINT, _sig)
            signal.signal(signal.SIGTERM, _sig)
        except Exception:
            pass

    # ----- WS callback -----
    def _on_trade_event(self, ev: Json) -> None:
        sym = ev.get("sym")
        if not isinstance(sym, str) or not sym:
            return
        try:
            self._q.put_nowait(ev)
        except queue.Full:
            try:
                _ = self._q.get_nowait()
            except queue.Empty:
                pass
            finally:
                try:
                    self._q.put_nowait(ev)
                except queue.Full:
                    self._dropped += 1
                    if self._dropped % 10000 == 1:
                        self.log.warning("Trade queue saturated; drops=%d", self._dropped)

    # ----- workers -----
    def _start_workers(self) -> None:
        for i in range(self._workers):
            t = threading.Thread(target=self._worker_loop, name=f"TradeWorker-{i}", daemon=True)
            self._threads.append(t)
            t.start()

    def _worker_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                item = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                break
            start_ns = time.perf_counter_ns()
            try:
                out = self._normalize_trade(item)
                if out is not None:
                    self.pub.publish(self.out_channel, json.dumps(out, separators=(",", ":")))
                    self._processed += 1
            except Exception:
                self.log.exception("Trade worker error")
            finally:
                self._last_proc_ns = time.perf_counter_ns() - start_ns

    @staticmethod
    def _normalize_trade(ev: Json) -> Optional[Dict[str, object]]:
        try:
            sym = str(ev["sym"])
            price = float(ev["p"])
            size = int(ev["s"])
            ts = int(ev["t"])
        except Exception:
            return None

        out = {"type": "trade", "symbol": sym, "price": price, "size": size, "ts": ts}
        x = ev.get("x")
        if isinstance(x, int):
            out["ex"] = x
        i = ev.get("i")
        if isinstance(i, (int, str)):
            out["id"] = i
        c = ev.get("c")
        if isinstance(c, list):
            out["cond"] = c
        return out

    # ----- control plane -----
    def _start_control_listener(self) -> None:
        t = threading.Thread(target=self._control_loop, name="TickControl", daemon=True)
        t.start()

    def _control_loop(self) -> None:
        ps = self.r.pubsub()
        ps.subscribe(self.control_channel)
        self.log.info("Subscribed to control channel: %s", self.control_channel)
        for msg in ps.listen():
            if self._stop_evt.is_set():
                break
            if msg.get("type") != "message":
                continue
            try:
                payload = json.loads(msg["data"])
                op = str(payload.get("op") or "").lower()
                channel = str(payload.get("channel") or "").upper()
                symbols = payload.get("symbols") or []
                if channel != "T":
                    continue
                if op == "subscribe":
                    self.ws.subscribe_many(symbols, channel="T")
                elif op == "unsubscribe":
                    self.ws.unsubscribe_many(symbols, channel="T")
                elif op == "replace":
                    self.ws.replace_with(symbols, channel="T")
            except Exception:
                self.log.exception("Invalid control message: %s", msg)

    # ----- health -----
    def _start_health_loop(self) -> None:
        t = threading.Thread(target=self._health_loop, name="TickHealth", daemon=True)
        t.start()

    def _health_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                stats = {
                    "proc": "tick",
                    "processed": self._processed,
                    "dropped": self._dropped,
                    "qsize": self._q.qsize(),
                    "subs": {k: sorted(list(v)) for k, v in self.ws.subscribed().items()},
                    "ts": int(time.time() * 1_000),
                }
                # health can live alongside other proc health in Redis
                self.r.set("reflex:health:tick_proc", json.dumps(stats, separators=(",", ":")))
            except Exception:
                self.log.debug("Health update failed", exc_info=True)
            time.sleep(2.0)


if __name__ == "__main__":
    TradeProcess().start()
