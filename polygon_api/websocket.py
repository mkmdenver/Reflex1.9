# polygon_api/websocket.py
# Production-ready Polygon WebSocket client with dynamic (un)subscribe hooks,
# robust reconnect, ping/pong, buffered send, and event dispatch.

from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
from collections import defaultdict
from typing import Callable, DefaultDict, Dict, Iterable, List, Optional, Set

try:
    import websocket  # websocket-client
except Exception as e:
    raise RuntimeError(
        "Missing dependency 'websocket-client'. Install with: pip install websocket-client"
    ) from e

Json = Dict[str, object]
EventHandler = Callable[[Json], None]


class PolygonWebSocketClient:
    """
    One WebSocket connection to Polygon for 'stocks' (default base_url).
    - Auth on open, auto-reconnect with exp backoff + jitter, re-auth
    - Ping/pong keepalive
    - Thread-safe dynamic subscribe/unsubscribe for channels: T (trades), Q (quotes), A (aggregates)
    - Buffered send queue: messages are queued when disconnected and flushed on reconnect
    - Per-event-type handler registry (ev: 'T','Q','A','*')
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "wss://socket.polygon.io/stocks",
        *,
        name: str = "PolygonWS",
        ping_interval: int = 20,
        ping_timeout: int = 10,
        reconnect: bool = True,
        max_backoff: int = 60,
        max_send_queue: int = 10000,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.name = name
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect = reconnect
        self.max_backoff = max_backoff

        self._log = logger or logging.getLogger(name)
        if not self._log.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("[%(asctime)s] %(name)s %(levelname)s: %(message)s")
            h.setFormatter(fmt)
            self._log.addHandler(h)
            self._log.setLevel(logging.INFO)

        self._wsapp: Optional[websocket.WebSocketApp] = None
        self._runner: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._connected_evt = threading.Event()

        self._subs: Dict[str, Set[str]] = {"T": set(), "Q": set(), "A": set()}
        self._lock = threading.RLock()

        self._sendq: "queue.Queue[str]" = queue.Queue(maxsize=max_send_queue)
        self._sender_thread: Optional[threading.Thread] = None

        self._handlers: DefaultDict[str, List[EventHandler]] = defaultdict(list)
        self._last_msg_time: float = 0.0
        self._connect_attempts: int = 0

    # ---------- public ----------
    def start(self) -> None:
        self._stop_evt.clear()
        self._spawn_ws()
        if self._sender_thread is None or not self._sender_thread.is_alive():
            self._sender_thread = threading.Thread(
                target=self._sendq_pump, name=f"{self.name}-sender", daemon=True
            )
            self._sender_thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_evt.set()
        self._connected_evt.clear()
        if self._wsapp:
            try:
                self._wsapp.close()
            except Exception:
                pass
        if self._runner and self._runner.is_alive():
            self._runner.join(timeout=timeout)
        if self._sender_thread and self._sender_thread.is_alive():
            self._sender_thread.join(timeout=timeout)

    def register_handler(self, ev: str, handler: EventHandler) -> None:
        self._handlers[ev].append(handler)

    def subscribe_symbol(self, symbol: str, channel: str = "T") -> None:
        s = symbol.strip().upper()
        if not s:
            return
        with self._lock:
            if s not in self._subs.setdefault(channel, set()):
                self._subs[channel].add(s)
                self._enqueue(self._make_sub_msg([s], channel))
                self._log.info("Subscribe %s.%s", channel, s)

    def unsubscribe_symbol(self, symbol: str, channel: str = "T") -> None:
        s = symbol.strip().upper()
        if not s:
            return
        with self._lock:
            if s in self._subs.get(channel, set()):
                self._subs[channel].remove(s)
                self._enqueue(self._make_unsub_msg([s], channel))
                self._log.info("Unsubscribe %s.%s", channel, s)

    def subscribe_many(self, symbols: Iterable[str], channel: str = "T") -> None:
        syms = [s.strip().upper() for s in symbols if s and s.strip()]
        if not syms:
            return
        with self._lock:
            new = [s for s in syms if s not in self._subs.setdefault(channel, set())]
            if new:
                self._subs[channel].update(new)
                self._enqueue(self._make_sub_msg(new, channel))
                self._log.info("Subscribe %s to %d symbols", channel, len(new))

    def unsubscribe_many(self, symbols: Iterable[str], channel: str = "T") -> None:
        syms = [s.strip().upper() for s in symbols if s and s.strip()]
        if not syms:
            return
        with self._lock:
            rem = [s for s in syms if s in self._subs.get(channel, set())]
            if rem:
                for s in rem:
                    self._subs[channel].remove(s)
                self._enqueue(self._make_unsub_msg(rem, channel))
                self._log.info("Unsubscribe %s from %d symbols", channel, len(rem))

    def replace_with(self, symbols: Iterable[str], channel: str = "T") -> None:
        """Replace current set for 'channel' with 'symbols' (diffs under the hood)."""
        target = {s.strip().upper() for s in symbols if s and s.strip()}
        with self._lock:
            current = self._subs.setdefault(channel, set())
            add = sorted(target - current)
            rem = sorted(current - target)
            if rem:
                self._enqueue(self._make_unsub_msg(rem, channel))
            if add:
                self._enqueue(self._make_sub_msg(add, channel))
            self._subs[channel] = target
            self._log.info("Replaced %s set: +%d / -%d (now %d)", channel, len(add), len(rem), len(target))

    def subscribed(self, channel: Optional[str] = None) -> Dict[str, Set[str]]:
        with self._lock:
            if channel:
                return {channel: set(self._subs.get(channel, set()))}
            return {ch: set(vals) for ch, vals in self._subs.items()}

    # ---------- internals ----------
    def _spawn_ws(self) -> None:
        self._wsapp = websocket.WebSocketApp(
            self.base_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._runner = threading.Thread(target=self._run_forever, name=f"{self.name}-runner", daemon=True)
        self._runner.start()

    def _run_forever(self) -> None:
        backoff = 1
        self._connect_attempts = 0
        while not self._stop_evt.is_set():
            self._connect_attempts += 1
            try:
                self._log.info("Connecting %s (attempt %d)", self.base_url, self._connect_attempts)
                assert self._wsapp is not None
                self._wsapp.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    reconnect=0,
                    sslopt={"check_hostname": False},
                )
            except Exception as e:
                self._log.exception("run_forever error: %s", e)

            self._connected_evt.clear()

            if self._stop_evt.is_set() or not self.reconnect:
                self._log.info("WS stopped (reconnect=%s).", self.reconnect)
                break

            slp = min(backoff, self.max_backoff)
            jitter = random.uniform(0, slp * 0.2)
            self._log.warning("WS disconnected; retrying in %.1fs", slp + jitter)
            time.sleep(slp + jitter)
            backoff = min(backoff * 2, self.max_backoff)
            self._spawn_ws()
            return  # handoff to new runner thread

    def _sendq_pump(self) -> None:
        while not self._stop_evt.is_set():
            try:
                msg = self._sendq.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if self._connected_evt.is_set() and self._wsapp and self._wsapp.sock:
                    self._wsapp.send(msg)
                else:
                    time.sleep(0.2)
                    self._sendq.put_nowait(msg)
            except Exception:
                time.sleep(0.5)
                try:
                    self._sendq.put_nowait(msg)
                except queue.Full:
                    self._log.error("Outbound queue full; dropping message.")

    def _enqueue(self, payload: Json) -> None:
        raw = json.dumps(payload, separators=(",", ":"))
        try:
            self._sendq.put_nowait(raw)
        except queue.Full:
            self._log.error("Outbound queue full; dropping: %s", raw[:256])

    # ---------- callbacks ----------
    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self._log.info("WS opened; sending auth.")
        self._connected_evt.set()
        self._enqueue({"action": "auth", "params": self.api_key})

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        self._last_msg_time = time.time()
        try:
            data = json.loads(message)
        except Exception:
            self._log.debug("Non-JSON message: %s", message)
            return

        events = data if isinstance(data, list) else [data]
        for ev in events:
            et = str(ev.get("ev") or "")
            if et in (None, "", "status"):
                status = str(ev.get("status", "")).lower()
                msg = str(ev.get("message", "")).lower()
                if "success" in status and "authenticated" in msg:
                    self._log.info("Authenticated. Re-subscribing prior sets.")
                    self._resub_all()
                continue
            for h in self._handlers.get(et, []):
                try:
                    h(ev)
                except Exception:
                    self._log.exception("Handler error for ev=%s", et)
            for h in self._handlers.get("*", []):
                try:
                    h(ev)
                except Exception:
                    self._log.exception("Wildcard handler error")

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        self._log.error("WS error: %s", error)

    def _on_close(self, ws: websocket.WebSocketApp, code: Optional[int], reason: Optional[str]) -> None:
        self._connected_evt.clear()
        self._log.warning("WS closed code=%s reason=%s", code, reason)

    def _resub_all(self) -> None:
        with self._lock:
            for ch, syms in self._subs.items():
                if syms:
                    self._enqueue(self._make_sub_msg(sorted(syms), ch))
                    self._log.info("Re-subscribed %s -> %d symbols", ch, len(syms))

    @staticmethod
    def _make_sub_msg(symbols: Iterable[str], channel: str) -> Json:
        params = ",".join(f"{channel}.{s}" for s in symbols)
        return {"action": "subscribe", "params": params}

    @staticmethod
    def _make_unsub_msg(symbols: Iterable[str], channel: str) -> Json:
        params = ",".join(f"{channel}.{s}" for s in symbols)
        return {"action": "unsubscribe", "params": params}
