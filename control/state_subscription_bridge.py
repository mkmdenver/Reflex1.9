# control/state_subscription_bridge.py
# State → Subscription Bridge. Config-first for Postgres and Redis via common/creds.py

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from typing import Dict, Optional, List, Tuple, Set

from common.creds import get_pg_dsn, get_redis_url

try:
    import redis
except Exception as e:
    raise RuntimeError("Missing dependency 'redis'. Install with: pip install redis") from e

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except Exception as e:
    raise RuntimeError("Missing dependency 'psycopg2-binary'. Install with: pip install psycopg2-binary") from e

STATE_ORDER = {"COLD": 0, "WARM": 1, "HOT": 2}
VALID_STATES = set(STATE_ORDER.keys())
SRC_OVERRIDE = "override"
SRC_EVAL = "evaluator"
SRC_CHART = "chart"
SRC_DB = "db"
SOURCE_PRIORITY = [SRC_OVERRIDE, SRC_EVAL, SRC_CHART, SRC_DB]

CHART_TTL_SEC = 45
DEBOUNCE_MS = 150


class StateSubscriptionBridge:
    def __init__(
        self,
        *,
        db_notify_channel: str = "reflex_state_changes",
        table: str = "symbol_state",
        col_symbol: str = "symbol",
        col_state: str = "state",
        col_dnt: str = "do_not_trade",
        in_eval: str = "reflex:state:evaluator",
        in_override: str = "reflex:state:override",
        in_chart: str = "reflex:state:chart",
        ctl_ticks: str = "reflex:wsctl:ticks",
        ctl_quotes: str = "reflex:wsctl:quotes",
        health_key: str = "reflex:health:state_bridge",
        log_level: str = "INFO",
    ) -> None:
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.log = logging.getLogger("StateBridge")
        if not self.log.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("[%(asctime)s] %(name)s %(levelname)s: %(message)s")
            h.setFormatter(fmt)
            self.log.addHandler(h)
        self.log.setLevel(level)

        # Endpoints from config.py
        self.redis_url = get_redis_url()
        self.pg_dsn = get_pg_dsn()

        # Redis
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        self.pub = self.r

        # Postgres
        self.pg_conn = None  # type: ignore
        self.pg_cur = None   # type: ignore

        # Schema / channels
        self.db_notify_channel = db_notify_channel
        self.tab = table
        self.col_sym = col_symbol
        self.col_state = col_state
        self.col_dnt = col_dnt

        self.in_eval = in_eval
        self.in_override = in_override
        self.in_chart = in_chart
        self.ctl_ticks = ctl_ticks
        self.ctl_quotes = ctl_quotes
        self.health_key = health_key

        # State maps
        self.state_by_source: Dict[str, Dict[str, str]] = {SRC_DB: {}, SRC_EVAL: {}, SRC_OVERRIDE: {}, SRC_CHART: {}}
        self.chart_ts: Dict[str, float] = {}

        self.eff_warm: Set[str] = set()
        self.eff_hot: Set[str] = set()

        self._stop_evt = threading.Event()
        self._push_evt = threading.Event()
        self._threads: List[threading.Thread] = []

        self.metrics = {"updates_in": 0, "db_boot_count": 0, "db_notify_in": 0, "push_out": 0, "chart_expired": 0}

    # ----- lifecycle -----
    def start(self) -> None:
        self._install_signals()
        self._start_pg()
        self._bootstrap_from_db()
        self._recompute_and_push(reason="bootstrap")
        self._start_redis_in_listeners()
        self._start_db_notify_listener()
        self._start_pusher()
        self._start_chart_ttl_expirer()
        self._start_health_publisher()
        self.log.info("StateSubscriptionBridge started.")
        try:
            while not self._stop_evt.is_set():
                time.sleep(0.5)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop_evt.set()
        for t in self._threads:
            t.join(timeout=5.0)
        try:
            if self.pg_cur:
                self.pg_cur.close()
            if self.pg_conn:
                self.pg_conn.close()
        except Exception:
            pass
        self.log.info("StateSubscriptionBridge stopped.")

    def _install_signals(self) -> None:
        def _sig(_s, _f):
            self.log.info("Signal received; stopping bridge.")
            self._stop_evt.set()
        try:
            signal.signal(signal.SIGINT, _sig)
            signal.signal(signal.SIGTERM, _sig)
        except Exception:
            pass

    # ----- Postgres -----
    def _start_pg(self) -> None:
        self.pg_conn = psycopg2.connect(self.pg_dsn)
        self.pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.pg_cur = self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.pg_cur.execute(f"LISTEN {self.db_notify_channel};")
        self.log.info("LISTEN on Postgres channel: %s", self.db_notify_channel)

    def _bootstrap_from_db(self) -> None:
        has_dnt = self._column_exists(self.tab, self.col_dnt)
        if has_dnt:
            sql = f"""
                SELECT {self.col_sym} AS symbol, {self.col_state} AS state
                FROM {self.tab}
                WHERE {self.col_state} IN ('WARM','HOT') AND COALESCE({self.col_dnt}, FALSE) = FALSE
            """
        else:
            sql = f"""
                SELECT {self.col_sym} AS symbol, {self.col_state} AS state
                FROM {self.tab}
                WHERE {self.col_state} IN ('WARM','HOT')
            """
        rows = self._pg_query(sql)
        base = {}
        for r in rows:
            sym = str(r["symbol"]).upper()
            st = str(r["state"]).upper()
            if st in VALID_STATES:
                base[sym] = st
        self.state_by_source[SRC_DB] = base
        self.metrics["db_boot_count"] = len(base)
        self.log.info("DB bootstrap: %d symbols (WARM/HOT).", len(base))

    def _column_exists(self, table: str, col: str) -> bool:
        try:
            sql = """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s
                LIMIT 1
            """
            cur = self.pg_conn.cursor()
            cur.execute(sql, (table, col))
            return cur.fetchone() is not None
        except Exception:
            self.log.debug("column_exists check failed; assuming column missing.", exc_info=True)
            return False

    def _pg_query(self, sql: str, args: Optional[Tuple] = None) -> List[Dict]:
        cur = self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, args or ())
        rows = cur.fetchall()
        cur.close()
        return rows

    def _start_db_notify_listener(self) -> None:
        t = threading.Thread(target=self._db_notify_loop, name="StateBridge-DBNotify", daemon=True)
        t.start()
        self._threads.append(t)

    def _db_notify_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self.pg_conn.poll()
                while self.pg_conn.notifies:
                    note = self.pg_conn.notifies.pop(0)
                    self.metrics["db_notify_in"] += 1
                    self._apply_db_notification(note.payload)
            except Exception:
                self.log.exception("DB notify loop error")
                time.sleep(1.0)
            time.sleep(0.2)

    def _apply_db_notification(self, payload: str) -> None:
        try:
            data = json.loads(payload)
            items = []
            if "batch" in data and isinstance(data["batch"], list):
                items = data["batch"]
            elif "symbol" in data and "state" in data:
                items = [data]
            else:
                self.log.warning("Unhandled DB notify payload: %s", payload)
                return
            for it in items:
                sym = str(it["symbol"]).upper()
                st = str(it["state"]).upper()
                if st in VALID_STATES:
                    self.state_by_source[SRC_DB][sym] = st
            self._schedule_push()
        except Exception:
            sym = payload.strip().upper()
            if sym:
                sql = f"SELECT {self.col_state} FROM {self.tab} WHERE {self.col_sym}=%s"
                try:
                    rows = self._pg_query(sql, (sym,))
                    if rows:
                        st = str(rows[0][self.col_state]).upper()
                        if st in VALID_STATES:
                            self.state_by_source[SRC_DB][sym] = st
                            self._schedule_push()
                except Exception:
                    self.log.exception("DB lookup failed for symbol from notify: %s", sym)

    # ----- Redis inputs (Evaluator / Override / Chart) -----
    def _start_redis_in_listeners(self) -> None:
        for name, chan in [(SRC_EVAL, self.in_eval), (SRC_OVERRIDE, self.in_override), (SRC_CHART, self.in_chart)]:
            t = threading.Thread(
                target=self._redis_in_loop,
                kwargs={"source": name, "channel": chan},
                name=f"StateBridge-RedisIn-{name}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

    def _redis_in_loop(self, source: str, channel: str) -> None:
        ps = self.r.pubsub()
        ps.subscribe(channel)
        self.log.info("Listening Redis source=%s channel=%s", source, channel)
        for msg in ps.listen():
            if self._stop_evt.is_set():
                break
            if msg.get("type") != "message":
                continue
            try:
                payload = json.loads(msg["data"])
                self._apply_source_payload(source, payload)
                self.metrics["updates_in"] += 1
            except Exception:
                self.log.exception("Invalid payload on %s: %s", channel, msg)

    def _apply_source_payload(self, source: str, payload: Dict) -> None:
        items: List[Dict] = []
        if "batch" in payload and isinstance(payload["batch"], list):
            items = payload["batch"]
        elif "symbol" in payload and "state" in payload:
            items = [payload]
        else:
            self.log.warning("Ignored payload for %s: %s", source, payload)
            return

        changed = False
        now = time.time()
        for it in items:
            sym = str(it.get("symbol", "")).upper().strip()
            st = str(it.get("state", "")).upper().strip()
            if not sym or st not in VALID_STATES:
                continue
            if source == SRC_CHART:
                self.chart_ts[sym] = now
                self.state_by_source[SRC_CHART][sym] = st
            else:
                self.state_by_source[source][sym] = st
            changed = True

        if changed:
            self._schedule_push()

    # ----- Debounced push -----
    def _start_pusher(self) -> None:
        t = threading.Thread(target=self._pusher_loop, name="StateBridge-Pusher", daemon=True)
        t.start()
        self._threads.append(t)

    def _schedule_push(self) -> None:
        self._push_evt.set()

    def _pusher_loop(self) -> None:
        while not self._stop_evt.is_set():
            if not self._push_evt.wait(timeout=0.25):
                continue
            time.sleep(DEBOUNCE_MS / 1000.0)
            self._push_evt.clear()
            try:
                self._recompute_and_push(reason="debounced-update")
            except Exception:
                self.log.exception("Recompute/push failed")

    def _recompute_and_push(self, *, reason: str) -> None:
        new_warm, new_hot = self._compute_effective()
        if new_warm != self.eff_warm or new_hot != self.eff_hot:
            self.eff_warm, self.eff_hot = new_warm, new_hot
            self._publish_control(new_hot, new_warm | new_hot)
            self.metrics["push_out"] += 1
            self.log.info(
                "Pushed subs (%s): HOT=%d, WARM+HOT=%d",
                reason, len(new_hot), len(new_warm | new_hot)
            )
        else:
            self.log.debug("No effective change (%s).", reason)

    def _compute_effective(self) -> Tuple[Set[str], Set[str]]:
        symbols: Set[str] = set()
        for srcmap in self.state_by_source.values():
            symbols.update(srcmap.keys())
        warm: Set[str] = set()
        hot: Set[str] = set()
        for sym in symbols:
            st = self._effective_state_for(sym)
            if st == "HOT":
                hot.add(sym)
            elif st == "WARM":
                warm.add(sym)
        return warm, hot

    def _effective_state_for(self, sym: str) -> str:
        now = time.time()
        for src in SOURCE_PRIORITY:
            st = self.state_by_source[src].get(sym)
            if st is None:
                continue
            if src == SRC_CHART:
                ts = self.chart_ts.get(sym, 0.0)
                if now - ts > CHART_TTL_SEC:
                    continue
            return st
        return "COLD"

    def _publish_control(self, hot: Set[str], warmhot: Set[str]) -> None:
        hot_list = sorted(list(hot))
        warmhot_list = sorted(list(warmhot))
        self.pub.publish(self.ctl_ticks, json.dumps({"op": "replace", "channel": "T", "symbols": hot_list}, separators=(",", ":")))
        self.pub.publish(self.ctl_quotes, json.dumps({"op": "replace", "channel": "Q", "symbols": warmhot_list}, separators=(",", ":")))

    # ----- Chart TTL -----
    def _start_chart_ttl_expirer(self) -> None:
        t = threading.Thread(target=self._chart_ttl_loop, name="StateBridge-ChartTTL", daemon=True)
        t.start()
        self._threads.append(t)

    def _chart_ttl_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                now = time.time()
                expired = [s for s, ts in list(self.chart_ts.items()) if now - ts > CHART_TTL_SEC]
                if expired:
                    for s in expired:
                        self.chart_ts.pop(s, None)
                        self.state_by_source[SRC_CHART].pop(s, None)
                    self.metrics["chart_expired"] += len(expired)
                    self._schedule_push()
            except Exception:
                self.log.debug("Chart TTL loop error", exc_info=True)
            time.sleep(1.0)

    # ----- Health -----
    def _start_health_publisher(self) -> None:
        t = threading.Thread(target=self._health_loop, name="StateBridge-Health", daemon=True)
        t.start()
        self._threads.append(t)

    def _health_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                payload = {
                    "proc": "state_bridge",
                    "sizes": {
                        "db": len(self.state_by_source[SRC_DB]),
                        "evaluator": len(self.state_by_source[SRC_EVAL]),
                        "override": len(self.state_by_source[SRC_OVERRIDE]),
                        "chart": len(self.state_by_source[SRC_CHART]),
                        "eff_hot": len(self.eff_hot),
                        "eff_warm": len(self.eff_warm),
                    },
                    "metrics": self.metrics,
                    "ts": int(time.time() * 1000),
                }
                self.r.set(self.health_key, json.dumps(payload, separators=(",", ":")))
            except Exception:
                self.log.debug("Health publish failed", exc_info=True)
            time.sleep(2.0)


if __name__ == "__main__":
    StateSubscriptionBridge().start()
