# dbmanager/init_db.py
# -----------------------------------------------------------------------------
# Production-grade database initializer for Reflex / TimeData
# - Exports drop_and_create_database() for Flask app import compatibility
# - Standardizes on `timestamp` as the time column everywhere
# - Permanently adds `day` DATE (generated, STORED) using America/New_York
# - Creates TimescaleDB hypertables, CAGGs, and pragmatic indexes
# - Provides verify & reset helpers
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import sys
import argparse
from dataclasses import dataclass
from typing import Optional

# ---- Path wiring to import from ../common ------------------------------------
CURRENT_FILE = os.path.abspath(__file__)
DBMANAGER_DIR = os.path.dirname(CURRENT_FILE)
PROJECT_ROOT = os.path.abspath(os.path.join(DBMANAGER_DIR, ".."))
COMMON_PATH = os.path.join(PROJECT_ROOT, "common")
if COMMON_PATH not in sys.path:
    sys.path.append(COMMON_PATH)

# ---- Logging -----------------------------------------------------------------
try:
    from app_logging import setup_logger  # common/app_logging.py
except Exception:
    import logging

    def setup_logger(name: str, level: str = "INFO"):
        logger = logging.getLogger(name)
        if not logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
            logger.addHandler(h)
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        return logger

log = setup_logger("dbmanager-init", level="INFO")

# ---- DB params ---------------------------------------------------------------
@dataclass
class DbParams:
    dbname: str
    user: str
    password: str
    host: str = "127.0.0.1"
    port: int = 5432


def _load_db_params() -> DbParams:
    """
    Prefer common.config.DB_PARAMS; fall back to env vars if not present.
    """
    try:
        from config import DB_PARAMS  # common/config.py
        if isinstance(DB_PARAMS, dict):
            return DbParams(
                dbname=DB_PARAMS["dbname"],
                user=DB_PARAMS["user"],
                password=DB_PARAMS["password"],
                host=DB_PARAMS.get("host", "127.0.0.1"),
                port=int(DB_PARAMS.get("port", 5432)),
            )
        return DbParams(
            dbname=DB_PARAMS.dbname,
            user=DB_PARAMS.user,
            password=DB_PARAMS.password,
            host=getattr(DB_PARAMS, "host", "127.0.0.1"),
            port=int(getattr(DB_PARAMS, "port", 5432)),
        )
    except Exception:
        return DbParams(
            dbname=os.environ.get("PGDATABASE", "timedata"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
            host=os.environ.get("PGHOST", "127.0.0.1"),
            port=int(os.environ.get("PGPORT", 5432)),
        )


DB = _load_db_params()

# ---- Psycopg3 ----------------------------------------------------------------
try:
    import psycopg
    from psycopg import sql
except ImportError as e:
    raise ImportError("psycopg (psycopg3) is required. pip install psycopg[binary]") from e

MARKET_TZ = "America/New_York"  # canonical trading-day timezone


# =============================================================================
# Public entry points (keep these names stable for the Flask app)
# =============================================================================
def drop_and_create_database() -> None:
    """
    PUBLIC API used by dbmanager.app:
    Drop the existing database (if any), recreate it, and apply the schema.
    """
    _drop_and_create_database(DB)
    _ensure_schema()


def reset_and_init_db(*_args, **_kwargs) -> None:
    """
    Back-compat alias some code may import.
    """
    drop_and_create_database()


def init_database(reset: bool = False, verify_only: bool = False) -> None:
    """
    Ensure schema idempotently. If reset=True, drop & recreate DB first.
    If verify_only=True, run verification checks and exit.
    """
    if verify_only:
        with _connect(DB.dbname) as conn:
            _verify_schema(conn)
        log.info("Verification complete.")
        return

    if reset:
        drop_and_create_database()
    else:
        _ensure_schema()


# =============================================================================
# Internal helpers
# =============================================================================
def _ensure_schema() -> None:
    with _connect(DB.dbname) as conn:
        conn.autocommit = True
        _ensure_extensions(conn)
        _create_base_schema(conn)  # tables + hypertables
        _ensure_day_columns_and_indexes(conn)  # permanent market-day column
        _ensure_caggs(conn)  # continuous aggregates
        _ensure_policies(conn)  # compression (best-effort)
        _verify_schema(conn)  # fail fast if something is off
    log.info("✅ Schema ensured.")


def _connect(dbname: str) -> "psycopg.Connection":
    return psycopg.connect(
        dbname=dbname,
        user=DB.user,
        password=DB.password,
        host=DB.host,
        port=DB.port,
    )


def _drop_and_create_database(params: DbParams) -> None:
    log.info(f"♻️  Dropping and recreating database '{params.dbname}'...")
    with _connect("postgres") as admin:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid();
                """,
                (params.dbname,),
            )
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (params.dbname,))
            if cur.fetchone():
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(params.dbname)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(params.dbname)))
    log.info("🆕 Database created.")


# -----------------------------------------------------------------------------
# Schema: extensions, tables, hypertables, indexes
# -----------------------------------------------------------------------------
def _ensure_extensions(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    log.info("Extensions ensured (timescaledb, uuid-ossp, pgcrypto).")


def _create_base_schema(conn) -> None:
    """
    Create base tables and hypertables. The time column is `timestamp` everywhere.
    """
    with conn.cursor() as cur:
        # ticks
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS tick_data (
            symbol           TEXT        NOT NULL,
            timestamp        TIMESTAMPTZ NOT NULL,
            sip_timestamp    BIGINT      NOT NULL,
            price            NUMERIC(18,6) NOT NULL,
            size             INTEGER,
            exchange         TEXT,
            conditions       TEXT[],
            tape             TEXT,
            participant_id   TEXT,
            is_trade_through BOOLEAN DEFAULT FALSE,
            UNIQUE (symbol, timestamp, sip_timestamp)
        );
        """
        )
        cur.execute(
            """
        SELECT create_hypertable('tick_data', 'timestamp',
                chunk_time_interval => interval '1 day',
                if_not_exists => TRUE);
        """
        )

        # quotes
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS quote_data (
            symbol     TEXT        NOT NULL,
            timestamp  TIMESTAMPTZ NOT NULL,
            bid_price  NUMERIC(18,6),
            bid_size   INTEGER,
            ask_price  NUMERIC(18,6),
            ask_size   INTEGER,
            exchange   TEXT,
            tape       TEXT,
            PRIMARY KEY (symbol, timestamp)
        );
        """
        )
        cur.execute(
            """
        SELECT create_hypertable('quote_data', 'timestamp',
                chunk_time_interval => interval '1 day',
                if_not_exists => TRUE);
        """
        )

        # minute bars
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS minute_bars (
            symbol     TEXT        NOT NULL,
            timestamp  TIMESTAMPTZ NOT NULL,
            open       NUMERIC(18,6),
            high       NUMERIC(18,6),
            low        NUMERIC(18,6),
            close      NUMERIC(18,6),
            volume     BIGINT,
            PRIMARY KEY (symbol, timestamp)
        );
        """
        )
        cur.execute(
            """
        SELECT create_hypertable('minute_bars', 'timestamp',
                chunk_time_interval => interval '1 day',
                if_not_exists => TRUE);
        """
        )

        # daily bars
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS daily_bars (
            symbol     TEXT        NOT NULL,
            timestamp  TIMESTAMPTZ NOT NULL,
            open       NUMERIC(18,6),
            high       NUMERIC(18,6),
            low        NUMERIC(18,6),
            close      NUMERIC(18,6),
            volume     BIGINT,
            PRIMARY KEY (symbol, timestamp)
        );
        """
        )
        cur.execute(
            """
        SELECT create_hypertable('daily_bars', 'timestamp',
                chunk_time_interval => interval '7 days',
                if_not_exists => TRUE);
        """
        )

        # fundamentals
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS fundamental_data (
            symbol                 TEXT PRIMARY KEY,
            company                TEXT,
            sector                 TEXT,
            industry               TEXT,
            country                TEXT,
            exchange               TEXT,
            market_cap             NUMERIC(18,2),
            pe_ratio               NUMERIC(10,2),
            shares_float           BIGINT,
            float_percent          NUMERIC(10,2),
            insider_transactions   NUMERIC(10,2),
            short_float            NUMERIC(10,2),
            average_true_range     NUMERIC(10,4),
            last_updated           TIMESTAMP DEFAULT NOW()
        );
        """
        )
        # convenient filters
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fundamental_symbol ON fundamental_data(symbol);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sector ON fundamental_data(sector);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_industry ON fundamental_data(industry);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_country ON fundamental_data(country);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange ON fundamental_data(exchange);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_cap ON fundamental_data(market_cap);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_ratio ON fundamental_data(pe_ratio);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shares_float ON fundamental_data(shares_float);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_float_percent ON fundamental_data(float_percent);")

        # symbol metadata
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS symbol_metadata (
            symbol       TEXT PRIMARY KEY,
            mode         TEXT NOT NULL,
            filters      TEXT[] DEFAULT ARRAY[]::TEXT[],
            last_updated TIMESTAMP DEFAULT NOW()
        );
        """
        )

        # sessions (useful for backfills)
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS ingest_sessions (
            session_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            start_time  TIMESTAMPTZ NOT NULL,
            source      TEXT,
            notes       TEXT
        );
        """
        )

        # evaluator flags
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS evaluator_flags (
            id         BIGSERIAL PRIMARY KEY,
            symbol     TEXT NOT NULL,
            timestamp  TIMESTAMPTZ NOT NULL,
            flag_type  TEXT,
            confidence NUMERIC(5,2),
            metadata   JSONB
        );
        """
        )

        # optional view; safe to recreate
        cur.execute(
            """
        DROP VIEW IF EXISTS symbol_profile_view;
        CREATE VIEW symbol_profile_view AS
        SELECT
            f.symbol, f.company, f.sector, f.industry, f.country, f.exchange,
            f.market_cap, f.shares_float, f.float_percent, f.pe_ratio, f.average_true_range,
            f.last_updated AS fundamentals_updated,
            m.mode, m.filters, m.last_updated AS metadata_updated
        FROM fundamental_data f
        LEFT JOIN symbol_metadata m ON f.symbol = m.symbol;
        """
        )

    log.info("Base tables and hypertables ensured.")


def _ensure_day_columns_and_indexes(conn) -> None:
    """
    Permanently ensure timezone-safe `day` (generated, STORED) and pragmatic indexes.
    """
    with conn.cursor() as cur:
        for tbl in ("minute_bars", "daily_bars", "quote_data", "tick_data"):
            cur.execute(
                f"""
                ALTER TABLE {tbl}
                ADD COLUMN IF NOT EXISTS day DATE
                GENERATED ALWAYS AS ((timestamp AT TIME ZONE '{MARKET_TZ}')::date) STORED;
                """
            )
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_day ON {tbl}(day);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{tbl}_symbol_day ON {tbl}(symbol, day);")
            if tbl in ("minute_bars", "tick_data", "quote_data"):
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS ix_{tbl}_symbol_ts ON {tbl}(symbol, timestamp DESC);"
                )
    log.info("Permanent `day` columns and indexes ensured on all time-series tables.")


def _ensure_caggs(conn) -> None:
    with conn.cursor() as cur:
        # 5m
        cur.execute(
            """
        CREATE MATERIALIZED VIEW IF NOT EXISTS agg_5m_bars
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('5 minutes', timestamp) AS bucket,
            first(open, timestamp)::NUMERIC(18,6)  AS open,
            max(high)::NUMERIC(18,6)              AS high,
            min(low)::NUMERIC(18,6)               AS low,
            last(close, timestamp)::NUMERIC(18,6) AS close,
            sum(volume)                           AS volume
        FROM minute_bars
        GROUP BY symbol, bucket
        WITH NO DATA;
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_agg_5m_symbol_bucket ON agg_5m_bars(symbol, bucket);")

        # 15m
        cur.execute(
            """
        CREATE MATERIALIZED VIEW IF NOT EXISTS agg_15m_bars
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('15 minutes', timestamp) AS bucket,
            first(open, timestamp)::NUMERIC(18,6)  AS open,
            max(high)::NUMERIC(18,6)              AS high,
            min(low)::NUMERIC(18,6)               AS low,
            last(close, timestamp)::NUMERIC(18,6) AS close,
            sum(volume)                           AS volume
        FROM minute_bars
        GROUP BY symbol, bucket
        WITH NO DATA;
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_agg_15m_symbol_bucket ON agg_15m_bars(symbol, bucket);")

        # 1h
        cur.execute(
            """
        CREATE MATERIALIZED VIEW IF NOT EXISTS agg_1h_bars
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('1 hour', timestamp) AS bucket,
            first(open, timestamp)::NUMERIC(18,6)  AS open,
            max(high)::NUMERIC(18,6)              AS high,
            min(low)::NUMERIC(18,6)               AS low,
            last(close, timestamp)::NUMERIC(18,6) AS close,
            sum(volume)                           AS volume
        FROM minute_bars
        GROUP BY symbol, bucket
        WITH NO DATA;
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_agg_1h_symbol_bucket ON agg_1h_bars(symbol, bucket);")

        # 1d
        cur.execute(
            """
        CREATE MATERIALIZED VIEW IF NOT EXISTS agg_1d_bars
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('1 day', timestamp) AS bucket,
            first(open, timestamp)::NUMERIC(18,6)  AS open,
            max(high)::NUMERIC(18,6)              AS high,
            min(low)::NUMERIC(18,6)               AS low,
            last(close, timestamp)::NUMERIC(18,6) AS close,
            sum(volume)                           AS volume
        FROM minute_bars
        GROUP BY symbol, bucket
        WITH NO DATA;
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_agg_1d_symbol_bucket ON agg_1d_bars(symbol, bucket);")

        # (Optional) add a refresh policy for 5m (ignore if function missing)
        try:
            cur.execute(
                """
                SELECT add_continuous_aggregate_policy('agg_5m_bars',
                    start_offset => INTERVAL '2 days',
                    end_offset   => INTERVAL '1 minute',
                    schedule_interval => INTERVAL '5 minutes');
                """
            )
        except Exception:
            pass

    log.info("Continuous aggregates ensured.")


def _ensure_policies(conn) -> None:
    with conn.cursor() as cur:
        for tbl, horizon in (
            ("minute_bars", "7 days"),
            ("tick_data", "2 days"),
            ("quote_data", "3 days"),
            ("daily_bars", "180 days"),
        ):
            try:
                cur.execute(
                    f"""
                    ALTER TABLE {tbl}
                    SET (timescaledb.compress = true,
                         timescaledb.compress_orderby   = 'timestamp DESC',
                         timescaledb.compress_segmentby = 'symbol');
                    """
                )
                cur.execute(f"SELECT add_compression_policy('{tbl}', INTERVAL '{horizon}');")
            except Exception:
                pass
    log.info("Compression policies ensured (best-effort).")


def _verify_schema(conn) -> None:
    with conn.cursor() as cur:
        # Ensure no legacy 'ts' columns remain
        cur.execute(
            """
            SELECT table_name FROM information_schema.columns
            WHERE table_schema='public' AND column_name='ts';
            """
        )
        bad = [r[0] for r in cur.fetchall()]
        if bad:
            raise RuntimeError(f"Legacy 'ts' column found in tables: {bad}. Must be 'timestamp'.")

        # Ensure 'day' exists and is generated ALWAYS
        for tbl in ("minute_bars", "daily_bars", "quote_data", "tick_data"):
            cur.execute(
                """
                SELECT is_generated, generation_expression
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s AND column_name='day';
                """,
                (tbl,),
            )
            row = cur.fetchone()
            if not row or row[0] != "ALWAYS":
                raise RuntimeError(f"Table {tbl} missing generated ALWAYS 'day' column.")

        # Ensure PK/UNIQUES exist on core tables
        cur.execute(
            """
            SELECT conrelid::regclass::text, conname
            FROM pg_constraint
            WHERE contype IN ('p','u') AND conrelid::regclass::text IN
                  ('minute_bars','daily_bars','tick_data','quote_data');
            """
        )
        cons = cur.fetchall()
        if not cons:
            raise RuntimeError("Expected PK/UNIQUE constraints are missing on core tables.")

    log.info("Schema verification passed.")


# =============================================================================
# CLI
# =============================================================================
def main(argv: Optional[list[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Initialize or reset the TimeData database.")
    p.add_argument("--reset", action="store_true", help="Drop and recreate the database.")
    p.add_argument("--verify-only", action="store_true", help="Only run verification checks.")
    args = p.parse_args(argv)
    init_database(reset=args.reset, verify_only=args.verify_only)


if __name__ == "__main__":
    main()