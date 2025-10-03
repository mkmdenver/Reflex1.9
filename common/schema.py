# common/schema.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# Canonical table names
T_TICKS = "ticks"
T_QUOTES = "quotes"
T_MINUTE = "minute_bars"
T_DAILY = "daily_bars"
T_SYMBOLS = "symbols"
T_FUNDS = "fundamentals"

# Records (Python-side)
@dataclass(slots=True)
class Tick:
    symbol: str
    ts_utc: datetime
    price: float
    size: int
    exchange: Optional[int] = None

@dataclass(slots=True)
class Quote:
    symbol: str
    ts_utc: datetime
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    exchange: Optional[int] = None

@dataclass(slots=True)
class MinuteBar:
    symbol: str
    ts_utc: datetime  # start of minute
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    trades: Optional[int] = None

@dataclass(slots=True)
class DailyBar:
    symbol: str
    session_date: datetime  # date at 00:00 UTC
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass(slots=True)
class Fundamentals:
    symbol: str
    ts_utc: datetime
    shares_out: Optional[float] = None
    float_shares: Optional[float] = None
    pe: Optional[float] = None
    market_cap: Optional[float] = None

CREATE_EXTENSION_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
"""

CREATE_TABLES_SQL = f"""
CREATE TABLE IF NOT EXISTS {T_SYMBOLS}(
  symbol TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'COLD',
  flags JSONB NOT NULL DEFAULT '{{}}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {T_TICKS}(
  symbol TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  size INTEGER NOT NULL,
  exchange INTEGER NULL,
  PRIMARY KEY(symbol, ts_utc)
);

CREATE TABLE IF NOT EXISTS {T_QUOTES}(
  symbol TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  bid DOUBLE PRECISION NOT NULL,
  ask DOUBLE PRECISION NOT NULL,
  bid_size INTEGER NOT NULL,
  ask_size INTEGER NOT NULL,
  exchange INTEGER NULL,
  PRIMARY KEY(symbol, ts_utc)
);

CREATE TABLE IF NOT EXISTS {T_MINUTE}(
  symbol TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  vwap DOUBLE PRECISION NULL,
  trades BIGINT NULL,
  PRIMARY KEY(symbol, ts_utc)
);

CREATE TABLE IF NOT EXISTS {T_DAILY}(
  symbol TEXT NOT NULL,
  session_date TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  PRIMARY KEY(symbol, session_date)
);

CREATE TABLE IF NOT EXISTS {T_FUNDS}(
  symbol TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  shares_out DOUBLE PRECISION NULL,
  float_shares DOUBLE PRECISION NULL,
  pe DOUBLE PRECISION NULL,
  market_cap DOUBLE PRECISION NULL,
  PRIMARY KEY(symbol, ts_utc)
);
"""

CREATE_HYPERTABLES_SQL = f"""
SELECT create_hypertable('{T_TICKS}', 'ts_utc', if_not_exists => TRUE);
SELECT create_hypertable('{T_QUOTES}', 'ts_utc', if_not_exists => TRUE);
SELECT create_hypertable('{T_MINUTE}', 'ts_utc', if_not_exists => TRUE);
SELECT create_hypertable('{T_DAILY}', 'session_date', if_not_exists => TRUE);
SELECT create_hypertable('{T_FUNDS}', 'ts_utc', if_not_exists => TRUE);
"""

INDEXES_SQL = f"""
CREATE INDEX IF NOT EXISTS idx_{T_TICKS}_symbol_ts ON {T_TICKS}(symbol, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_{T_QUOTES}_symbol_ts ON {T_QUOTES}(symbol, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_{T_MINUTE}_symbol_ts ON {T_MINUTE}(symbol, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_{T_DAILY}_symbol_dt ON {T_DAILY}(symbol, session_date DESC);
CREATE INDEX IF NOT EXISTS idx_{T_FUNDS}_symbol_ts ON {T_FUNDS}(symbol, ts_utc DESC);
"""
