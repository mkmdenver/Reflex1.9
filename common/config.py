# common/config.py
from __future__ import annotations
from dataclasses import dataclass
import os
from pathlib import Path

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Paths ---
ROOT_DIR = Path(os.getenv("REFLEX_ROOT", Path(__file__).resolve().parents[1]))
LOG_DIR  = Path(os.getenv("REFLEX_LOG_DIR", ROOT_DIR / "logs"))
DATA_DIR = Path(os.getenv("REFLEX_DATA_DIR", ROOT_DIR / "data"))
for p in (LOG_DIR, DATA_DIR):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

# --- Environment ---
APP_ENV = os.getenv("APP_ENV", "dev").strip().lower()
DEBUG   = os.getenv("REFLEX_DEBUG", "0") in ("1", "true", "True")
TZ      = os.getenv("REFLEX_TZ", "America/New_York")

# --- Typed Config Blocks ---
@dataclass(frozen=True)
class DBParams:
    host: str = os.getenv("PGHOST", "127.0.0.1")
    port: int = int(os.getenv("PGPORT", "5432"))
    dbname: str = "stock_data"
    user: str = os.getenv("PGUSER", "postgres")
    password: str = "4Asp44!"
    sslmode: str = os.getenv("PGSSLMODE", "disable")  # or 'require'

@dataclass(frozen=True)
class RedisParams:
    host: str = os.getenv("REDIS_HOST", "127.0.0.1")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    password: str | None = os.getenv("REDIS_PASSWORD") or None
    scheme: str = os.getenv("REDIS_SCHEME", "redis")  # 'redis' or 'rediss'

@dataclass(frozen=True)
class PolygonParams:
    api_key: str = os.getenv("POLYGON_API_KEY", "QiJFJRvmCaea9OGVfp6n2IyYpnHF3qFN")
    rest_base: str = os.getenv("POLYGON_REST_BASE", "https://api.polygon.io")
    ws_base: str = os.getenv("POLYGON_WS_BASE", "wss://socket.polygon.io/stocks")

# Canonical singletons
DB_PARAMS    = DBParams()
REDIS_PARAMS = RedisParams()
POLY_PARAMS  = PolygonParams()

# Exposed helpers
def get_db_params() -> DBParams: return DB_PARAMS
def get_redis_params() -> RedisParams: return REDIS_PARAMS
def get_polygon_params() -> PolygonParams: return POLY_PARAMS

# --- Aggregated runtime config used by modules expecting 'Config' ---
class Config:
    # Services
    DATAHUB_HOST: str = os.getenv("DATAHUB_HOST", "127.0.0.1")
    DATAHUB_PORT: int = int(os.getenv("DATAHUB_PORT", "5001"))

    # Alpaca (paper/live controlled by base URL)
    ALPACA_BASE_URL: str = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
    ALPACA_API_KEY: str = os.getenv("ALPACA_API_KEY", "")
    ALPACA_API_SECRET: str = os.getenv("ALPACA_API_SECRET", "")

    # Environment / paths
    APP_ENV: str  = APP_ENV
    DEBUG: bool   = DEBUG
    LOG_DIR: Path = LOG_DIR
    DATA_DIR: Path= DATA_DIR
    TZ: str       = TZ
