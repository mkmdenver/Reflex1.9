# common/creds.py
from __future__ import annotations
import os
from .config import DB_PARAMS, REDIS_PARAMS, POLY_PARAMS

def get_pg_dsn() -> str:
    """
    Return a PostgreSQL DSN string. Respects DATABASE_URL first, then DB_PARAMS.
    Example format: postgresql://user:pass@host:port/dbname?sslmode=require
    """
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit

    user = DB_PARAMS.user or ""
    pwd  = DB_PARAMS.password or ""
    host = DB_PARAMS.host or "127.0.0.1"
    port = str(DB_PARAMS.port or 5432)
    db   = DB_PARAMS.dbname or "TimeData"
    ssl  = (DB_PARAMS.sslmode or "disable").lower()

    auth = user
    if pwd:
        auth += ":" + pwd
    auth += "@"

    dsn = "postgresql://" + auth + host + ":" + port + "/" + db
    if ssl and ssl != "disable":
        dsn += "?sslmode=" + ssl
    return dsn

def get_redis_url() -> str:
    """
    Resolve a Redis/Garnet connection URL. Prefers REDIS_URL/GARNET_URL envs.
    Falls back to REDIS_PARAMS.
    """
    url = os.getenv("REDIS_URL") or os.getenv("GARNET_URL")
    if url:
        return url

    host = REDIS_PARAMS.host or "127.0.0.1"
    port = str(REDIS_PARAMS.port or 6379)
    db   = str(REDIS_PARAMS.db or 0)
    pwd  = REDIS_PARAMS.password or ""
    scheme = (REDIS_PARAMS.scheme or "redis").strip() or "redis"

    if pwd:
        return scheme + "://:" + pwd + "@" + host + ":" + port + "/" + db
    return scheme + "://" + host + ":" + port + "/" + db

def get_polygon_key() -> str:
    """Primary accessor used across modules."""
    return POLY_PARAMS.api_key or ""

# Back-compat alias used by stream processes
def get_polygon_api_key() -> str:
    return get_polygon_key()

__all__ = [
    "get_pg_dsn",
    "get_redis_url",
    "get_polygon_key",
    "get_polygon_api_key",
]
