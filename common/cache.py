# common/cache.py
from __future__ import annotations
import os, time, logging
from typing import Optional

try:
    import redis
except ImportError:
    redis = None

_LOG = logging.getLogger("cache")
if not _LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(name)s: %(message)s"))
    _LOG.addHandler(h)
_LOG.setLevel(logging.INFO)

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)

def _build_url() -> str:
    for key in ("CACHE_URL", "GARNET_URL", "REDIS_URL"):
        url = _env(key)
        if url:
            return url
    host = _env("CACHE_HOST") or _env("GARNET_HOST") or _env("REDIS_HOST") or "127.0.0.1"
    port = int(_env("CACHE_PORT") or _env("GARNET_PORT") or _env("REDIS_PORT") or "6379")
    db = int(_env("CACHE_DB") or _env("GARNET_DB") or _env("REDIS_DB") or "0")
    password = _env("CACHE_PASSWORD") or _env("GARNET_PASSWORD") or _env("REDIS_PASSWORD")
    auth = f":{password}@" if password else ""
    return f"redis://{auth}{host}:{port}/{db}"

def get_cache(**kwargs):
    if redis is None:
        raise RuntimeError("redis package not installed; pip install redis")
    url = _build_url()
    return redis.Redis.from_url(url, decode_responses=True, **kwargs)

def ensure_cache_available(timeout_sec: int = 5, interval: float = 0.25) -> None:
    client = get_cache(socket_connect_timeout=timeout_sec)
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            client.ping()
            _LOG.info("Cache is available")
            return
        except Exception as ex:
            last_err = ex
            time.sleep(interval)
    raise RuntimeError(f"Cache not reachable: {last_err}")
