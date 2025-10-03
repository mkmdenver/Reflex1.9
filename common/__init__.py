# common/__init__.py
"""
Lightweight, import-safe package facade for Reflex 'common'.
This file avoids importing heavy or non-existent symbols so that
`from common.creds import ...` works in all processes.
"""

# Export the config helpers that exist today
from .config import (
    DB_PARAMS, REDIS_PARAMS, POLY_PARAMS,
    get_db_params, get_redis_params, get_polygon_params,
    Config,
)

# Logger surface: keep both names for back-compat
from .logger import setup_logger as get_logger, setup_logger

# Provide a harmless no-op config initializer to satisfy older imports
def configure_app_logging(name: str = "Reflex"):
    """Back-compat shim; calling this ensures logger singleton is initialized."""
    _ = get_logger(name)

# Back-compat shims: some legacy modules import Settings / load_settings.
# We map them to the current Config surface.
class Settings:
    """Compatibility wrapper that exposes selected attributes from Config."""
    def __init__(self) -> None:
        # Services
        self.DATAHUB_HOST = Config.DATAHUB_HOST
        self.DATAHUB_PORT = Config.DATAHUB_PORT
        # Alpaca
        self.ALPACA_BASE_URL = Config.ALPACA_BASE_URL
        self.ALPACA_API_KEY = Config.ALPACA_API_KEY
        self.ALPACA_API_SECRET = Config.ALPACA_API_SECRET

def load_settings() -> Settings:
    """Return a Settings instance built from environment via Config."""
    return Settings()

__all__ = [
    # config
    "DB_PARAMS", "REDIS_PARAMS", "POLY_PARAMS",
    "get_db_params", "get_redis_params", "get_polygon_params",
    "Config",
    # logging
    "get_logger", "setup_logger", "configure_app_logging",
    # legacy
    "Settings", "load_settings",
]
