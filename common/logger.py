# common/logger.py
# Keep both names so callers can import either style.
from .app_logging import get_logger

# Back-compat alias
setup_logger = get_logger

__all__ = ["get_logger", "setup_logger"]
