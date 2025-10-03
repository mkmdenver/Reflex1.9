# common/app_logging.py
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import json
from datetime import datetime
from typing import Optional, Iterable

__all__ = [
    "get_logger",
    "setup_logger",
    "configure_root_logging",
    "quiet_third_party",
    "LOG_DIR",
]

# ---------- Configuration defaults ----------
# Project root = parent of this file's directory (common/)
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Default log directory (override with REFLEX_LOG_DIR env var)
LOG_DIR = os.getenv("REFLEX_LOG_DIR", os.path.join(_PROJECT_ROOT, "logs"))

# Marker attribute to prevent duplicate handlers
_REFLEX_HANDLER_FLAG = "_reflex_handler"


# ---------- Utilities ----------

def _level_to_int(level: str | int | None, default: int = logging.INFO) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        lvl = getattr(logging, level.upper(), None)
        if isinstance(lvl, int):
            return lvl
    return default


class JsonFormatter(logging.Formatter):
    """
    Minimal, safe JSON formatter. Serializes core record fields and extras.
    Non-serializable values are converted to strings.
    """
    def __init__(self, include_time: bool = True, time_key: str = "time",
                 timefmt: str = "%Y-%m-%dT%H:%M:%S.%fZ"):
        super().__init__()
        self.include_time = include_time
        self.time_key = time_key
        self.timefmt = timefmt

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if self.include_time:
            payload[self.time_key] = datetime.utcnow().strftime(self.timefmt)

        payload.update({
            "filename": record.filename,
            "lineno": record.lineno,
            "func": record.funcName,
            "process": record.process,
            "thread": record.thread,
        })

        # Merge extra fields if any
        standard = set(vars(logging.makeLogRecord({})).keys())
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in standard:
                continue
            try:
                json.dumps({key: value})
                payload[key] = value
            except TypeError:
                payload[key] = str(value)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def _ensure_log_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _have_reflex_handler(logger: logging.Logger, kind: str) -> bool:
    for h in logger.handlers:
        if getattr(h, _REFLEX_HANDLER_FLAG, None) == kind:
            return True
    return False


def _install_stdout_handler(
    logger: logging.Logger,
    formatter: logging.Formatter,
) -> None:
    if _have_reflex_handler(logger, "stdout"):
        return
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    setattr(sh, _REFLEX_HANDLER_FLAG, "stdout")
    logger.addHandler(sh)


def _install_rotating_file_handler(
    logger: logging.Logger,
    log_file: str,
    formatter: logging.Formatter,
    max_bytes: int,
    backup_count: int,
) -> None:
    if _have_reflex_handler(logger, f"file:{log_file}"):
        return

    _ensure_log_dir(os.path.dirname(log_file) or ".")
    fh = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setFormatter(formatter)
    setattr(fh, _REFLEX_HANDLER_FLAG, f"file:{log_file}")
    logger.addHandler(fh)


# ---------- Public API ----------

def get_logger(
    name: str,
    level: str | int = "INFO",
    *,
    to_stdout: bool = True,
    to_file: Optional[str | bool] = None,
    fmt: Optional[str] = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    json_logs: bool = False,
    propagate: bool = False,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 10,
) -> logging.Logger:
    """
    Create or retrieve a configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(_level_to_int(level))
    logger.propagate = bool(propagate)

    if json_logs:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(fmt or "%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    if to_stdout:
        _install_stdout_handler(logger, formatter)

    if to_file:
        if isinstance(to_file, str):
            log_path = to_file
        else:
            log_path = os.path.join(LOG_DIR, f"{name}.log")
        _install_rotating_file_handler(logger, log_path, formatter, max_bytes, backup_count)

    return logger


def setup_logger(
    name: str,
    level: str | int = "INFO",
    **kwargs,
) -> logging.Logger:
    """
    Back-compat wrapper used by legacy code paths.
    Accepts the same signature as older callers (notably the 'level' kwarg).
    Delegates to get_logger with sane defaults.
    """
    return get_logger(name, level=level, **kwargs)


def configure_root_logging(
    level: str | int = "INFO",
    *,
    to_stdout: bool = True,
    to_file: Optional[str | bool] = None,
    fmt: Optional[str] = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    json_logs: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 10,
) -> logging.Logger:
    """
    Configure the root logger. Useful for scripts/tools.
    """
    root = logging.getLogger()
    root.setLevel(_level_to_int(level))

    if json_logs:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(fmt or "%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    if to_stdout:
        _install_stdout_handler(root, formatter)

    if to_file:
        if isinstance(to_file, str):
            log_path = to_file
        else:
            log_path = os.path.join(LOG_DIR, "root.log")
        _install_rotating_file_handler(root, log_path, formatter, max_bytes, backup_count)

    return root


def quiet_third_party(
    names: Iterable[str] = ("werkzeug", "urllib3", "psycopg2", "asyncio"),
    level: str | int = "WARNING",
) -> None:
    """
    Reduce verbosity from common noisy libraries.
    """
    lvl = _level_to_int(level)
    for n in names:
        try:
            logging.getLogger(n).setLevel(lvl)
        except Exception:
            pass
