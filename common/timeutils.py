# common/timeutils.py
from __future__ import annotations
from datetime import datetime, time, timedelta, timezone
from typing import Tuple

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    _ET = ZoneInfo("America/New_York")
except Exception:
    # Fallback: treat ET as UTC-5/UTC-4 approximation; avoids extra deps on Windows
    class _FixedTZ(timezone): pass  # type: ignore
    _ET = timezone(timedelta(hours=-5))

UTC = timezone.utc

MARKET_OPEN_ET = time(9, 30)
MARKET_CLOSE_ET = time(16, 0)

def now_utc() -> datetime:
    return datetime.now(tz=UTC)

def to_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)

def to_et(dt: datetime) -> datetime:
    if dt.tzinfo:
        return dt.astimezone(_ET)
    return dt.replace(tzinfo=UTC).astimezone(_ET)

def market_session_bounds_et(dt: datetime) -> Tuple[datetime, datetime]:
    d_et = to_et(dt).date()
    start = datetime.combine(d_et, MARKET_OPEN_ET, tzinfo=_ET)
    end = datetime.combine(d_et, MARKET_CLOSE_ET, tzinfo=_ET)
    return start, end

def is_market_open(dt: datetime) -> bool:
    s, e = market_session_bounds_et(dt)
    d = to_et(dt)
    return s <= d <= e

def clamp_to_session(dt: datetime) -> datetime:
    s, e = market_session_bounds_et(dt)
    d = to_et(dt)
    if d < s: return s
    if d > e: return e
    return d
