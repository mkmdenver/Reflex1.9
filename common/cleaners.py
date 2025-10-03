# common/cleaners.py
from __future__ import annotations

def clean_float(v, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        fv = float(v)
        if fv != fv:  # NaN
            return default
        return fv
    except Exception:
        return default
def clean_int(v, default: int | None = None) -> int | None:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default

def valid_bar(open_: float, high: float, low: float, close: float) -> bool:
    if any(x is None for x in (open_, high, low, close)):
        return False
    if high < low: return False
    if not (low <= open_ <= high): return False
    if not (low <= close <= high): return False
    return True
