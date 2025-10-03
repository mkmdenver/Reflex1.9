# common/fundamentals.py
from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any
from .dbutils import bulk_upsert
from .schema import T_FUNDS, Fundamentals
from .common import ExternalApiError
try:
    from polygon_api import rest as poly_rest
except Exception:
    poly_rest = None

def _norm_float(v) -> float | None:
    try:
        if v is None: return None
        fv = float(v)
        if fv != fv:  # NaN
            return None
        return fv
    except Exception:
        return None

def fetch_and_store_fundamentals(symbols: List[str], asof_utc: datetime, api_key: str, rest_base: str) -> int:
    """Fetch fundamentals via polygon_api.rest and upsert into DB."""
    if not poly_rest:
        raise ExternalApiError("polygon_api.rest not available.")
    rows: list[dict[str, Any]] = []
    for sym in symbols:
        f = poly_rest.get_fundamentals(api_key=api_key, base_url=rest_base, symbol=sym)
        rows.append(dict(
            symbol=sym,
            ts_utc=asof_utc,
            shares_out=_norm_float(f.get("shares_outstanding")),
            float_shares=_norm_float(f.get("float")),
            pe=_norm_float(f.get("pe_ratio")),
            market_cap=_norm_float(f.get("market_cap")),
        ))
    return bulk_upsert(T_FUNDS, rows, conflict_cols=["symbol","ts_utc"],
                       update_cols=["shares_out","float_shares","pe","market_cap"])
