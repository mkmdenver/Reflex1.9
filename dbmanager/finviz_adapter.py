# dbmanager/finviz_adapter.py
from __future__ import annotations

import re
import time
from typing import Dict, Iterable, List, Optional, Tuple

# ---- Dependencies (pin in requirements.txt for production) ----
# pandas>=2.0
try:
    import pandas as pd
except Exception:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

# finvizfinance>=0.16.0
try:
    from finvizfinance.screener.overview import Overview  # type: ignore
    from finvizfinance.screener.ownership import Ownership  # type: ignore
except Exception:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "finvizfinance"])
    from finvizfinance.screener.overview import Overview  # type: ignore
    from finvizfinance.screener.ownership import Ownership  # type: ignore

# Logging
try:
    from common.app_logging import setup_logger
    log = setup_logger("Finviz")
except Exception:
    import logging
    log = logging.getLogger("Finviz")
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s | %(name)s | %(message)s"))
        log.addHandler(h)
    log.setLevel("INFO")

from common.dbutils import execute_many


# ------------------ Constants / Validation ------------------

VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
DEFAULT_MARKET_CAP = "Small ($300mln to $2bln)"
DEFAULT_PRICE = "Over $1"

def _validate_exchanges(exchanges: List[str]) -> List[str]:
    bad = [e for e in exchanges if e not in VALID_EXCHANGES]
    if bad:
        log.warning("Unknown exchanges ignored: %s", bad)
    return [e for e in exchanges if e in VALID_EXCHANGES]


# ------------------ Fetch (finvizfinance) ------------------

def _fetch_view(cls, filters: Dict[str, str], view_name: str, sleep_s: float) -> pd.DataFrame:
    """
    Call finvizfinance view and return a DataFrame (or empty DF).
    """
    try:
        view = cls()
        view.set_filter(filters_dict=filters)
        df = view.screener_view()
        if df is None:
            log.warning("[%s] returned None for filters=%s", view_name, filters)
            return pd.DataFrame()
        df = pd.DataFrame(df).drop_duplicates()
        return df
    except Exception as e:
        log.error("[%s] failed (filters=%s): %s", view_name, filters, e)
        return pd.DataFrame()
    finally:
        if sleep_s > 0:
            time.sleep(sleep_s)


def fetch_finviz_fundamentals(
    exchanges: List[str] = ["NASDAQ", "NYSE", "AMEX"],
    *,
    market_cap: str = DEFAULT_MARKET_CAP,
    price: str = DEFAULT_PRICE,
    extra_filters: Optional[Dict[str, str]] = None,
    sleep_between_calls_s: float = 0.25,
    join_how: str = "outer",
) -> pd.DataFrame:
    """
    Fetch Overview + Ownership frames per exchange and merge on Ticker.
    Emits a wide DataFrame with these normalized columns added:
      - symbol (upper), name (Company), source_exchange
      - plus original prefixed columns: overview_*, ownership_*

    join_how: "outer" keeps union; "inner" keeps intersection.
    """
    exchanges = _validate_exchanges(exchanges)
    if not exchanges:
        log.error("No valid exchanges provided.")
        return pd.DataFrame()

    all_dfs: List[pd.DataFrame] = []

    for exch in exchanges:
        filters = {"Exchange": exch, "Market Cap.": market_cap, "Price": price}
        if extra_filters:
            filters.update(extra_filters)

        log.info("[Finviz] Fetching Overview + Ownership for %s ...", exch)
        df_over = _fetch_view(Overview, filters, "Overview", sleep_between_calls_s)
        if df_over.empty:
            log.warning("[Finviz] Overview returned 0 rows for %s", exch)
            continue
        if "Ticker" not in df_over.columns:
            log.warning("[Finviz] Overview missing 'Ticker' for %s (columns=%s)", exch, list(df_over.columns))
            continue
        df_over = df_over.rename(columns=lambda c: f"overview_{c}")

        df_own = _fetch_view(Ownership, filters, "Ownership", sleep_between_calls_s)
        if not df_own.empty:
            if "Ticker" not in df_own.columns:
                log.warning("[Finviz] Ownership missing 'Ticker' for %s", exch)
                df_own = pd.DataFrame()
            else:
                df_own = df_own.rename(columns=lambda c: f"ownership_{c}")

        # Merge (outer by default keeps more)
        if df_own.empty:
            df = df_over.copy()
        else:
            df = pd.merge(df_over, df_own, left_on="overview_Ticker", right_on="ownership_Ticker", how=join_how)
            df.drop(columns=["ownership_Ticker"], inplace=True, errors="ignore")

        df.rename(columns={"overview_Ticker": "symbol"}, inplace=True)
        df["symbol"] = df["symbol"].astype(str).str.upper()
        # Best-effort company name
        df["name"] = df.get("overview_Company")
        # Best-effort exchange preference: Overview.Exchange > provided 'exch'
        df["source_exchange"] = (df.get("overview_Exchange") if "overview_Exchange" in df.columns else exch)
        all_dfs.append(df)
        log.info("[Finviz] Merged %d rows for %s", len(df), exch)

    if not all_dfs:
        log.error("[Finviz] No data returned from any exchange.")
        return pd.DataFrame()

    out = pd.concat(all_dfs, ignore_index=True)
    # Deduplicate by symbol+source_exchange
    out = out.sort_values(["symbol"]).drop_duplicates(subset=["symbol", "source_exchange"], keep="first")
    return out


# ------------------ Parsing helpers (string -> typed) ------------------

_SUFFIX = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}

def _parse_num_with_suffix(s: Optional[str]) -> Optional[float]:
    """
    Parse '1.2M' => 1200000.0, '950K' => 950000, '123' => 123.0, None/'' => None
    """
    if s is None:
        return None
    txt = str(s).strip().replace(",", "")
    if txt == "" or txt.upper() in {"N/A", "-"}:
        return None
    m = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)([KMBT])?", txt, flags=re.IGNORECASE)
    if not m:
        # fall back: plain float
        try:
            return float(txt)
        except Exception:
            return None
    val = float(m.group(1))
    suf = m.group(2)
    if suf:
        val *= _SUFFIX[suf.upper()]
    return val

def _parse_percent(s: Optional[str]) -> Optional[float]:
    """
    '12.3%' => 12.3   (not 0.123) — matches your NUMERIC(10,2) percent semantics
    """
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    if t == "" or t.upper() in {"N/A", "-"}:
        return None
    if t.endswith("%"):
        t = t[:-1]
    try:
        return float(t)
    except Exception:
        return None

def _parse_int(s: Optional[str]) -> Optional[int]:
    val = _parse_num_with_suffix(s)
    if val is None:
        return None
    try:
        return int(round(val))
    except Exception:
        return None

def _parse_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    if t == "" or t.upper() in {"N/A", "-"}:
        return None
    try:
        return float(t)
    except Exception:
        return None


# ------------------ Hydration (meta‑centric) ------------------

def _hydrate_fundamental_data(df: pd.DataFrame) -> int:
    """
    Upsert into fundamental_data:
      symbol (PK), company, sector, industry, country, exchange,
      market_cap, pe_ratio, shares_float, float_percent (optional/null),
      short_float, average_true_range (optional/null), last_updated = NOW()
    """
    if df.empty:
        return 0

    # Lowercase columns for consistent access
    d = df.copy()
    d.columns = [c.lower() for c in d.columns]

    payload: List[Tuple] = []
    for _, row in d.iterrows():
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        company = row.get("name") or row.get("overview_company")
        sector = row.get("overview_sector")
        industry = row.get("overview_industry")
        country = row.get("overview_country")
        exchange = row.get("overview_exchange") or row.get("source_exchange")

        # Numbers
        market_cap = _parse_num_with_suffix(row.get("overview_market cap"))
        pe_ratio = _parse_float(row.get("overview_p/e"))  # Overview column is often 'P/E'
        shares_float = _parse_int(row.get("ownership_float"))
        short_float = _parse_percent(row.get("ownership_short float"))
        # We don't have a direct 'float_percent' from Finviz; leaving NULL is fine.
        float_percent = None
        average_true_range = _parse_float(row.get("overview_atr"))  # rarely present; else NULL

        payload.append((
            symbol, company, sector, industry, country, exchange,
            market_cap, pe_ratio, shares_float, float_percent, short_float, average_true_range
        ))

    if not payload:
        return 0

    stmt = """
        INSERT INTO fundamental_data
          (symbol, company, sector, industry, country, exchange,
           market_cap, pe_ratio, shares_float, float_percent, short_float, average_true_range, last_updated)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
          company              = COALESCE(EXCLUDED.company,              fundamental_data.company),
          sector               = COALESCE(EXCLUDED.sector,               fundamental_data.sector),
          industry             = COALESCE(EXCLUDED.industry,             fundamental_data.industry),
          country              = COALESCE(EXCLUDED.country,              fundamental_data.country),
          exchange             = COALESCE(EXCLUDED.exchange,             fundamental_data.exchange),
          market_cap           = COALESCE(EXCLUDED.market_cap,           fundamental_data.market_cap),
          pe_ratio             = COALESCE(EXCLUDED.pe_ratio,             fundamental_data.pe_ratio),
          shares_float         = COALESCE(EXCLUDED.shares_float,         fundamental_data.shares_float),
          float_percent        = COALESCE(EXCLUDED.float_percent,        fundamental_data.float_percent),
          short_float          = COALESCE(EXCLUDED.short_float,          fundamental_data.short_float),
          average_true_range   = COALESCE(EXCLUDED.average_true_range,   fundamental_data.average_true_range),
          last_updated         = NOW()
    """
    affected = execute_many(stmt, payload, commit=True)
    log.info("[Finviz] Upserted %s rows into 'fundamental_data'.", affected if (affected and affected >= 0) else len(payload))
    return len(payload)

def _hydrate_symbol_metadata(df: pd.DataFrame, default_mode: str = "cold") -> int:
    """
    Upsert into symbol_metadata: (symbol, mode DEFAULT 'cold').
    Filters left as default (empty array).
    """
    if df.empty:
        return 0

    d = df.copy()
    d.columns = [c.lower() for c in d.columns]

    payload: List[Tuple[str, str]] = []
    seen = set()
    for _, row in d.iterrows():
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        payload.append((symbol, default_mode))

    if not payload:
        return 0

    stmt = """
        INSERT INTO symbol_metadata (symbol, mode, last_updated)
        VALUES (%s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
          mode         = COALESCE(EXCLUDED.mode, symbol_metadata.mode),
          last_updated = NOW()
    """
    affected = execute_many(stmt, payload, commit=True)
    log.info("[Finviz] Upserted %s rows into 'symbol_metadata'.", affected if (affected and affected >= 0) else len(payload))
    return len(payload)


# ------------------ Orchestrator ------------------

def prime_with_finviz(
    exchanges: List[str] = ["NASDAQ", "NYSE", "AMEX"],
    *,
    market_cap: str = DEFAULT_MARKET_CAP,
    price: str = DEFAULT_PRICE,
    extra_filters: Optional[Dict[str, str]] = None,
    sleep_between_calls_s: float = 0.25,
    join_how: str = "outer",
) -> Tuple[int, int]:
    """
    Full pipeline for meta‑centric schema:
      1) fetch_finviz_fundamentals -> DataFrame
      2) hydrate fundamental_data
      3) hydrate symbol_metadata (default mode='cold')
    Returns: (count_fundamental_data_upserts, count_symbol_metadata_upserts)
    """
    df = fetch_finviz_fundamentals(
        exchanges=exchanges,
        market_cap=market_cap,
        price=price,
        extra_filters=extra_filters,
        sleep_between_calls_s=sleep_between_calls_s,
        join_how=join_how,
    )
    n_fd = _hydrate_fundamental_data(df)
    n_meta = _hydrate_symbol_metadata(df)
    return n_fd, n_meta