# dbmanager/dashboard.py
import os
from flask import Blueprint, render_template, url_for
from typing import Tuple, Optional

dashboard_bp = Blueprint("dashboard", __name__, template_folder="templates")

# Common imports
try:
    from common.config import DB_PARAMS, ENV
    from common.dbutils import get_connection
    from common.app_logging import setup_logger
except Exception as e:
    # Allow page to render even if common isn't ready; all values guarded
    DB_PARAMS, ENV = {"dbname": os.getenv("PGDATABASE", "postgres")}, os.getenv("REFLEX_ENV", "dev")
    def get_connection():  # type: ignore
        raise RuntimeError("common.dbutils.get_connection unavailable")
    def setup_logger(*args, **kwargs):  # type: ignore
        class _N: 
            def info(self,*a,**k): ...
            def warning(self,*a,**k): ...
            def error(self,*a,**k): ...
        return _N()

log = setup_logger("dbm-dashboard")


def _try_query(sql: str, params=None) -> Optional[list]:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        log.warning(f"DB query failed: {e}")
        return None


def _db_overview() -> Tuple[str, bool, dict]:
    """Return (status_text, available, metrics) guarded."""
    # Basic ping
    available = False
    status_text = "unavailable"
    metrics = {"symbol_count": None, "profile_count": None, "db_size_mb": None}

    try:
        rows = _try_query("SELECT 1;")
        available = rows is not None
        status_text = "available" if available else "unavailable"
    except Exception as e:
        log.warning(f"Ping failed: {e}")

    if available:
        # symbol count (metadata table may not exist yet)
        syms = _try_query("SELECT COUNT(*) FROM symbol_metadata;")
        if syms:
            metrics["symbol_count"] = syms[0][0]
        # profile count (fundamentals table may not exist yet)
        prof = _try_query("SELECT COUNT(*) FROM fundamental_data;")
        if prof:
            metrics["profile_count"] = prof[0][0]
        # DB size
        size = _try_query("SELECT pg_size_pretty(pg_database_size(current_database()));")
        if size:
            txt = size[0][0]
            # best-effort to MB
            metrics["db_size_mb"] = txt
    return status_text, available, metrics
@dashboard_bp.get("/")
def view_dashboard():
    status_text, available, metrics = _db_overview()

    ctx = {
        "title": "Dashboard",
        "app_env": ENV,
        "db_params": type("DB", (), DB_PARAMS) if isinstance(DB_PARAMS, dict) else DB_PARAMS,
        "db_status": status_text,
        "init_required": not available,
        "symbol_count": metrics["symbol_count"],
        "profile_count": metrics["profile_count"],
        "db_size_mb": metrics["db_size_mb"],
    }
    return render_template("dashboard.html", **ctx)
