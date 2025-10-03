# dbmanager/symbols.py
from flask import Blueprint, render_template
from common.app_logging import setup_logger
from common.dbutils import get_connection

symbols_bp = Blueprint("symbols", __name__, template_folder="templates")
log = setup_logger("dbm-symbols")


@symbols_bp.get("/")
def list_symbols():
    symbols = []
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, mode, COALESCE(filters, ARRAY[]::text[]), last_updated
            FROM symbol_metadata
            ORDER BY symbol ASC
            LIMIT 5000
        """)
        for sym, mode, filters, updated in cur.fetchall():
            symbols.append({
                "ticker": sym,
                "company": None,
                "status": mode,
                "do_not_trade": ("do_not_trade" in (filters or [])),
                "updated": updated,
            })
        cur.close()
        conn.close()
    except Exception as e:
        log.warning(f"Symbols list unavailable: {e}")

    return render_template("symbols.html", symbols=symbols)
