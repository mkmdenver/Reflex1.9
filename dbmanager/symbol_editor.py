# dbmanager/symbol_editor.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from common.app_logging import setup_logger
from common.dbutils import get_connection

editor_bp = Blueprint("editor", __name__, template_folder="templates")
log = setup_logger("dbm-editor")


@editor_bp.get("/")
def edit_symbols():
    rows = []
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, mode, COALESCE(filters, ARRAY[]::text[]), last_updated
            FROM symbol_metadata
            ORDER BY symbol LIMIT 500
        """)
        for sym, mode, filters, updated in cur.fetchall():
            rows.append({
                "symbol": sym, "mode": mode,
                "filters": ",".join(filters or []),
                "last_updated": updated
            })
        cur.close(); conn.close()
    except Exception as e:
        log.warning(f"Editor load failed: {e}")
    return render_template("symbol_editor.html", rows=rows)


@editor_bp.post("/update")
def update_symbol():
    symbol = request.form.get("symbol", "").upper().strip()
    mode = request.form.get("mode", "cold").lower().strip()
    filters_raw = request.form.get("filters", "").strip()
    filters = [f.strip() for f in filters_raw.split(",") if f.strip()] if filters_raw else []

    if not symbol:
        flash("Symbol is required", "error")
        return redirect(url_for("editor.edit_symbols"))

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO symbol_metadata(symbol, mode, filters, last_updated)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                mode=EXCLUDED.mode,
                filters=EXCLUDED.filters,
                last_updated=NOW()
        """, (symbol, mode, filters))
        conn.commit()
        cur.close(); conn.close()
        flash(f"Updated {symbol}", "ok")
    except Exception as e:
        log.error(f"Update failed: {e}")
        flash(f"Update failed: {e}", "error")

    return redirect(url_for("editor.edit_symbols"))
