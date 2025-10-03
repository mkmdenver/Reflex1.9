# dbmanager/backfill.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from common.app_logging import setup_logger
from common.dbutils import get_connection

# use your existing backfill engine
from dbmanager.db_backfill import run_backfill


log = setup_logger("dbm-backfill")



def run_backfill_route():
    mode = request.form.get("mode", "recent").lower()
    raw_symbols = request.form.get("symbols", "").strip()
    symbols = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()] if raw_symbols else []

    if not symbols:
        # Fallback: take top N from symbol_metadata
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT symbol FROM symbol_metadata ORDER BY symbol LIMIT 20")
            symbols = [r[0] for r in cur.fetchall()]
            cur.close(); conn.close()
        except Exception as e:
            flash("No symbols provided and metadata unavailable.", "error")
            return redirect(url_for("backfill.view_backfill"))

    try:
        run_backfill(symbols, mode=mode)
        flash(f"Backfill '{mode}' completed for {len(symbols)} symbols.", "ok")
    except Exception as e:
        log.error(f"Backfill failed: {e}")
        flash(f"Backfill failed: {e}", "error")

    return redirect(url_for("backfill.view_backfill"))
