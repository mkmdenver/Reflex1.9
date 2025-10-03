# dbmanager/app.py
# -----------------------------------------------------------------------------
# Flask app for Reflex DB Manager
# - Preserves original route map (home, dashboard, backfill, finviz, s3, init DB, settings, logs, symbol editor)
# - Factory create_app() so: flask --app dbmanager.app:create_app run
# - Uses DB_PARAMS from common.config (or env-safe defaults)
# - Error handlers return proper templates (no empty template names)
# - Sets insertmanyvalues_page_size to avoid 65k param issues on bulk inserts
# -----------------------------------------------------------------------------

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Any
from urllib.parse import quote_plus

from flask import (
    Flask, render_template, request, redirect, url_for, flash, current_app
)
from sqlalchemy import create_engine, text

# Keep your public APIs exactly as your app expects
from .db_backfill import run_backfill
from .init_db import drop_and_create_database

# Prefer your shared config
try:
    from common.config import DB_PARAMS  # object or dict: dbname, user, password, host, port
except Exception:
    DB_PARAMS = {
        "dbname": "timedata",
        "user": "postgres",
        "password": "",
        "host": "127.0.0.1",
        "port": 5432,
    }

# -----------------------------------------------------------------------------
# Simple in-memory state for the UI (safe placeholders)
# -----------------------------------------------------------------------------
STATE: Dict[str, Any] = {
    "db_initialized": True,
    "symbols_count": 0,
    "last_backfill": None,
    "last_finviz_import": None,
    "last_s3_load": None,
}

# -----------------------------------------------------------------------------
# Engine helpers
# -----------------------------------------------------------------------------
def _build_db_url() -> str:
    if isinstance(DB_PARAMS, dict):
        dbname = DB_PARAMS.get("dbname", "timedata")
        user = DB_PARAMS.get("user", "postgres")
        password = DB_PARAMS.get("password", "")
        host = DB_PARAMS.get("host", "127.0.0.1")
        port = DB_PARAMS.get("port", 5432)
    else:
        dbname = getattr(DB_PARAMS, "dbname", "timedata")
        user = getattr(DB_PARAMS, "user", "postgres")
        password = getattr(DB_PARAMS, "password", "")
        host = getattr(DB_PARAMS, "host", "127.0.0.1")
        port = getattr(DB_PARAMS, "port", 5432)

    return f"postgresql+psycopg://{user}:{quote_plus(str(password))}@{host}:{port}/{dbname}"


def _build_engine():
    url = _build_db_url()
    engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        # force paging for large multi-row inserts to avoid 65k parameter limit
        execution_options={"insertmanyvalues_page_size": 1000},
    )
    return engine

# -----------------------------------------------------------------------------
# App factory
# -----------------------------------------------------------------------------
def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = "reflex-dbmanager"  # replace via config/secret in production

    # Attach SQLAlchemy engine
    engine = _build_engine()
    app.config["ENGINE"] = engine

    # Logging
    if not app.logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
        app.logger.addHandler(h)
        app.logger.setLevel(logging.INFO)

    # -------------------------------------------------------------------------
    # ROUTES (preserved)
    # -------------------------------------------------------------------------

    # Front door: simple home splash (your template exists)
    @app.route("/")
    def home():
        app.logger.info("Home route accessed, checking DB status...")
        db_ok = False
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            db_ok = True
        except Exception as e:
            app.logger.warning("DB status check failed: %s", e)
        return render_template("home.html", db_ok=db_ok)

    # Optional dashboard summary page (your index.html shows state dict)
    @app.route("/index")
    def index():
        return render_template("index.html", state=STATE)

    # Backfill from Polygon (GET shows form, POST runs)
    @app.route("/backfill_polygon", methods=["GET", "POST"])
    def backfill_polygon():
        app.logger.info("Backfill polygon route accessed")
        if request.method == "GET":
            return render_template("backfill_polygon.html")

        # POST
        symbols_raw = request.form.get("symbols") or request.form.get("symbol") or ""
        mode = (request.form.get("mode") or "recent").strip().lower()
        # Optional date window (if your run_backfill/backfill_range supports it)
        start = request.form.get("start") or None
        end = request.form.get("end") or None

        # Storage toggles: default daily-only if omitted
        store_daily = (request.form.get("store_daily") or "on").lower() in ("on", "true", "1", "yes")
        store_minute = (request.form.get("store_minute") or "").lower() in ("on", "true", "1", "yes")
        store_tick = (request.form.get("store_tick") or "").lower() in ("on", "true", "1", "yes")

        # Normalize symbols CSV / newline
        symbols = []
        for tok in symbols_raw.replace(",", " ").split():
            tok = tok.strip().upper()
            if tok:
                symbols.append(tok)
        if not symbols:
            flash("Please provide at least one symbol.", "warning")
            return render_template("backfill_polygon.html", result=None)

        try:
            result = run_backfill(
                symbols=symbols,
                mode=mode,
                start=start,
                end=end,
                store_daily=store_daily,
                store_minute=store_minute,
                store_tick=store_tick,
            )
            # Update simple state
            STATE["last_backfill"] = datetime.utcnow()
            flash("Backfill complete.", "success")
            return render_template("backfill_polygon.html", result=result)
        except Exception as e:
            app.logger.exception("Backfill failed")
            return render_template("error.html", code=500, message=f"Backfill failed: {e}", error=str(e)), 500

    # Symbol editor (present but wire later)
    @app.route("/symbol_editor", methods=["GET"])
    def symbol_editor():
        return render_template("symbol_editor.html")

    # S3 loader (form + mock action)
    @app.route("/s3_load", methods=["GET", "POST"])
    def s3_load():
        if request.method == "GET":
            return render_template("s3_load.html")
        # POST:
        prefix = request.form.get("prefix") or ""
        # TODO: wire to your S3 loader pipeline
        STATE["last_s3_load"] = datetime.utcnow()
        flash(f"S3 load requested (prefix='{prefix}')", "info")
        return redirect(url_for("s3_load"))

    # Finviz importer (form + mock action)
    @app.route("/finviz_load", methods=["GET", "POST"])
    def finviz_load():
        if request.method == "GET":
            return render_template("finviz_load.html")
        # POST:
        # TODO: call your finviz_adapter to fetch and persist symbols/fundamentals
        STATE["last_finviz_import"] = datetime.utcnow()
        flash("FinViz import requested.", "info")
        return redirect(url_for("finviz_load"))

    # Initialize (reset) the DB with a safety latch
    @app.route("/init_db", methods=["GET", "POST"])
    def init_db():
        if request.method == "GET":
            return render_template("init_db.html")
        # POST: expect confirmation text "DELETE"
        confirmation = (request.form.get("confirm") or "").strip().upper()
        if confirmation != "DELETE":
            flash("Type DELETE to confirm database initialization.", "warning")
            return render_template("init_db.html")
        try:
            drop_and_create_database()  # your exported initializer
            STATE["db_initialized"] = True
            flash("Database initialized successfully.", "success")
            # Optional confirmation page (you have init_confirm.html)
            token = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            return render_template("init_confirm.html", message="DB reset complete.", token=token)
        except Exception as e:
            current_app.logger.exception("Database init failed")
            return render_template("error.html", code=500, message=f"DB init failed: {e}", error=str(e)), 500

    # Dashboard (alt landing)
    @app.route("/dashboard", methods=["GET"])
    def dashboard():
        # Fill simple placeholders
        app_env = "DEV"
        db_status = "OK"
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            db_status = "OK"
        except Exception:
            db_status = "DOWN"
        # You can wire real counts here later
        symbol_count = STATE.get("symbols_count", 0)
        profile_count = 0
        db_size_mb = 0
        return render_template(
            "dashboard.html",
            app_env=app_env,
            db_params=DB_PARAMS,
            db_status=db_status,
            init_required=(db_status != "OK"),
            symbol_count=symbol_count,
            profile_count=profile_count,
            db_size_mb=db_size_mb,
        )

    # Logs page
    @app.route("/logs", methods=["GET"])
    def logs_page():
        return render_template("logs.html")

    # Settings pages
    @app.route("/settings/locations", methods=["GET"])
    def settings_locations():
        return render_template("settings_locations.html")

    @app.route("/settings/passwords", methods=["GET"])
    def settings_passwords():
        return render_template("settings_passwords.html")

    # Integrative check
    @app.route("/integrative_check", methods=["GET"])
    def integrative_check():
        return render_template("integrative_check.html")

    # -------------------------------------------------------------------------
    # ERROR HANDLERS
    # -------------------------------------------------------------------------
    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    @app.errorhandler(500)
    def internal_error(e):
        current_app.logger.exception("Unhandled 500")
        return render_template("error.html", code=500, message="Internal Server Error", error=str(e)), 500

    return app


# Allow "python -m dbmanager.app" for local testing
if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=5050, debug=False)