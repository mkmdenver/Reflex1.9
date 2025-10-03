# dbmanager/init.py
import os
from flask import Blueprint, render_template, request, redirect, url_for, flash
from common.app_logging import setup_logger
from common.dbutils import get_connection, execute_sql_commands

# Import your SQL statements from dbmanager/init_db.py
try:
    from dbmanager.init_db import SQL_STATEMENTS
except Exception:
    SQL_STATEMENTS = []

init_bp = Blueprint("init", __name__, template_folder="templates")
log = setup_logger("dbm-init")

_CONFIRM_TOKEN = os.getenv("DBM_INIT_TOKEN", "I_UNDERSTAND_DROP_AND_REBUILD")


@init_bp.get("/")
def confirm_init():
    message = (
        "This will DROP and REBUILD the database schema (hypertables, views, indexes). "
        "Type the confirmation token to proceed."
    )
    return render_template("init_confirm.html", message=message, token=_CONFIRM_TOKEN)


@init_bp.post("/run")
def run_init():
    token = request.form.get("token", "")
    if token != _CONFIRM_TOKEN:
        flash("Confirmation token mismatch. Init aborted.", "error")
        return redirect(url_for("init.confirm_init"))

    try:
        if not SQL_STATEMENTS:
            raise RuntimeError("No SQL statements available.")
        execute_sql_commands(SQL_STATEMENTS)
        flash("Database initialized successfully.", "ok")
    except Exception as e:
        log.error(f"Init failed: {e}")
        flash(f"Init failed: {e}", "error")
    return redirect(url_for("dashboard.view_dashboard"))
