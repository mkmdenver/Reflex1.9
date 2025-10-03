# dbmanager/logs.py
import os
from flask import Blueprint, render_template
from common.app_logging import setup_logger

logs_bp = Blueprint("logs", __name__, template_folder="templates")
log = setup_logger("dbm-logs")

DEFAULT_LOG_PATHS = [
    os.path.join(os.getcwd(), "logs", "reflex_dev.log"),
    os.path.join(os.getcwd(), "dbmanager.log"),
]


@logs_bp.get("/")
def view_logs():
    lines = []
    for p in DEFAULT_LOG_PATHS:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    tail = f.readlines()[-500:]  # last ~500 lines
                    lines.extend([f"[{os.path.basename(p)}] {ln.rstrip()}" for ln in tail])
            except Exception as e:
                lines.append(f"[{os.path.basename(p)}] <error reading: {e}>")
    if not lines:
        lines = ["<no logs found>"]
    return render_template("logs.html", lines=lines)
