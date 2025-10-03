# common/common.py
from __future__ import annotations
import os
import json
import psycopg2
from common.app_logging import setup_logger
from common.config import DB_PARAMS  # <-- use DB_PARAMS, not DB_CONFIG

log = setup_logger("common")

# ── Paths ────────────────────────────────────────────────────────────────
ROOT_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FLAGS_PATH = os.path.join(ROOT_DIR, "common", "lifecycle_flags.json")
CONFIG_PATH= os.path.join(ROOT_DIR, "common", "config.json")

# ── Metadata loaders ─────────────────────────────────────────────────────
def load_symbols_from_db() -> list[str]:
    """
    Returns DISTINCT symbols from ticks table.
    """
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM ticks")
            rows = cur.fetchall()
            return [r[0] for r in rows]
    finally:
        conn.close()

def load_lifecycle_flags(path: str = FLAGS_PATH) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"[❌] Failed to load lifecycle_flags.json: {e}")
        return {}

def load_json_config(path: str = CONFIG_PATH) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"[❌] Failed to load config.json: {e}")
        return {}
