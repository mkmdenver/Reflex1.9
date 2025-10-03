import os, sys, runpy

PG_BIN = r"C:\Program Files\PostgreSQL\17\bin"  # change if your version differs

# Ensure DLL directory is available in THIS process (Python 3.8+ behavior on Windows)
if sys.platform == "win32":
    try:
        os.add_dll_directory(PG_BIN)
        # also add to PATH for any subprocesses this script might spawn
        os.environ["PATH"] = os.environ.get("PATH", "") + ";" + PG_BIN
    except Exception as e:
        print("Warning: add_dll_directory failed:", e)

print("Verifying psycopg2 import...")
import psycopg2
print("psycopg2 OK on", sys.version)

# 1) Initialize the DB (bypass UI safety latch) in the SAME interpreter
print("Running dbmanager.init_db --force ...")
sys.argv = ["dbmanager.init_db", "--force"]
runpy.run_module("dbmanager.init_db", run_name="__main__")
print("DB init complete.")

# 2) Start the State Subscription Bridge (direct python, not the .bat)
print("Starting State Subscription Bridge ...")
runpy.run_path(os.path.join("control", "state_subscription_bridge.py"), run_name="__main__")
