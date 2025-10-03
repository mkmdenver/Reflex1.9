@echo off
setlocal
title Reflex - StateSubscriptionBridge (Garnet + DB_CONFIG)
pushd "%~dp0"

REM --- Ensure local venv ---
if not exist ".venv\Scripts\python.exe" (
  echo [Bridge] Creating virtual environment...
  py -3 -m venv .venv 2>nul || python -m venv .venv
  if errorlevel 1 (
    echo [Bridge] ERROR: Failed to create virtualenv.
    exit /b 1
  )
)

echo [Bridge] Upgrading pip...
".venv\Scripts\python.exe" -m pip install --upgrade pip >nul 2>&1

REM --- Install project requirements (if any) ---
if exist "requirements.txt" (
  echo [Bridge] Installing requirements.txt ...
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
)

REM --- Ensure bridge-specific deps are present regardless of requirements.txt ---
REM Garnet uses the redis client; bridge also needs websocket-client (used by ingestion peers) and psycopg2-binary for DB.
echo [Bridge] Ensuring bridge dependencies...
".venv\Scripts\python.exe" -m pip install redis websocket-client psycopg2-binary

REM --- Optional: log directory ---
if not exist "logs" mkdir "logs"

REM --- Ensure Python can import local packages (common/, control/) ---
set "PYTHONPATH=%CD%"

echo [Bridge] Starting State → Subscription Bridge...
REM Foreground (see live logs in this window):
python -m control.state_subscription_bridge

REM If you prefer logging to file instead of console, comment the line above and uncomment below:
REM ".venv\Scripts\python.exe" -m control.state_subscription_bridge >> "logs\state_bridge.log" 2>&1

popd
endlocal
