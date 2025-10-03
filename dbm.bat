@echo off
setlocal
cd /d "%~dp0"

REM ------------------------------------------------------------
REM Reflex DBManager launcher (Windows, no activation required)
REM ------------------------------------------------------------

REM Ensure Python launcher is available
where py >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python launcher 'py' not found in PATH.
  echo Install Python from https://www.python.org/downloads/windows/ and enable "Add python.exe to PATH".
  exit /b 1
)

REM Create venv if missing
if not exist ".venv\Scripts\python.exe" (
  echo [INFO] Creating virtual environment: .venv
  py -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    exit /b 1
  )
)

REM Upgrade pip and install requirements (if present)
echo [INFO] Upgrading pip...
".venv\Scripts\python.exe" -m pip install --upgrade pip

if exist "requirements.txt" (
  echo [INFO] Installing requirements from requirements.txt ...
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
) else (
  echo [WARN] requirements.txt not found; proceeding without dependency install.
)

REM Configure Flask app factory
set "FLASK_APP=dbmanager.app:create_app"
if not defined DBM_INIT_TOKEN set "DBM_INIT_TOKEN=I_UNDERSTAND_DROP_AND_REBUILD"
if not defined DBMANAGER_PORT set "DBMANAGER_PORT=5050"

echo [INFO] Starting dbmanager on http://127.0.0.1:%DBMANAGER_PORT%/
".venv\Scripts\python.exe" -m flask run --port %DBMANAGER_PORT%
endlocal
