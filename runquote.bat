@echo off
setlocal
pushd "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  py -3 -m venv .venv
)
".venv\Scripts\python.exe" -m pip install --upgrade pip >nul 2>&1
if exist "requirements.txt" (
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
) else (
  ".venv\Scripts\python.exe" -m pip install websocket-client redis
)

REM >>> THIS IS THE MISSING PART <<<
echo Starting Quote Stream Process...
".venv\Scripts\python.exe" -m ingestion.quote_stream

popd
endlocal
