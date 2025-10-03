@echo off
setlocal
set DATAHUB_HOST=127.0.0.1
set DATAHUB_PORT=5001
python -m datahub.server --host %DATAHUB_HOST% --port %DATAHUB_PORT%
pause
exit /b 