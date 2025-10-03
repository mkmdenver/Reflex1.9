@echo off
setlocal
if "%COCKPIT_PORT%"=="" set COCKPIT_PORT=5002
python -m cockpit.app --port %COCKPIT_PORT%
