@echo off
echo Starting Reflex modules in correct order...
start "Bridge" "C:\Projects\Reflex\bin\bridge.bat"
timeout /t 2 >nul
start "Datahub" "C:\Projects\Reflex\bin\datahub.bat"
timeout /t 2 >nul
start "Tick Stream" "C:\Projects\Reflex\bin\tickstream.bat"
timeout /t 2 >nul
start "Quote Stream" "C:\Projects\Reflex\bin\quotestream.bat"
timeout /t 2 >nul
start "Trader" "C:\Projects\Reflex\bin\trader.bat"
echo All modules launched.
