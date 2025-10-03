# Reflex v1.16 — Overview & Quickstart (Windows-first)

A production‑focused, modular trading system: **DataHub** (ingest), **Evaluator** (signals), **Trader** (execution), **Cockpit** (UI), plus **dbmanager** for database lifecycle.

## Quickstart (Windows)
```bat
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

set REFLEX_ENV=dev
set DATAHUB_HOST=127.0.0.1
set DATAHUB_PORT=5001
set COCKPIT_PORT=5002
set EVALUATOR_PORT=5003
set TRADER_PORT=5004
set LOG_LEVEL=INFO
:: Alpaca (optional for live orders)
:: set ALPACA_BASE_URL=https://paper-api.alpaca.markets
:: set ALPACA_API_KEY=YOUR_KEY
:: set ALPACA_API_SECRET=YOUR_SECRET

scripts\start_datahub.bat
scripts\start_evaluator.bat
scripts\start_trader.bat
scripts\start_cockpit.bat
