# Reflex — Working Notes (Cleaned)

## 1) Path to Live Trading (small-cap, high-momentum focus)
- **Brokers:** Alpaca (sim), Webull (live), IB, Schwab — verify keys, throttles, logging.
- **Model:** Bull flag (tight stops), with **throttle** (size) and **torque** (stop) controls.
- **Capital:** Start small, scale with observed performance and slippage.

## 2) Verification Dashboard (Cockpit)
- State transitions: **COLD → WATCH → WARM → HOT**
- Trade signals & outcomes; latency and model health
- P&L and session metrics; environment/keys sanity checks

## 3) Go‑Live Checklist
- DB reset (**v1.16**, safety‑latched)
- Replay mode parity checks (ingest → evaluator → trader)
- Evaluator real‑time scoring enabled
- Trader executing (paper → staged live)
- Logging/diagnostics green

## 4) Instrument Panel (Model Watcher)
- **Symbol Map:** counts by state; promote/demote actions
- **Evaluator Output:** active model, confidence, filters
- **Signals & Orders:** entries/exits, broker route, fills
- **Latency Monitor:** tick → decision → order
- **P&L:** per symbol/model; session totals

## 5) Programming Rules / Notes
- Platform: **Windows‑only**; Python primary; VS Code.
- Data: Polygon (REST/WebSocket); TimescaleDB; **MongoDB later** (JSON for now).
- Imports robust; ensure `pip install` guidance where needed.
- **No stubs** — production‑quality code unless explicitly in mockup mode.

---
*Cleaned and de‑duplicated from [Reflex-Notes.md](https://rockymountaintechnet-my.sharepoint.com/personal/mike_malone_rockymountaintech_net/Documents/Forms/DispForm.aspx?ID=675&web=1&EntityRepresentationId=5dde0990-189e-4362-bf81-1e1127f289c6).* [9](https://rockymountaintechnet-my.sharepoint.com/personal/mike_malone_rockymountaintech_net/Documents/Reflex-compromised/Reflex-Notes.md)

# Reflex — Startup Flow

## 1) Boot
- Load model configs (e.g., `momentum_filter_v1.json`)
- Validate DB connectivity
- Start Flask API; heartbeat begins
## 2) Registry Hydration
- Load symbols (prioritize those marked **WARM**)
- Build snapshots; initialize buffers
- States progress across full ladder: **COLD → WATCH → WARM → HOT**

## 3) Live Streams
- Subscribe via Polygon WebSocket:
  - Top‑of‑book quotes (bid/ask)
  - Tick‑level trades
## 4) Transition to HOT
- Buffers fill; snapshots update; quote‑depth tracking begins
- State flips through **WATCH/WARM** to **HOT** as readiness criteria are met

## 5) Evaluator Loop
- Continuous scoring: volatility, volume, reflexive depth, snapshot readiness
- Emits signals → Trader; diagnostics recorded for replay parity

---
*Normalized from [Reflexion.md](https://rockymountaintechnet-my.sharepoint.com/personal/mike_malone_rockymountaintech_net/Documents/Forms/DispForm.aspx?ID=672&web=1&EntityRepresentationId=37759668-d1b8-4378-aa80-14a8a1f21e47); added explicit **WATCH** stage per system design.* [13](https://rockymountaintechnet-my.sharepoint.com/personal/mike_malone_rockymountaintech_net/Documents/Reflex-compromised/Reflexion.md)
