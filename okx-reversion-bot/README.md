# OKX Reversion Bot

Automated short-reversion engine for OKX USDT-margined perpetual futures.
Detects rapid upward moves, waits for momentum exhaustion, opens shorts,
manages exits with trailing stops.

> **This is phase 1 scaffolding.** The strategy engine, OKX client, and risk
> engine are stubbed. Paper trading and live execution are NOT YET FUNCTIONAL.
> What works today: project structure, database schema, encrypted credential
> vault, settings UI, health endpoints.

## Build phases

1. **Phase 1 (current)** — scaffold: backend skeleton, DB schema, settings UI, credential vault.
2. **Phase 2** — OKX market-data adapter (WebSocket + REST), live tickers in UI.
3. **Phase 3** — strategy state machine + paper executor + risk engine, end-to-end on paper.
4. **Phase 4** — OKX demo/testnet integration, then live execution behind explicit gate.
5. **Phase 5** — premium UI polish + analytics.
6. **Phase 6** — measurement + recommender (the "learning layer").

## Running locally

### Backend

```powershell
cd backend
python -m venv .venv

# PowerShell — activate then run:
.\.venv\Scripts\Activate.ps1
pip install -e .
uvicorn app.main:app --reload --port 8000

# Or skip activation entirely (works in any Windows shell):
.venv\Scripts\python.exe -m pip install -e .
.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
```

If PowerShell blocks `Activate.ps1` with an execution-policy error, run
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` once per session,
or just use the no-activation form above.

On macOS/Linux: `source .venv/bin/activate`.

API docs: http://localhost:8000/docs

### Frontend

```bash
cd frontend
npm install
npm run dev
```

UI: http://localhost:5173

## Configuration

On first launch the backend creates `data/.master.key` — this is the symmetric
key that encrypts every API credential stored in the database. **Back it up
and never commit it.** It is in `.gitignore`.

Application settings live in `.env` (copy from `.env.example`). API
credentials (OKX, Perplexity) are entered through the Settings page in the
UI and encrypted at rest.

## Safety defaults

- `BOT_TRADE_MODE=paper` — no real orders are placed.
- The bot starts in `paused` state. Operator must explicitly resume.
- Live trading requires `BOT_TRADE_MODE=live` AND a per-session unlock toggle in the UI (not yet implemented).
