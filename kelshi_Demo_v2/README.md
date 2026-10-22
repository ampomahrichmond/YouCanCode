# ⚡ KALSHI BOT v2 — APEX Trading Suite
Automated prediction market trading across 7 strategies.

---

## 🚀 FIRST-TIME SETUP (3 steps)

### Step 1 — Install
```bash
pip install -r requirements.txt
```

### Step 2 — Configure credentials
```bash
cp .env.template .env
# Edit .env — fill in KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH
```

**How to get Kalshi API credentials:**
1. Create a demo account at **demo.kalshi.co** (separate from live)
2. Go to **Settings → API → Create API Key**
3. Download the private key `.pem` file
4. Save it as `./kalshi_private_key.pem` (or any path you put in `.env`)
5. Copy the **Key ID** into `KALSHI_API_KEY_ID` in `.env`

> ⚠️ Demo keys only work on `demo-api.kalshi.co`
> ⚠️ Live keys only work on `trading-api.kalshi.com`
> These are separate accounts and separate key pairs.

### Step 3 — Test your connection
```bash
python diagnose.py           # Tests demo mode — run this before anything else
python diagnose.py --live    # Tests live mode
```

If you see ✅ All checks passed, you're ready.

---

## 🎯 Running the Bot

```bash
# Signal scan only — no orders placed (safest first step)
python bot.py --mode demo --scan

# Full trading on your Kalshi demo account (real demo sandbox, fake money)
python bot.py --mode demo

# Visual signal dashboard — see all opportunities in a table
python scanner.py

# Use your real live Kalshi account (requires "YES" confirmation)
python bot.py --mode live
```

---

## 🔧 Common Connection Fixes

| Symptom | Fix |
|---------|-----|
| `401 Authentication failed` | Key ID doesn't match PEM file — re-download both from Kalshi settings |
| `File not found: .pem` | Set `KALSHI_PRIVATE_KEY_PATH` to the correct path in `.env` |
| Bot shows `$1,000 simulation balance` | API keys not loaded — check `.env` exists with correct values |
| `Host unreachable` | Internet issue, or using demo URL with live keys |
| Demo keys rejected on live | Demo and live accounts are **separate** — use the right keys for each mode |
| `403 Forbidden` | Enable Trading permission on the key in Kalshi Settings → API |

**The single most common issue:** Using a live account key with `--mode demo` (or vice versa). They use different hostnames. Always use `python diagnose.py` to confirm before running the bot.

---

## 📁 File Structure

```
kalshi_bot/
├── diagnose.py        ← RUN THIS FIRST — connection tester
├── bot.py             ← Main trading engine
├── scanner.py         ← Visual signal dashboard (no trades)
├── kalshi_client.py   ← Authenticated API client (RSA-PSS fixed)
├── strategies.py      ← All 7 strategies
├── risk_manager.py    ← Kelly sizing + risk controls
├── notifier.py        ← Telegram alerts
├── requirements.txt
├── .env.template      ← Copy to .env and fill in
└── README.md
```

---

## 📊 Strategies (7 Active)

### 1. 💡 Value Edge
Finds mispriced finance/crypto/weather markets. Computes a 10-period EMA fair value
from price history; enters when the current price deviates by > 6%.
**Best on:** S&P, Bitcoin, Fed rate, CPI, weather

### 2. 🏈 NO-Side Public Bias
Fades systematic YES-side overbetting in sports. The public overpays for YES
(underdog narrative, recency bias). Buys NO when NO implied prob = 55–82%.
**Empirical edge:** ~74% win rate on NCAAB NO-side, +10% ROI
**Best on:** NFL, NBA, NCAAB, MLB

### 3. 📈 Momentum
Enters in the direction of an 8%+ price move over 5 periods, near market close (< 6h).
Momentum self-reinforces as information confirms near expiry.
**Best on:** High-volume finance and crypto markets

### 4. ⚖️ Market Making (Spread Capture)
Posts limit orders on both sides of a market's order book simultaneously.
With Kalshi's **0% fee** structure, the entire bid-ask spread is pure profit.
Example: Market at 47¢ bid / 55¢ ask → post 49¢ bid and 53¢ ask → capture 4¢/round-trip.
Cancels orphaned legs after 30 minutes to manage inventory risk.
**Best on:** High-volume markets with spread ≥ 6¢, 2–48h to close

### 5. 🔄 Statistical Arbitrage
Two sub-types:

**Ladder Arb:** Within the same series, probabilities must be monotonic.
"S&P above 5900" can't be MORE likely than "S&P above 5800". If inverted → one is mispriced.

**H2H Sum Arb:** In head-to-head markets, both YES probabilities should sum near 1.0.
If they sum to < 0.94, buy both YES (guaranteed $1+ if mutually exclusive).
If they sum to > 1.06, buy both NO.

### 6. 🎣 Stink Bids (Longshot Limit Orders)
Places deeply discounted limit orders (50% below market) on longshot markets (3–15% YES).
Not directional — these are "set and forget" orders waiting for panic-selling bursts.
Only 1–3 contracts each; positive EV at the discounted entry price.
**Best on:** Volatile crypto and finance markets

### 7. ⏳ Time Decay (Theta Play)
Captures residual uncertainty premium in near-expiry high-confidence markets.

**Sub-play A:** Market at 80–95% YES with < 4h to close → buy YES.
The 5–20¢ NO premium must decay to 0 at expiry — you collect it.

**Sub-play B:** Market at 3–20% YES with < 3h to close → buy NO.
The remaining YES premium must decay to 0 — you collect it.

**Best on:** Finance daily closes, Bitcoin level markets

---

## ⚙️ Risk Controls

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `MAX_POSITION_SIZE_PCT` | 5% | Max % of balance per trade (Kelly-capped) |
| `MAX_CONCURRENT_POSITIONS` | 10 | Open positions cap |
| `MAX_DAILY_LOSS_PCT` | 10% | Auto-halt if daily loss exceeds this |
| `MIN_EDGE_THRESHOLD` | 4% | Min signal edge to act on |
| `MIN_VOLUME` | 100 | Min market volume to consider |

**Position sizing:** Fractional Kelly (25% of full Kelly) — conservative and
proven to avoid ruin while compounding gains.

---

## 🔑 How Kalshi RSA-PSS Auth Works

Every API request requires 3 custom headers signed with your private key:

```
KALSHI-ACCESS-KEY:       <key-id>
KALSHI-ACCESS-TIMESTAMP: <unix-ms>
KALSHI-ACCESS-SIGNATURE: base64(RSA-PSS(timestamp + "GET" + "/trade-api/v2/portfolio/balance"))
```

**Critical:** The path signed must include the full `/trade-api/v2` prefix.
This was the v1 bug — v2 fixes this.

---

## 📡 Telegram Alerts

```bash
# 1. Create bot: message @BotFather → /newbot
# 2. Get your chat ID: message @userinfobot
# 3. Add to .env:
TELEGRAM_BOT_TOKEN=1234567890:AAF...
TELEGRAM_CHAT_ID=987654321
```

You'll receive alerts for: signals found, orders placed, wins/losses, daily P&L, halts.

⚠️ Treat your bot token like a password — never commit it to git.

---

## ⚠️ Disclaimer
Educational/research use. Trading prediction markets involves risk of loss.
Kalshi currently faces regulatory challenges in MA, AZ, and NV — verify legality in your state.
