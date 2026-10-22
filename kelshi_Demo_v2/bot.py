"""
bot.py
======
APEX Kalshi Trading Bot — Main Engine

Strategies: ValueEdge | NoSideBias | Momentum | MarketMaking | StatArb | StinkBid | TimeDecay

Run:
  python bot.py --mode demo              # Trade on real Kalshi demo account
  python bot.py --mode demo --scan       # Signal scan only, no orders
  python bot.py --mode demo --diagnose   # Test connection only
  python bot.py --mode live              # Real money (requires confirmation)

Author: APEX Trading Suite - Kalshi Module
"""

import os, sys, time, logging, argparse, json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from dotenv import load_dotenv

# ── Robust .env loading (works on Windows regardless of CWD) ─────────────────
def _load_env():
    script_dir = Path(__file__).resolve().parent
    for candidate in [script_dir / ".env", Path.cwd() / ".env"]:
        if candidate.exists():
            load_dotenv(dotenv_path=str(candidate), override=True)
            return
    load_dotenv(override=True)  # fallback

_load_env()

from kalshi_client import KalshiClient, KalshiAPIError
from risk_manager  import RiskManager, RiskConfig
from strategies    import (StrategyEngine, TradeSignal, SignalType,
                           SignalDirection)
from notifier      import TelegramNotifier


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
def setup_logging(level: str = "INFO"):
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level), format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("kalshi_bot.log", encoding="utf-8"),
        ]
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger("KalshiBot")


# ─────────────────────────────────────────────────────────────────────────────
# P&L Journal
# ─────────────────────────────────────────────────────────────────────────────
class PnLTracker:
    def __init__(self, path: str = "pnl_log.json"):
        self.path    = path
        self.records: List[Dict] = self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                return json.load(open(self.path))
            except Exception:
                return []
        return []

    def _save(self):
        json.dump(self.records, open(self.path, "w"), indent=2, default=str)

    def log(self, record: Dict):
        record["ts"] = datetime.now().isoformat()
        self.records.append(record)
        self._save()

    def log_signal(self, sig: TradeSignal, contracts: int, cost: float):
        self.log({"type":"SIGNAL","ticker":sig.ticker,"direction":sig.direction.value,
                  "strategy":sig.strategy_name,"edge":sig.edge,"conf":sig.confidence,
                  "contracts":contracts,"cost_usd":cost})

    def log_trade(self, ticker, action, side, contracts, price_cents):
        self.log({"type":"TRADE","ticker":ticker,"action":action,"side":side,
                  "contracts":contracts,"price_cents":price_cents,
                  "cost_usd":(contracts*price_cents)/100.0})

    def summary(self) -> str:
        buys = [r for r in self.records if r.get("type")=="TRADE" and r.get("action")=="buy"]
        sigs = [r for r in self.records if r.get("type")=="SIGNAL"]
        return f"{len(buys)} trades placed | {len(sigs)} signals generated"


# ─────────────────────────────────────────────────────────────────────────────
# Market-Making Order Tracker
# ─────────────────────────────────────────────────────────────────────────────
class MMTracker:
    """Track open market-making quote pairs. Cancel orphaned legs."""
    def __init__(self):
        self.quotes: Dict[str, Dict] = {}   # ticker → {bid_id, ask_id, entered_at, ...}

    def add(self, ticker: str, bid_order_id: str, ask_order_id: str,
            bid_cents: int, ask_cents: int, contracts: int):
        self.quotes[ticker] = {
            "bid_id":     bid_order_id,
            "ask_id":     ask_order_id,
            "bid_cents":  bid_cents,
            "ask_cents":  ask_cents,
            "contracts":  contracts,
            "entered_at": datetime.now(),
            "bid_filled": False,
            "ask_filled": False,
        }

    def remove(self, ticker: str):
        self.quotes.pop(ticker, None)

    def stale_tickers(self, max_age_minutes: int = 30) -> List[str]:
        now  = datetime.now()
        stale = []
        for t, q in self.quotes.items():
            age = (now - q["entered_at"]).total_seconds() / 60
            if age > max_age_minutes and not (q["bid_filled"] and q["ask_filled"]):
                stale.append(t)
        return stale


# ─────────────────────────────────────────────────────────────────────────────
# Main Bot
# ─────────────────────────────────────────────────────────────────────────────
class KalshiBot:

    STRATEGY_LABELS = {
        "ValueEdge":   "💡",
        "NoSideBias":  "🏈",
        "Momentum":    "📈",
        "MarketMaking":"⚖️ ",
        "StatArb":     "🔄",
        "StinkBid":    "🎣",
        "TimeDecay":   "⏳",
    }

    def __init__(self, mode="demo", scan_only=False, scan_interval=300,
                 max_close_hours=48, debug_mode=False):
        self.mode             = mode
        self.scan_only        = scan_only
        self.scan_interval    = scan_interval
        self.max_close_hours  = max_close_hours
        self.debug_mode       = debug_mode

        self.client   = self._init_client()
        self.risk     = self._init_risk()
        self.engine   = StrategyEngine(
            min_edge=float(os.getenv("MIN_EDGE_THRESHOLD", 0.04)),
            min_confidence=0.50,
        )
        self.notifier = TelegramNotifier()
        self.pnl      = PnLTracker()
        self.mm       = MMTracker()

        self.open_positions: Dict[str, Dict] = {}
        self.is_running = False

    # ── Init ──────────────────────────────────────────────────────────────────
    def _init_client(self) -> Optional[KalshiClient]:
        key_id   = os.getenv("KALSHI_API_KEY_ID", "").strip().strip('"').strip("'")
        raw_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem").strip().strip('"').strip("'")

        # Resolve key path relative to script dir (reliable on Windows)
        script_dir = Path(__file__).resolve().parent
        key_path   = Path(raw_path)
        if not key_path.is_absolute():
            key_path = (script_dir / key_path).resolve()

        if not key_id:
            logger.warning("⚠️  KALSHI_API_KEY_ID not set → LOCAL SIMULATION mode")
            logger.warning("    Run: python diagnose.py  to debug .env loading")
            return None

        try:
            client = KalshiClient(key_id=key_id, private_key_path=str(key_path), mode=self.mode)
            return client
        except (FileNotFoundError, ValueError) as e:
            logger.error(str(e))
            logger.warning("Falling back to LOCAL SIMULATION mode")
            return None

    def _init_risk(self) -> RiskManager:
        return RiskManager(RiskConfig(
            max_position_size_pct     = float(os.getenv("MAX_POSITION_SIZE_PCT", 0.05)),
            max_concurrent_positions  = int(os.getenv("MAX_CONCURRENT_POSITIONS", 10)),
            max_daily_loss_pct        = float(os.getenv("MAX_DAILY_LOSS_PCT", 0.10)),
            min_edge_threshold        = float(os.getenv("MIN_EDGE_THRESHOLD", 0.04)),
            min_volume                = int(os.getenv("MIN_VOLUME", 100)),
            max_expiry_hours          = int(os.getenv("MAX_EXPIRY_HOURS", 48)),
        ))

    # ── Startup ───────────────────────────────────────────────────────────────
    def start(self):
        self.is_running = True
        self._print_banner()

        # ── Connection check ─────────────────────────────────────────────────
        if self.client:
            ok = self.client.print_diagnostics()
            if not ok:
                print("\n❌ Cannot connect to Kalshi API. Fix credentials and retry.")
                print("   Run: python bot.py --mode demo --diagnose  for details\n")
                sys.exit(1)
            connected_label = f"✅ Connected to Kalshi {self.mode.upper()} API"
        else:
            print("\n⚠️  Running in LOCAL SIMULATION (no real API calls)")
            print("   To connect: fill KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH in .env\n")
            connected_label = "🖥️  LOCAL SIMULATION"

        balance = self._get_balance()
        self.risk.set_balance(balance)
        self.notifier.bot_started(self.mode, balance)

        logger.info(f"\n{'═'*60}")
        logger.info(f"  ⚡ KALSHI BOT RUNNING")
        logger.info(f"  {connected_label}")
        logger.info(f"  Balance:  ${balance:,.2f}")
        logger.info(f"  Interval: {self.scan_interval}s  |  ScanOnly: {self.scan_only}")
        logger.info(f"  Strategies: 7 active (ValueEdge|NoSide|Momentum|MM|StatArb|Stink|Theta)")
        logger.info(f"{'═'*60}\n")

        try:
            self._main_loop()
        except KeyboardInterrupt:
            logger.info("⏹  Stopped by user")
        except Exception as e:
            logger.error(f"Fatal: {e}", exc_info=True)
            self.notifier.error_alert(str(e))
        finally:
            self._shutdown()

    # ── Main Loop ─────────────────────────────────────────────────────────────
    def _main_loop(self):
        cycle = 0
        while self.is_running:
            cycle += 1
            logger.info(f"\n{'─'*55}")
            logger.info(f"  🔄 CYCLE #{cycle} | {datetime.now().strftime('%H:%M:%S')}")
            logger.info(f"{'─'*55}")

            try:
                self._run_cycle()
            except KalshiAPIError as e:
                logger.error(f"API error: {e}")
                if e.status_code == 401:
                    logger.error("  → Authentication failed. Check API keys.")
                self.notifier.error_alert(str(e))
            except Exception as e:
                logger.error(f"Cycle error: {e}", exc_info=True)

            if cycle % 24 == 0:
                self.notifier.daily_summary(self.risk.get_stats(), self._get_balance())

            logger.info(f"💤 Sleeping {self.scan_interval}s...")
            time.sleep(self.scan_interval)

    # ── Scan Cycle ────────────────────────────────────────────────────────────
    def _run_cycle(self):
        balance = self._get_balance()
        self.risk.set_balance(balance)

        ok, reason = self.risk.can_trade(balance)
        if not ok:
            logger.warning(f"⛔ {reason}")
            return

        # Clean up stale MM quotes
        self._cleanup_mm_quotes()

        markets = self._get_markets()
        if not markets:
            logger.warning("No markets returned")
            return

        logger.info(f"📊 {len(markets)} markets | balance=${balance:,.2f} | "
                    f"positions={len(self.open_positions)}")

        signals = self.engine.scan_all(markets)

        if not signals:
            logger.info("📭 No qualifying signals")
            if self.debug_mode:
                debug_info = self.engine.debug_scan(markets)
                logger.info(f"🔍 DEBUG breakdown:\n{debug_info}")
            return

        logger.info(f"💡 {len(signals)} signals | Top 5:")
        for s in signals[:5]:
            lbl = self.STRATEGY_LABELS.get(s.strategy_name, "•")
            logger.info(f"  {lbl} {s.strategy_name:12} {s.direction.value:14} "
                        f"edge={s.edge:.0%} conf={s.confidence:.0%} EV={s.ev:.2%}  {s.ticker}")

        executed = 0
        for sig in signals[:6]:
            if sig.ticker in self.open_positions and sig.direction not in (
                SignalType.MAKE_MARKET, SignalType.STINK_BID_YES, SignalType.STINK_BID_NO
            ):
                continue

            if self._execute_signal(sig, balance):
                executed += 1
                balance = self._get_balance()

        logger.info(f"✅ {executed} orders placed | P&L today: ${self.risk.daily_pnl_usd:+.2f}")

    # ── Signal Execution Router ───────────────────────────────────────────────
    def _execute_signal(self, sig: TradeSignal, balance: float) -> bool:
        """Route signal to the appropriate execution handler."""
        if sig.direction == SignalType.MAKE_MARKET:
            return self._execute_market_make(sig, balance)
        elif sig.direction in (SignalType.STINK_BID_YES, SignalType.STINK_BID_NO):
            return self._execute_stink_bid(sig, balance)
        elif sig.direction in (SignalType.BUY_YES, SignalType.BUY_NO):
            return self._execute_directional(sig, balance)
        else:
            logger.debug(f"Unhandled signal type: {sig.direction}")
            return False

    # ── Directional (BUY_YES / BUY_NO) ───────────────────────────────────────
    def _execute_directional(self, sig: TradeSignal, balance: float) -> bool:
        ok, reason = self.risk.can_trade(balance)
        if not ok:
            logger.warning(f"⛔ {reason}")
            return False

        if sig.direction == SignalType.BUY_YES:
            side, price_cents = "yes", int(sig.market_price_yes * 100) + 1
        else:
            side, price_cents = "no",  int((1.0 - sig.market_price_yes) * 100) + 1

        price_cents = max(1, min(99, price_cents))

        contracts = self.risk.size_position(
            balance_usd=balance, price_cents=price_cents, edge=sig.edge)
        if contracts < 1:
            return False

        cost = (contracts * price_cents) / 100.0
        emoji = self.STRATEGY_LABELS.get(sig.strategy_name, "•")

        logger.info(f"{emoji} {sig.strategy_name} → BUY {contracts}x {side} @ {price_cents}¢ "
                    f"on {sig.ticker}  edge={sig.edge:.0%}  cost=${cost:.2f}")

        self.notifier.signal_found(sig, contracts, cost)
        self.pnl.log_signal(sig, contracts, cost)

        if self.scan_only:
            logger.info(f"   [SCAN ONLY] — not placing order")
            return True

        return self._place_order_and_record(
            ticker=sig.ticker, side=side, action="buy",
            contracts=contracts, price_cents=price_cents,
            sig=sig, balance=balance, cost=cost
        )

    # ── Market Making ─────────────────────────────────────────────────────────
    def _execute_market_make(self, sig: TradeSignal, balance: float) -> bool:
        """
        Post limit orders on both sides to capture the spread.
        YES-BID and YES-ASK are stored in sig.mm_bid_yes / sig.mm_ask_yes.
        """
        ticker = sig.ticker

        # Only one MM quote per market at a time
        if ticker in self.mm.quotes:
            return False

        ok, reason = self.risk.can_trade(balance)
        if not ok:
            return False

        bid_cents = sig.mm_bid_yes
        ask_cents = sig.mm_ask_yes
        contracts = sig.mm_contracts or 3

        if bid_cents is None or ask_cents is None:
            return False

        logger.info(f"⚖️  MarketMake {ticker}: "
                    f"post YES_BID={bid_cents}¢ / YES_ASK={ask_cents}¢ × {contracts}")

        if self.scan_only:
            logger.info("   [SCAN ONLY] — not placing MM quotes")
            return True

        bid_order_id = ask_order_id = None

        try:
            if self.client:
                # Place YES-BID (buy YES at bid_cents)
                r_bid = self.client.place_order(
                    ticker=ticker, side="yes", action="buy",
                    count=contracts, order_type="limit",
                    yes_price=bid_cents,
                    client_order_id=f"MM-BID-{uuid4().hex[:6]}",
                )
                bid_order_id = r_bid.get("order",{}).get("order_id","")

                # Place YES-ASK (sell YES at ask_cents = buy NO at 100-ask_cents)
                r_ask = self.client.place_order(
                    ticker=ticker, side="yes", action="sell",
                    count=contracts, order_type="limit",
                    yes_price=ask_cents,
                    client_order_id=f"MM-ASK-{uuid4().hex[:6]}",
                )
                ask_order_id = r_ask.get("order",{}).get("order_id","")
            else:
                bid_order_id = f"SIM-MM-BID-{ticker[:6]}"
                ask_order_id = f"SIM-MM-ASK-{ticker[:6]}"
                logger.info(f"   [SIM] MM quotes: {bid_order_id} / {ask_order_id}")

            self.mm.add(ticker, bid_order_id, ask_order_id,
                        bid_cents, ask_cents, contracts)

            capture = ask_cents - bid_cents
            logger.info(f"✅ MM quotes live  spread_capture={capture}¢/round-trip")
            return True

        except KalshiAPIError as e:
            logger.error(f"MM order failed: {e}")
            # Cancel bid if it was placed but ask failed
            if bid_order_id and self.client:
                try:
                    self.client.cancel_order(bid_order_id)
                except Exception:
                    pass
            return False

    # ── Stink Bids ────────────────────────────────────────────────────────────
    def _execute_stink_bid(self, sig: TradeSignal, balance: float) -> bool:
        """Place a deeply discounted limit order to catch panic sellers."""
        ticker      = sig.ticker
        stink_cents = sig.stink_price_cents
        side        = sig.stink_side or "yes"

        if stink_cents is None or stink_cents < 1:
            return False

        # Stink bids are always small: 1–3 contracts (lottery ticket sizing)
        contracts = min(3, self.risk.size_position(
            balance_usd=balance, price_cents=stink_cents, edge=sig.edge))
        contracts = max(1, contracts)

        cost = (contracts * stink_cents) / 100.0

        logger.info(f"🎣 StinkBid {ticker}: limit {side.upper()} "
                    f"{contracts}x @ {stink_cents}¢  (market={sig.market_price_yes:.0%})  cost=${cost:.2f}")

        if self.scan_only:
            logger.info("   [SCAN ONLY] — not placing order")
            return True

        try:
            if self.client:
                r = self.client.place_order(
                    ticker=ticker, side=side, action="buy",
                    count=contracts, order_type="limit",
                    yes_price=stink_cents if side == "yes" else None,
                    no_price=stink_cents  if side == "no"  else None,
                    client_order_id=f"STINK-{uuid4().hex[:6]}",
                )
                order_id = r.get("order",{}).get("order_id","")
            else:
                order_id = f"SIM-STINK-{ticker[:6]}"
                logger.info(f"   [SIM] Stink order: {order_id}")

            self.pnl.log_trade(ticker, "buy", side, contracts, stink_cents)
            logger.info(f"✅ Stink bid placed: {order_id}")
            return True

        except KalshiAPIError as e:
            logger.error(f"Stink bid failed: {e}")
            return False

    # ── Order Placement ───────────────────────────────────────────────────────
    def _place_order_and_record(self, ticker, side, action, contracts,
                                price_cents, sig, balance, cost) -> bool:
        try:
            order_id = f"SIM-{uuid4().hex[:8]}"

            if self.client:
                r = self.client.place_order(
                    ticker=ticker, side=side, action=action,
                    count=contracts, order_type="limit",
                    yes_price=price_cents if side == "yes" else None,
                    no_price =price_cents if side == "no"  else None,
                    client_order_id=order_id,
                )
                order_id = r.get("order", {}).get("order_id", order_id)
            else:
                logger.info(f"   [SIM] Simulated order: {order_id}")

            self.risk.record_trade(ticker, side, action, contracts, price_cents)
            self.pnl.log_trade(ticker, action, side, contracts, price_cents)

            self.open_positions[ticker] = {
                "order_id":    order_id,
                "signal":      sig,
                "side":        side,
                "contracts":   contracts,
                "price_cents": price_cents,
                "entered_at":  datetime.now(),
            }
            self.notifier.trade_placed(ticker, side, action, contracts,
                                       price_cents, balance - cost)
            logger.info(f"✅ Order confirmed: {order_id}")
            return True

        except KalshiAPIError as e:
            logger.error(f"Order failed: {e}")
            self.notifier.error_alert(f"Order failed {ticker}: {e}")
            return False

    # ── MM Cleanup ────────────────────────────────────────────────────────────
    def _cleanup_mm_quotes(self):
        """Cancel MM quotes that have been sitting unfilled for > 30 min."""
        stale = self.mm.stale_tickers(max_age_minutes=30)
        for ticker in stale:
            q = self.mm.quotes[ticker]
            logger.info(f"⏰ Cancelling stale MM quotes on {ticker}")
            if self.client:
                for oid in [q["bid_id"], q["ask_id"]]:
                    if oid:
                        try:
                            self.client.cancel_order(oid)
                        except Exception as e:
                            logger.debug(f"Cancel {oid}: {e}")
            self.mm.remove(ticker)

    # ── Data Access ───────────────────────────────────────────────────────────
    def _get_balance(self) -> float:
        if self.client:
            try:
                return self.client.get_balance_usd()
            except Exception as e:
                logger.error(f"Balance error: {e}")
                return self.risk.starting_balance_usd or 1000.0
        # Simulation balance
        spent = sum(p["contracts"]*p["price_cents"]/100.0
                    for p in self.open_positions.values())
        return max(0.0, 1000.0 - spent + self.risk.daily_pnl_usd)

    def _get_markets(self) -> List[Dict]:
        if not self.client:
            return self._sim_markets()
        try:
            # Step 1: bulk fetch all open markets (fast, metadata only)
            all_markets = self.client.get_all_open_markets(
                max_close_hours=self.max_close_hours)

            if not all_markets:
                return []

            # Step 2: count how many already have price data
            with_price = sum(
                1 for m in all_markets
                if m.get("yes_bid") is not None
                or m.get("yes_ask") is not None
                or m.get("last_price") is not None
            )

            logger.info(
                f"Raw: {len(all_markets)} markets | "
                f"{with_price} already priced | "
                f"{len(all_markets)-with_price} need enrichment"
            )

            # Step 3: enrich prices via individual market fetches
            # Sorted by soonest close_time = live/in-progress games first
            # max_enrich=500 @ 60ms each = ~30s max per cycle
            # On demo: OI and volume are 0 even for active markets, so we
            # fetch by close_time proximity, not by OI
            tradeable = self.client.enrich_prices(all_markets, max_enrich=500)

            if not tradeable:
                logger.warning(
                    "No markets with price data after enrichment.\n"
                    "  The Kalshi API returned no price data for any market.\n"
                    "  Check: https://demo.kalshi.co — are there active markets?\n"
                    "  If yes, there may be a rate-limit issue. Try --interval 600"
                )

            return tradeable

        except Exception as e:
            logger.error(f"Market fetch error: {e}")
            return []

    def _sim_markets(self) -> List[Dict]:
        import random
        random.seed(int(time.time()) // 300)
        now = int(time.time())
        rows = [
            # Sports — NO-side
            ("KXNBA-HAWKS-WIN",    "Will Hawks beat Celtics?",              "KXNBA",    28,32,1100,490, 5),
            ("KXNBA-LAKERS-WIN",   "Will Lakers win tonight?",              "KXNBA",    30,34, 850,380, 6),
            ("KXNFL-RAIDERS-CVR",  "Will Raiders cover vs Chiefs?",         "KXNFL",    32,36,2800,1100,18),
            ("KXNCAAB-DUKE-CVR",   "Will Duke cover -8 vs UNC?",            "KXNCAAB",  38,42, 720,310,12),
            # Finance — value edge + MM (wide spread)
            ("INXCHI-SP500-5800",  "S&P 500 above 5,800 by March 31?",      "INXCHI",   58,67,2200,900,36),
            ("INXCHI-SP500-5900",  "S&P 500 above 5,900 by April 30?",      "INXCHI",   44,53,1900,780,48),
            ("KXNASDAQ-18K",       "Nasdaq 100 above 18,000 EOW?",          "KXNASDAQ", 52,60,1400,600,24),
            # Crypto — stink bid + value edge
            ("KXBTC-90K-APR",      "Bitcoin above $90K by April 1?",        "KXBTC",    8, 13,3100,1200,36),
            ("KXBTC-80K-MAR",      "Bitcoin stays above $80K by March 31?", "KXBTC",    68,73,2700,1100,48),
            ("KXETH-3K-APR",       "Ethereum above $3,000 by April 30?",    "KXETH",    5,  9,1800, 700,48),
            # Macro — stat arb (two thresholds same series)
            ("KXFED-MAY-CUT",      "Fed cuts in May 2026?",                 "KXFED",    54,59,4200,1700,48),
            ("KXFED-JUN-CUT",      "Fed cuts in Jun 2026?",                 "KXFED",    62,66,3800,1500,48),  # Should be > MAY
            ("KXCPI-3PCT",         "CPI < 3.0% for March 2026?",            "KXCPI",    48,54,1600, 650,48),
            # Time decay — near-expiry high-probability
            ("INXCHI-SP500-TD",    "S&P 500 above 5,200 today?",            "INXCHI",   88,92, 800, 340, 2),
            ("KXBTC-60K-TD",       "Bitcoin above $60K today?",             "KXBTC",    91,94,1500, 600, 1),
            # Weather
            ("KXWEATHER-NYC-SNOW", "NYC snow > 1in this week?",             "KXWEATHER",31,37, 380, 140,12),
        ]

        markets = []
        for (ticker,title,series,bid,ask,vol,oi,hrs) in rows:
            j = random.randint(-2,2)
            b = max(1, min(97, bid+j)); a = min(98, ask+j+random.randint(0,1))
            markets.append({
                "ticker":ticker,"title":title,"series_ticker":series,"status":"open",
                "yes_bid":b,"yes_ask":a,"last_price":(b+a)//2,
                "volume":vol+random.randint(-50,50),
                "volume_24h":(vol//2)+random.randint(-20,20),
                "open_interest":oi+random.randint(-20,20),
                "close_time":now+hrs*3600+random.randint(-600,600),
                "event_ticker":series,  # for stat-arb grouping
            })
        return markets

    # ── Shutdown ──────────────────────────────────────────────────────────────
    def _shutdown(self):
        if self.client:
            try:
                n = self.client.cancel_all_orders()
                logger.info(f"Cancelled {n} open orders on shutdown")
            except Exception:
                pass

        stats = self.risk.get_stats()
        bal   = self._get_balance()
        logger.info(f"\n{'═'*55}")
        logger.info(f"  SESSION COMPLETE")
        logger.info(f"  Trades: {stats['total_trades']} | "
                    f"Win rate: {stats['win_rate']:.0%} | "
                    f"P&L: ${stats['daily_pnl_usd']:+.2f}")
        logger.info(f"  Balance: ${bal:,.2f}")
        logger.info(f"  {self.pnl.summary()}")
        logger.info(f"{'═'*55}")
        self.notifier.daily_summary(stats, bal)

    def _print_banner(self):
        ml = "DEMO 🧪" if self.mode == "demo" else "LIVE 🔴"
        print(f"""
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║    ██╗  ██╗ █████╗ ██╗     ███████╗██╗  ██╗██╗          ║
║    ██║ ██╔╝██╔══██╗██║     ██╔════╝██║  ██║██║          ║
║    █████╔╝ ███████║██║     ███████╗███████║██║          ║
║    ██╔═██╗ ██╔══██║██║     ╚════██║██╔══██║██║          ║
║    ██║  ██╗██║  ██║███████╗███████║██║  ██║██║          ║
║    ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝          ║
║                                                          ║
║     KALSHI BOT v2  ·  APEX Suite  [{ml:^14}]     ║
║     7 Strategies: Edge|NoBias|Momentum|MM|Arb|Stink|Theta║
╚══════════════════════════════════════════════════════════╝
""")


# ─────────────────────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode",     choices=["demo","live"], default="demo")
    p.add_argument("--scan",     action="store_true", help="Signal scan only, no orders")
    p.add_argument("--diagnose", action="store_true", help="Test connection and exit")
    p.add_argument("--debug",    action="store_true", help="Show WHY signals are filtered when none found")
    p.add_argument("--interval", type=int, default=300,  help="Scan interval seconds")
    p.add_argument("--hours",    type=int, default=48,   help="Max hours to market close")
    p.add_argument("--log",      default="INFO", choices=["DEBUG","INFO","WARNING"])
    args = p.parse_args()

    setup_logging(args.log)
    load_dotenv()

    # Diagnose-only mode
    if args.diagnose:
        key_id   = os.getenv("KALSHI_API_KEY_ID","").strip()
        key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH","./kalshi_private_key.pem")
        if not key_id:
            print("❌ KALSHI_API_KEY_ID not set in .env")
            sys.exit(1)
        try:
            c = KalshiClient(key_id=key_id, private_key_path=key_path, mode=args.mode)
            ok = c.print_diagnostics()
            sys.exit(0 if ok else 1)
        except Exception as e:
            print(f"❌ {e}")
            sys.exit(1)

    if args.mode == "live":
        print("\n" + "⚠️ " * 22)
        print("  WARNING: LIVE MODE — REAL MONEY AT RISK")
        confirm = input("  Type YES to continue: ").strip()
        if confirm != "YES":
            print("Aborted.")
            sys.exit(0)

    KalshiBot(
        mode=args.mode, scan_only=args.scan,
        scan_interval=args.interval, max_close_hours=args.hours,
        debug_mode=args.debug,
    ).start()


if __name__ == "__main__":
    main()