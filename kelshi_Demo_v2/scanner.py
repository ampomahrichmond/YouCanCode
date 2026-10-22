"""
scanner.py
==========
Standalone market scanner — shows live opportunities WITHOUT placing any trades.
Perfect for validating strategies before going live.

Usage:
    python scanner.py                    # Scan all categories
    python scanner.py --category sports  # Sports only
    python scanner.py --top 10           # Show top 10 signals
    python scanner.py --demo-markets     # Use synthetic demo data (no API keys needed)

Author: APEX Trading Suite - Kalshi Module
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from dotenv import load_dotenv
from tabulate import tabulate
from colorama import init, Fore, Style, Back

from kalshi_client import KalshiClient
from strategies import StrategyEngine, TradeSignal, SignalType, _category as _get_category

init(autoreset=True)

# Robust .env loading — works on Windows regardless of CWD
def _load_env():
    script_dir = Path(__file__).resolve().parent
    for candidate in [script_dir / ".env", Path.cwd() / ".env"]:
        if candidate.exists():
            load_dotenv(dotenv_path=str(candidate), override=True)
            return
    load_dotenv(override=True)

_load_env()

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("Scanner")


# ─────────────────────────────────────────────────────────────────────────────
# Colors
# ─────────────────────────────────────────────────────────────────────────────
def green(s): return f"{Fore.GREEN}{s}{Style.RESET_ALL}"
def red(s):   return f"{Fore.RED}{s}{Style.RESET_ALL}"
def yellow(s):return f"{Fore.YELLOW}{s}{Style.RESET_ALL}"
def cyan(s):  return f"{Fore.CYAN}{s}{Style.RESET_ALL}"
def bold(s):  return f"{Style.BRIGHT}{s}{Style.RESET_ALL}"


def direction_colored(sig) -> str:
    d = sig.direction.value
    if d in ("buy_yes",):          return green("▲ BUY YES")
    elif d in ("buy_no",):         return red("▼ BUY NO")
    elif d == "make_market":       return cyan("⚖  MKT MAKE")
    elif d == "stink_bid_yes":     return yellow("🎣 STINK BID")
    elif d == "stink_bid_no":      return yellow("🎣 STINK NO")
    elif d == "sell_yes":          return red("▼ SELL YES")
    else:                          return yellow(d.upper())


def edge_colored(edge: float) -> str:
    pct = f"{edge:.1%}"
    if edge >= 0.10:   return green(bold(pct))
    elif edge >= 0.06: return green(pct)
    else:              return yellow(pct)


def confidence_colored(conf: float) -> str:
    pct = f"{conf:.0%}"
    if conf >= 0.75:  return green(bold(pct))
    elif conf >= 0.60: return green(pct)
    else:              return yellow(pct)


# ─────────────────────────────────────────────────────────────────────────────
# Demo market generator (same as in bot.py)
# ─────────────────────────────────────────────────────────────────────────────
def generate_demo_markets() -> List[dict]:
    """
    Generate synthetic Kalshi-style markets for demo/testing.
    Markets are designed to trigger realistic signals across all strategies.
    """
    import random
    random.seed(int(time.time()) // 300)  # Stable per 5-min window

    now = int(time.time())

    # (ticker, title, series, yes_bid, yes_ask, volume, oi, close_offset_hours)
    templates = [
        # Sports — NO-side bias targets (YES overpriced, NO 55-80%)
        ("KXNBA-LAKERS-WIN",       "Will Lakers win tonight?",              "KXNBA",     30, 34,  850,  380, 6),
        ("KXNBA-HAWKS-WIN",        "Will Hawks beat Celtics?",              "KXNBA",     27, 31, 1100,  490, 5),
        ("KXNFL-RAIDERS-WIN",      "Will Raiders cover vs Chiefs?",         "KXNFL",     32, 36, 2800, 1100, 18),
        ("KXNCAAB-DUKE-COVER",     "Will Duke cover -8 vs UNC?",            "KXNCAAB",   38, 42,  720,  310, 12),
        ("KXNCAAB-KANSAS-ML",      "Will Kansas win straight up?",          "KXNCAAB",   35, 39,  680,  290, 8),
        ("KXMLB-METS-WIN",         "Will Mets win Opening Day?",            "KXMLB",     42, 46,  530,  210, 22),
        # Sports — YES side (underdogs that should win)
        ("KXNBA-NUGGETS-WIN",      "Will Nuggets win at home?",             "KXNBA",     61, 65, 1350,  580, 7),
        ("KXNFL-CHIEFS-WIN",       "Will Chiefs win AFC Championship?",     "KXNFL",     62, 66, 4100, 1800, 15),
        # Finance — Value edge opportunities
        ("INXCHI-SP500-5800",      "S&P 500 above 5,800 by March 31?",     "INXCHI",    58, 63, 2200,  900, 36),
        ("INXCHI-SP500-5900",      "S&P 500 above 5,900 by April 30?",     "INXCHI",    44, 49, 1900,  780, 48),
        ("KXNASDAQ-ABOVE-18K",     "Nasdaq 100 above 18,000 EOW?",         "KXNASDAQ",  52, 57, 1400,  600, 24),
        # Crypto
        ("KXBTC-90K-APR",          "Bitcoin above $90K by April 1?",       "KXBTC",     56, 61, 3100, 1200, 36),
        ("KXBTC-80K-MAR",          "Bitcoin stays above $80K by March 31?","KXBTC",     68, 72, 2700, 1100, 48),
        ("KXETH-3K-APR",           "Ethereum above $3,000 by April 30?",   "KXETH",     39, 44, 1800,  700, 48),
        # Macro / Finance
        ("KXFED-MAY-CUT",          "Fed cuts rates in May 2026?",           "KXFED",     54, 58, 4200, 1700, 48),
        ("KXCPI-25MAR-3PCT",       "CPI < 3.0% for March 2026?",           "KXCPI",     48, 53, 1600,  650, 48),
        ("KXUNEMP-ABOVE-4",        "US unemployment above 4.0% in Feb?",   "KXUNEMP",   41, 46, 2100,  850, 36),
        # Weather
        ("KXWEATHER-NYC-SNOW",     "NYC snow accumulation > 1in this week?","KXWEATHER", 31, 36,  380,  140, 12),
        ("KXWEATHER-LAX-RAIN",     "Rain in LA this weekend?",             "KXWEATHER", 22, 27,  290,  110, 36),
    ]

    markets = []
    for (ticker, title, series, yes_bid, yes_ask, volume, oi, close_h) in templates:
        # Add small random jitter to make it realistic
        jitter      = random.randint(-3, 3)
        adj_bid     = max(5, min(92, yes_bid + jitter))
        adj_ask     = min(98, yes_ask + jitter + random.randint(0, 2))
        adj_vol     = volume + random.randint(-80, 80)
        close_jit   = random.randint(-1, 1)
        close_ts    = now + (close_h + close_jit) * 3600

        markets.append({
            "ticker":        ticker,
            "title":         title,
            "series_ticker": series,
            "status":        "open",
            "yes_bid":       adj_bid,
            "yes_ask":       min(98, adj_ask),
            "last_price":    (adj_bid + adj_ask) // 2,
            "volume":        max(100, adj_vol),
            "volume_24h":    max(50, adj_vol // 2),
            "open_interest": oi + random.randint(-30, 30),
            "close_time":    close_ts,
        })

    return markets


# ─────────────────────────────────────────────────────────────────────────────
# Main Scanner
# ─────────────────────────────────────────────────────────────────────────────
def run_scanner(
    top_n: int = 20,
    category: Optional[str] = None,
    use_demo_markets: bool = False,
    live: bool = False,
    repeat: bool = False,
    interval: int = 60,
):
    print(f"""
{Back.BLUE}{Style.BRIGHT}{'═'*60}{Style.RESET_ALL}
{Back.BLUE}{Style.BRIGHT}  ⚡ KALSHI SIGNAL SCANNER — APEX Trading Suite            {Style.RESET_ALL}
{Back.BLUE}{Style.BRIGHT}{'═'*60}{Style.RESET_ALL}
""")

    engine = StrategyEngine(min_edge=0.04, min_confidence=0.45)

    # Setup client if using real API
    client = None
    key_id   = os.getenv("KALSHI_API_KEY_ID", "").strip().strip('"').strip("'")
    raw_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem").strip().strip('"').strip("'")

    # Resolve path relative to script directory (reliable on Windows)
    script_dir = Path(__file__).resolve().parent
    key_path   = Path(raw_path)
    if not key_path.is_absolute():
        key_path = (script_dir / key_path).resolve()

    if not use_demo_markets and key_id and key_path.exists():
        try:
            mode = "live" if live else "demo"
            client = KalshiClient(key_id=key_id, private_key_path=str(key_path), mode=mode)
            print(f"✅ Connected to Kalshi API | mode={mode.upper()}\n")
        except Exception as e:
            print(f"⚠️  API connection failed: {e}")
            print("   Falling back to demo markets...\n")
            use_demo_markets = True
    else:
        if not key_id:
            print("ℹ️  No API keys found — using synthetic demo markets")
            print("   (Run python diagnose.py to debug .env loading)\n")
        else:
            print("ℹ️  Using synthetic demo markets\n")
        use_demo_markets = True

    while True:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{cyan('🔍 SCANNING')} | {ts}\n")

        # Fetch markets
        if use_demo_markets:
            markets = generate_demo_markets()
            print(f"  📊 Using {len(markets)} synthetic demo markets")
        else:
            try:
                markets = client.get_all_open_markets(max_close_hours=48)
                print(f"  📊 Fetched {len(markets)} live markets from Kalshi API")
            except Exception as e:
                print(f"  ❌ Market fetch error: {e}")
                markets = []

        if not markets:
            print("  No markets available")
            if not repeat:
                break
            time.sleep(interval)
            continue

        # Filter by category if specified
        if category:
            markets = [m for m in markets if _get_category(m) == category]
            print(f"  🏷️  Filtered to {len(markets)} '{category}' markets")

        # Run strategies
        signals = engine.scan_all(markets)

        if not signals:
            print(f"\n  {yellow('📭 No qualifying signals found this scan')}")
        else:
            print(f"\n  {green(bold(f'💡 {len(signals)} SIGNALS FOUND'))}\n")

            # Build display table
            table_data = []
            for i, sig in enumerate(signals[:top_n], 1):
                # Title truncated
                title = sig.market_title[:38] + "…" if len(sig.market_title) > 38 else sig.market_title

                yes_price_pct = f"{sig.market_price_yes:.0%}"
                ev = sig.edge * sig.confidence

                table_data.append([
                    bold(str(i)),
                    direction_colored(sig),
                    sig.strategy_name,
                    edge_colored(sig.edge),
                    confidence_colored(sig.confidence),
                    cyan(f"{ev:.1%}"),
                    sig.category,
                    yes_price_pct,
                    title,
                ])

            headers = [
                bold("#"), bold("Direction"), bold("Strategy"),
                bold("Edge"), bold("Conf"), bold("EV"),
                bold("Cat"), bold("YES%"), bold("Market"),
            ]
            print(tabulate(table_data, headers=headers, tablefmt="rounded_outline"))

            # Top signal detail
            top = signals[0]
            print(f"\n  {bold('🔬 TOP SIGNAL DEEP DIVE:')}")
            print(f"     Ticker:    {cyan(top.ticker)}")
            print(f"     Market:    {top.market_title}")
            print(f"     Direction: {direction_colored(top)}")
            print(f"     Edge:      {edge_colored(top.edge)}")
            print(f"     Reasoning: {top.reasoning}\n")

        if not repeat:
            break

        print(f"\n  {yellow(f'⏳ Next scan in {interval}s... (Ctrl+C to stop)')}")
        time.sleep(interval)


# ─────────────────────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi Signal Scanner")
    parser.add_argument("--top",           type=int, default=15,
                        help="Number of top signals to display (default: 15)")
    parser.add_argument("--category",      type=str, default=None,
                        choices=["sports", "finance", "crypto", "weather", "politics"],
                        help="Filter to specific market category")
    parser.add_argument("--demo-markets",  action="store_true",
                        help="Use synthetic demo markets (no API keys needed)")
    parser.add_argument("--live",          action="store_true",
                        help="Connect to live Kalshi API (default: demo API)")
    parser.add_argument("--repeat",        action="store_true",
                        help="Repeat scan every N seconds")
    parser.add_argument("--interval",      type=int, default=60,
                        help="Repeat interval in seconds (default: 60)")
    args = parser.parse_args()

    try:
        run_scanner(
            top_n=args.top,
            category=args.category,
            use_demo_markets=args.demo_markets,
            live=args.live,
            repeat=args.repeat,
            interval=args.interval,
        )
    except KeyboardInterrupt:
        print(f"\n\n  {yellow('Scanner stopped.')}\n")