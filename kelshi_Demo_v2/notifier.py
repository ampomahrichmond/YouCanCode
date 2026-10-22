"""
notifier.py
===========
Telegram notification system for Kalshi bot alerts and trade updates.

Author: APEX Trading Suite - Kalshi Module
"""

import logging
import os
import requests
from typing import Optional
from strategies import TradeSignal

logger = logging.getLogger("Notifier")


class TelegramNotifier:
    """Sends trade alerts, signals, and P&L updates via Telegram."""

    BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        self.token   = token   or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            logger.warning("Telegram not configured — notifications disabled")

    def send(self, message: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            logger.debug(f"[Telegram disabled] {message}")
            return False

        url = self.BASE_URL.format(token=self.token)
        try:
            resp = requests.post(url, json={
                "chat_id":    self.chat_id,
                "text":       message,
                "parse_mode": parse_mode,
            }, timeout=10)
            return resp.ok
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Pre-formatted Messages
    # ─────────────────────────────────────────────────────────────────────────
    def bot_started(self, mode: str, balance_usd: float):
        emoji = "🧪" if mode == "demo" else "🔴"
        msg = (
            f"<b>⚡ KALSHI BOT STARTED</b>\n"
            f"{'━'*30}\n"
            f"{emoji} Mode: <b>{mode.upper()}</b>\n"
            f"💰 Balance: <b>${balance_usd:,.2f}</b>\n"
            f"{'━'*30}\n"
            f"Strategies active: ValueEdge | NoSideBias | Momentum"
        )
        self.send(msg)

    def signal_found(self, signal, contracts: int, cost_usd: float):
        from strategies import SignalType
        d = signal.direction
        label_map = {
            SignalType.BUY_YES:       "🟢 BUY YES",
            SignalType.BUY_NO:        "🔴 BUY NO",
            SignalType.MAKE_MARKET:   "⚖️  MKT MAKE",
            SignalType.STINK_BID_YES: "🎣 STINK BID YES",
            SignalType.STINK_BID_NO:  "🎣 STINK BID NO",
            SignalType.SELL_YES:      "📤 SELL YES",
        }
        dir_label = label_map.get(d, str(d.value))

        extra = ""
        if d == SignalType.MAKE_MARKET:
            extra = f"\n📐 Spread: {signal.mm_bid_yes}¢ / {signal.mm_ask_yes}¢"
        elif d in (SignalType.STINK_BID_YES, SignalType.STINK_BID_NO):
            extra = f"\n🎯 Stink price: {signal.stink_price_cents}¢"

        msg = (
            f"<b>{dir_label}</b>\n"
            f"{'─'*28}\n"
            f"📌 <b>{signal.market_title[:50]}</b>\n"
            f"🎯 Ticker: <code>{signal.ticker}</code>\n"
            f"📊 Strategy: {signal.strategy_name}\n"
            f"💡 Edge: <b>{signal.edge:.1%}</b> | Conf: {signal.confidence:.0%} | EV: {signal.ev:.2%}\n"
            f"📉 Market YES: {signal.market_price_yes:.0%}\n"
            f"📦 Contracts: {contracts} | Cost: ${cost_usd:.2f}{extra}\n"
            f"💭 {signal.reasoning[:150]}"
        )
        self.send(msg)

    def trade_placed(self, ticker: str, side: str, action: str,
                     contracts: int, price_cents: int, balance_usd: float):
        emoji = "✅"
        msg = (
            f"<b>{emoji} ORDER PLACED</b>\n"
            f"{'─'*28}\n"
            f"📌 <code>{ticker}</code>\n"
            f"💼 {action.upper()} {contracts}x {side.upper()} @ {price_cents}¢\n"
            f"💵 Cost: ${(contracts * price_cents / 100):.2f}\n"
            f"🏦 Remaining: ${balance_usd:.2f}"
        )
        self.send(msg)

    def settlement_alert(self, ticker: str, won: bool, pnl_usd: float):
        if won:
            msg = (
                f"<b>🎯 WIN — SETTLED</b>\n"
                f"{'─'*28}\n"
                f"📌 <code>{ticker}</code>\n"
                f"💰 P&L: <b>+${pnl_usd:.2f}</b>"
            )
        else:
            msg = (
                f"<b>💔 LOSS — SETTLED</b>\n"
                f"{'─'*28}\n"
                f"📌 <code>{ticker}</code>\n"
                f"📉 P&L: <b>-${abs(pnl_usd):.2f}</b>"
            )
        self.send(msg)

    def daily_summary(self, stats: dict, balance_usd: float):
        pnl = stats.get("daily_pnl_usd", 0)
        pnl_emoji = "📈" if pnl >= 0 else "📉"
        win_rate = stats.get("win_rate", 0)

        msg = (
            f"<b>📊 DAILY SUMMARY</b>\n"
            f"{'━'*30}\n"
            f"{pnl_emoji} Daily P&L: <b>${pnl:+.2f}</b>\n"
            f"✅ Wins: {stats.get('wins', 0)} | "
            f"❌ Losses: {stats.get('losses', 0)}\n"
            f"🎯 Win Rate: {win_rate:.0%}\n"
            f"📦 Trades: {stats.get('settled_trades', 0)}\n"
            f"🏦 Balance: ${balance_usd:.2f}\n"
            f"{'━'*30}"
        )
        self.send(msg)

    def halt_alert(self, reason: str):
        msg = (
            f"<b>🚨 TRADING HALTED</b>\n"
            f"{'━'*30}\n"
            f"Reason: {reason}\n"
            f"Bot will resume next trading day."
        )
        self.send(msg)

    def error_alert(self, error: str):
        msg = f"<b>⚠️ BOT ERROR</b>\n<code>{error[:200]}</code>"
        self.send(msg)
