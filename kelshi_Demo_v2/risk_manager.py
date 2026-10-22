"""
risk_manager.py
===============
Position sizing, daily loss limits, and risk controls for Kalshi trading.

Author: APEX Trading Suite - Kalshi Module
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional

logger = logging.getLogger("RiskManager")


@dataclass
class RiskConfig:
    max_position_size_pct: float = 0.05    # Max 5% of balance per trade
    max_concurrent_positions: int = 10
    max_daily_loss_pct: float = 0.10       # Halt if daily loss > 10%
    min_edge_threshold: float = 0.05       # Minimum 5% edge to enter
    min_volume: int = 100                  # Minimum contracts volume
    min_open_interest: int = 50
    max_expiry_hours: int = 48


@dataclass
class TradeRecord:
    ticker: str
    side: str
    action: str
    count: int
    price_cents: int
    timestamp: datetime = field(default_factory=datetime.now)
    pnl_cents: int = 0
    settled: bool = False

    @property
    def cost_usd(self) -> float:
        return (self.count * self.price_cents) / 100.0

    @property
    def pnl_usd(self) -> float:
        return self.pnl_cents / 100.0


class RiskManager:
    """
    Enforces trading risk controls:
    - Position sizing based on account balance
    - Daily loss limits
    - Max concurrent position caps
    - Minimum edge requirements
    """

    def __init__(self, config: RiskConfig):
        self.config = config
        self.trade_history: List[TradeRecord] = []
        self.open_positions: Dict[str, TradeRecord] = {}
        self.starting_balance_usd: float = 0.0
        self.daily_pnl_usd: float = 0.0
        self.trade_day: date = date.today()
        self.halted: bool = False

    def set_balance(self, balance_usd: float):
        """Call at startup and after each trade to sync balance."""
        if self.starting_balance_usd == 0:
            self.starting_balance_usd = balance_usd
        self._reset_daily_if_needed()

    def _reset_daily_if_needed(self):
        today = date.today()
        if today != self.trade_day:
            logger.info(f"📅 New trading day. Resetting daily P&L.")
            self.daily_pnl_usd = 0.0
            self.trade_day = today
            self.halted = False  # Reset halt at start of new day

    # ─────────────────────────────────────────────────────────────────────────
    # Core Checks
    # ─────────────────────────────────────────────────────────────────────────
    def can_trade(self, balance_usd: float) -> tuple[bool, str]:
        """Master check before any trade. Returns (allowed, reason)."""
        self.set_balance(balance_usd)

        if self.halted:
            return False, "🚨 Trading HALTED — daily loss limit hit"

        if balance_usd <= 0:
            return False, "❌ Zero balance"

        if len(self.open_positions) >= self.config.max_concurrent_positions:
            return False, f"❌ Max positions reached ({self.config.max_concurrent_positions})"

        daily_loss_pct = abs(min(0, self.daily_pnl_usd)) / max(self.starting_balance_usd, 1)
        if daily_loss_pct >= self.config.max_daily_loss_pct:
            self.halted = True
            return False, f"🚨 Daily loss limit hit ({daily_loss_pct:.1%})"

        return True, "✅ OK"

    def size_position(
        self,
        balance_usd: float,
        price_cents: int,
        edge: float,
        kelly_fraction: float = 0.25,
    ) -> int:
        """
        Calculate number of contracts to buy using fractional Kelly criterion.

        Kelly formula: f = (edge * (1/price - 1) - (1-edge)) / (1/price - 1)
        We use 25% Kelly (conservative).

        Returns: number of contracts (min 1, max based on max_position_size_pct)
        """
        price = price_cents / 100.0
        if price <= 0 or price >= 1:
            return 0

        # Fractional Kelly
        b = (1.0 / price) - 1   # net odds ratio
        p = edge                  # estimated win probability
        q = 1 - p
        kelly_pct = (p * b - q) / b if b > 0 else 0
        kelly_pct = max(0, kelly_pct * kelly_fraction)

        # Cap at max position size
        max_pct = self.config.max_position_size_pct
        position_pct = min(kelly_pct, max_pct)

        dollar_amount = balance_usd * position_pct
        contracts = int(dollar_amount / price)
        return max(1, contracts)

    def check_market_liquidity(self, market: Dict) -> tuple[bool, str]:
        """Check if market meets minimum liquidity requirements."""
        volume = market.get("volume", 0)
        oi = market.get("open_interest", 0)

        if volume < self.config.min_volume:
            return False, f"Low volume: {volume} < {self.config.min_volume}"
        if oi < self.config.min_open_interest:
            return False, f"Low OI: {oi} < {self.config.min_open_interest}"

        return True, "OK"

    # ─────────────────────────────────────────────────────────────────────────
    # Position Tracking
    # ─────────────────────────────────────────────────────────────────────────
    def record_trade(self, ticker: str, side: str, action: str,
                     count: int, price_cents: int) -> TradeRecord:
        record = TradeRecord(
            ticker=ticker,
            side=side,
            action=action,
            count=count,
            price_cents=price_cents,
        )
        self.trade_history.append(record)

        if action == "buy":
            self.open_positions[ticker] = record
            cost = record.cost_usd
            self.daily_pnl_usd -= cost  # Debit cost
            logger.info(f"📝 Trade recorded: BUY {count}x {side} @ {ticker} | -${cost:.2f}")
        elif action == "sell":
            if ticker in self.open_positions:
                entry = self.open_positions[ticker]
                proceeds = (count * price_cents) / 100.0
                pnl = proceeds - entry.cost_usd
                self.daily_pnl_usd += proceeds
                record.pnl_cents = int(pnl * 100)
                del self.open_positions[ticker]
                logger.info(f"📝 Trade recorded: SELL {count}x {side} @ {ticker} | PnL: ${pnl:+.2f}")

        return record

    def record_settlement(self, ticker: str, won: bool):
        """Record market settlement (YES or NO resolved)."""
        if ticker in self.open_positions:
            pos = self.open_positions[ticker]
            if won:
                # Each contract pays $1.00
                proceeds = pos.count * 1.0
                pnl = proceeds - pos.cost_usd
            else:
                pnl = -pos.cost_usd

            self.daily_pnl_usd += pnl
            pos.settled = True
            pos.pnl_cents = int(pnl * 100)
            del self.open_positions[ticker]
            result_emoji = "🎯" if won else "💔"
            logger.info(f"{result_emoji} Settlement: {ticker} | PnL: ${pnl:+.2f}")

    # ─────────────────────────────────────────────────────────────────────────
    # Stats
    # ─────────────────────────────────────────────────────────────────────────
    def get_stats(self) -> Dict:
        settled = [t for t in self.trade_history if t.settled]
        wins = [t for t in settled if t.pnl_cents > 0]
        losses = [t for t in settled if t.pnl_cents <= 0]

        total_pnl = sum(t.pnl_usd for t in settled)
        win_rate = len(wins) / len(settled) if settled else 0

        return {
            "total_trades":      len(self.trade_history),
            "settled_trades":    len(settled),
            "open_positions":    len(self.open_positions),
            "wins":              len(wins),
            "losses":            len(losses),
            "win_rate":          win_rate,
            "total_pnl_usd":     total_pnl,
            "daily_pnl_usd":     self.daily_pnl_usd,
            "halted":            self.halted,
        }
