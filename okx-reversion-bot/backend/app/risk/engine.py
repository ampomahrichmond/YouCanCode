"""Risk engine — enforces hard limits before any order leaves the system.

PHASE 1: limits + Pydantic config defined; the `evaluate` method has the
deterministic rule set in place but is not yet wired into a live strategy
loop (that comes in phase 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import models


class RiskLimits(BaseModel):
    per_trade_max_loss_usdt: Decimal = Field(default=Decimal("25"))
    daily_loss_cap_usdt: Decimal = Field(default=Decimal("100"))
    max_concurrent_positions: int = 2
    cooldown_seconds_after_loss: int = 600
    min_liquidation_distance_pct: Decimal = Field(default=Decimal("0.20"))
    max_order_notional_usdt: Decimal = Field(default=Decimal("500"))
    max_leverage: Decimal = Field(default=Decimal("3"))


@dataclass
class RiskDecision:
    allowed: bool
    reasons: list[str]
    severity: str  # info | warn | block

    @classmethod
    def ok(cls) -> "RiskDecision":
        return cls(allowed=True, reasons=[], severity="info")

    @classmethod
    def block(cls, reason: str) -> "RiskDecision":
        return cls(allowed=False, reasons=[reason], severity="block")


@dataclass
class TradeProposal:
    symbol: str
    side: str
    notional_usdt: Decimal
    leverage: Decimal
    expected_loss_usdt: Decimal
    liquidation_distance_pct: Decimal | None = None


class RiskEngine:
    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()

    def evaluate(self, session: Session, proposal: TradeProposal) -> RiskDecision:
        reasons: list[str] = []

        if proposal.expected_loss_usdt > self.limits.per_trade_max_loss_usdt:
            reasons.append(
                f"per-trade max loss exceeded: "
                f"{proposal.expected_loss_usdt} > {self.limits.per_trade_max_loss_usdt}"
            )

        if proposal.notional_usdt > self.limits.max_order_notional_usdt:
            reasons.append(
                f"order notional exceeded: "
                f"{proposal.notional_usdt} > {self.limits.max_order_notional_usdt}"
            )

        if proposal.leverage > self.limits.max_leverage:
            reasons.append(
                f"leverage exceeded: {proposal.leverage} > {self.limits.max_leverage}"
            )

        if (
            proposal.liquidation_distance_pct is not None
            and proposal.liquidation_distance_pct < self.limits.min_liquidation_distance_pct
        ):
            reasons.append(
                f"liquidation too close: "
                f"{proposal.liquidation_distance_pct} < {self.limits.min_liquidation_distance_pct}"
            )

        open_positions = (
            session.query(models.Position).filter(models.Position.is_open.is_(True)).count()
        )
        if open_positions >= self.limits.max_concurrent_positions:
            reasons.append(
                f"max concurrent positions reached: {open_positions}"
            )

        cooldown_until = datetime.now(timezone.utc) - timedelta(
            seconds=self.limits.cooldown_seconds_after_loss
        )
        recent_loss = (
            session.query(models.TradeRun)
            .filter(
                models.TradeRun.outcome == "loss",
                models.TradeRun.ended_at.is_not(None),
                models.TradeRun.ended_at >= cooldown_until,
            )
            .first()
        )
        if recent_loss is not None:
            reasons.append("cooldown active after recent loss")

        daily_pnl = self._daily_realized_pnl(session)
        if daily_pnl <= -self.limits.daily_loss_cap_usdt:
            reasons.append(
                f"daily loss cap hit: {daily_pnl} <= -{self.limits.daily_loss_cap_usdt}"
            )

        if reasons:
            return RiskDecision(allowed=False, reasons=reasons, severity="block")
        return RiskDecision.ok()

    def _daily_realized_pnl(self, session: Session) -> Decimal:
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        rows = (
            session.query(models.TradeRun.realized_pnl)
            .filter(
                models.TradeRun.ended_at.is_not(None),
                models.TradeRun.ended_at >= today_start,
                models.TradeRun.realized_pnl.is_not(None),
            )
            .all()
        )
        total = Decimal("0")
        for (pnl_str,) in rows:
            try:
                total += Decimal(pnl_str)
            except Exception:
                continue
        return total
