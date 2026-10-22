"""ORM models for the trade journal.

Schema covers the 11 entities specified in the spec:
market_snapshots, strategy_candidates, signals, orders, fills,
positions, trade_runs, risk_events, bot_config_versions,
performance_daily, lessons_learned.

Plus a `credentials` table for the encrypted API-key vault.

All numeric prices/sizes are stored as String (decimal) to avoid float
imprecision; convert with Decimal at the boundary.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------- credentials


class Credential(Base):
    """Encrypted API credential store. One row per provider."""

    __tablename__ = "credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    key_hint: Mapped[str] = mapped_column(String(16), default="", nullable=False)
    environment: Mapped[str] = mapped_column(String(16), default="live", nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


# ------------------------------------------------------------ bot config


class BotConfigVersion(Base):
    """Append-only history of strategy/risk parameter changes."""

    __tablename__ = "bot_config_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    author: Mapped[str] = mapped_column(String(64), default="operator")
    note: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)


# ------------------------------------------------------------ market data


class MarketSnapshot(Base):
    """Periodic snapshot of mark price + indicators per symbol."""

    __tablename__ = "market_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    mark_price: Mapped[str] = mapped_column(String(32), nullable=False)
    last_price: Mapped[str] = mapped_column(String(32), nullable=False)
    volume_1m: Mapped[str] = mapped_column(String(32), default="0")
    volatility: Mapped[str] = mapped_column(String(32), default="0")
    funding_rate: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)


# ------------------------------------------------------------ signals


class StrategyCandidate(Base):
    """An impulse detected; may or may not progress to a confirmed signal."""

    __tablename__ = "strategy_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    impulse_pct: Mapped[str] = mapped_column(String(32), nullable=False)
    impulse_window_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    observation_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(24), default="observing", index=True)
    rejected_reason: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)


class Signal(Base):
    """A confirmed short-entry signal emitted to the executor."""

    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    candidate_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("strategy_candidates.id"), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    direction: Mapped[str] = mapped_column(String(8), default="short")
    emitted_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    entry_reason: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[str] = mapped_column(String(16), default="0")
    suggested_size: Mapped[str] = mapped_column(String(32), nullable=False)
    suggested_leverage: Mapped[str] = mapped_column(String(16), nullable=False)
    suggested_trail_distance: Mapped[str] = mapped_column(String(32), nullable=False)
    market_regime: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)


# ------------------------------------------------------------ orders / fills


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    signal_id: Mapped[Optional[int]] = mapped_column(ForeignKey("signals.id"), nullable=True)
    trade_run_id: Mapped[Optional[int]] = mapped_column(ForeignKey("trade_runs.id"), nullable=True)
    client_order_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    exchange_order_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)  # buy | sell
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)  # market | limit | trail
    size: Mapped[str] = mapped_column(String(32), nullable=False)
    limit_price: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    trail_distance: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    finalized_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_paper: Mapped[bool] = mapped_column(Boolean, default=True)


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True, nullable=False)
    filled_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    price: Mapped[str] = mapped_column(String(32), nullable=False)
    size: Mapped[str] = mapped_column(String(32), nullable=False)
    fee: Mapped[str] = mapped_column(String(32), default="0")
    fee_currency: Mapped[str] = mapped_column(String(16), default="USDT")
    liquidity: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)  # M | T


# ------------------------------------------------------------ positions


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_run_id: Mapped[Optional[int]] = mapped_column(ForeignKey("trade_runs.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    size: Mapped[str] = mapped_column(String(32), nullable=False)
    entry_price: Mapped[str] = mapped_column(String(32), nullable=False)
    leverage: Mapped[str] = mapped_column(String(16), nullable=False)
    liquidation_price: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    is_open: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_paper: Mapped[bool] = mapped_column(Boolean, default=True)


# ------------------------------------------------------------ trade lifecycle


class TradeRun(Base):
    """End-to-end view of a single setup → entry → exit cycle."""

    __tablename__ = "trade_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    signal_id: Mapped[Optional[int]] = mapped_column(ForeignKey("signals.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    direction: Mapped[str] = mapped_column(String(8), default="short")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    entry_price: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    exit_price: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    size: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    leverage: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    trail_distance: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    observation_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    market_regime: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    volatility_at_entry: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    max_favorable_excursion: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    max_adverse_excursion: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    realized_pnl: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    fees_total: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    slippage_total: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    outcome: Mapped[Optional[str]] = mapped_column(String(24), nullable=True, index=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_paper: Mapped[bool] = mapped_column(Boolean, default=True)


# ------------------------------------------------------------ risk + governance


class RiskEvent(Base):
    __tablename__ = "risk_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)  # info | warn | block
    code: Mapped[str] = mapped_column(String(48), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    related_signal_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("signals.id"), nullable=True
    )


class PerformanceDaily(Base):
    """Aggregate performance per UTC day. Updated by a periodic job."""

    __tablename__ = "performance_daily"
    __table_args__ = (UniqueConstraint("day"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    day: Mapped[str] = mapped_column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    trades: Mapped[int] = mapped_column(Integer, default=0)
    wins: Mapped[int] = mapped_column(Integer, default=0)
    losses: Mapped[int] = mapped_column(Integer, default=0)
    realized_pnl: Mapped[str] = mapped_column(String(32), default="0")
    fees_total: Mapped[str] = mapped_column(String(32), default="0")
    max_drawdown: Mapped[str] = mapped_column(String(32), default="0")


class LessonLearned(Base):
    """Recommendations from the (future) learning module. Human-approved."""

    __tablename__ = "lessons_learned"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, index=True)
    category: Mapped[str] = mapped_column(String(48), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_json: Mapped[str] = mapped_column(Text, default="{}")
    recommended_change_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(24), default="pending")  # pending|applied|rejected
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


Index("ix_orders_symbol_status", Order.symbol, Order.status)
Index("ix_trade_runs_symbol_outcome", TradeRun.symbol, TradeRun.outcome)
