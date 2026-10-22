from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.db import models
from app.db.session import get_session

router = APIRouter(prefix="/trades", tags=["trades"])


class TradeRunOut(BaseModel):
    id: int
    symbol: str
    direction: str
    started_at: str
    ended_at: str | None
    entry_price: str | None
    exit_price: str | None
    realized_pnl: str | None
    outcome: str | None
    is_paper: bool

    model_config = ConfigDict(from_attributes=True)


@router.get("/runs", response_model=list[TradeRunOut])
def list_trade_runs(
    limit: int = Query(50, le=500),
    session: Session = Depends(get_session),
) -> list[TradeRunOut]:
    rows = (
        session.query(models.TradeRun)
        .order_by(desc(models.TradeRun.started_at))
        .limit(limit)
        .all()
    )
    return [
        TradeRunOut(
            id=r.id,
            symbol=r.symbol,
            direction=r.direction,
            started_at=r.started_at.isoformat(),
            ended_at=r.ended_at.isoformat() if r.ended_at else None,
            entry_price=r.entry_price,
            exit_price=r.exit_price,
            realized_pnl=r.realized_pnl,
            outcome=r.outcome,
            is_paper=r.is_paper,
        )
        for r in rows
    ]


class SignalOut(BaseModel):
    id: int
    symbol: str
    direction: str
    emitted_at: str
    entry_reason: str
    market_regime: str | None


@router.get("/signals", response_model=list[SignalOut])
def list_signals(
    limit: int = Query(50, le=500),
    session: Session = Depends(get_session),
) -> list[SignalOut]:
    rows = (
        session.query(models.Signal)
        .order_by(desc(models.Signal.emitted_at))
        .limit(limit)
        .all()
    )
    return [
        SignalOut(
            id=r.id,
            symbol=r.symbol,
            direction=r.direction,
            emitted_at=r.emitted_at.isoformat(),
            entry_reason=r.entry_reason,
            market_regime=r.market_regime,
        )
        for r in rows
    ]


class PositionOut(BaseModel):
    id: int
    symbol: str
    side: str
    size: str
    entry_price: str
    leverage: str
    opened_at: str
    is_paper: bool


@router.get("/positions", response_model=list[PositionOut])
def list_open_positions(
    session: Session = Depends(get_session),
) -> list[PositionOut]:
    rows = (
        session.query(models.Position)
        .filter(models.Position.is_open.is_(True))
        .order_by(desc(models.Position.opened_at))
        .all()
    )
    return [
        PositionOut(
            id=r.id,
            symbol=r.symbol,
            side=r.side,
            size=r.size,
            entry_price=r.entry_price,
            leverage=r.leverage,
            opened_at=r.opened_at.isoformat(),
            is_paper=r.is_paper,
        )
        for r in rows
    ]


class RiskEventOut(BaseModel):
    id: int
    occurred_at: str
    severity: str
    code: str
    message: str
    symbol: str | None


@router.get("/risk-events", response_model=list[RiskEventOut])
def list_risk_events(
    limit: int = Query(100, le=500),
    session: Session = Depends(get_session),
) -> list[RiskEventOut]:
    rows = (
        session.query(models.RiskEvent)
        .order_by(desc(models.RiskEvent.occurred_at))
        .limit(limit)
        .all()
    )
    return [
        RiskEventOut(
            id=r.id,
            occurred_at=r.occurred_at.isoformat(),
            severity=r.severity,
            code=r.code,
            message=r.message,
            symbol=r.symbol,
        )
        for r in rows
    ]
