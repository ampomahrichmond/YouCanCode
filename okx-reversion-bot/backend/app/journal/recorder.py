"""Trade journal writer.

Phase 1: helpers for risk-event and bot-config-version writes (used by the
running app today). Order/Fill/Position/TradeRun writers arrive in phase 3
when the executor goes online.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.db import models


def record_risk_event(
    session: Session,
    *,
    severity: str,
    code: str,
    message: str,
    symbol: str | None = None,
    related_signal_id: int | None = None,
) -> models.RiskEvent:
    row = models.RiskEvent(
        severity=severity,
        code=code,
        message=message,
        symbol=symbol,
        related_signal_id=related_signal_id,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def record_config_version(
    session: Session,
    *,
    payload: dict[str, Any],
    author: str = "operator",
    note: str = "",
) -> models.BotConfigVersion:
    row = models.BotConfigVersion(
        payload_json=json.dumps(payload, sort_keys=True),
        author=author,
        note=note,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row
