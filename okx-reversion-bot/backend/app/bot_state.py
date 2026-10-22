"""In-memory operator-controlled bot state.

Phase 1: pause/resume + trade-mode toggle. Persisted across restarts only
through `BOT_TRADE_MODE` env. The "live unlock" gate will be added in
phase 4 — until then `live` mode is accepted but the live executor refuses
to act on it.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.config import TradeMode, settings


@dataclass
class BotState:
    paused: bool = True
    trade_mode: TradeMode = field(default_factory=lambda: settings.bot_trade_mode)
    last_change_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_change_by: str = "system"

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def pause(self, by: str = "operator") -> None:
        with self._lock:
            self.paused = True
            self.last_change_at = datetime.now(timezone.utc)
            self.last_change_by = by

    def resume(self, by: str = "operator") -> None:
        with self._lock:
            self.paused = False
            self.last_change_at = datetime.now(timezone.utc)
            self.last_change_by = by

    def set_mode(self, mode: TradeMode, by: str = "operator") -> None:
        with self._lock:
            self.trade_mode = mode
            self.last_change_at = datetime.now(timezone.utc)
            self.last_change_by = by


bot_state = BotState()
