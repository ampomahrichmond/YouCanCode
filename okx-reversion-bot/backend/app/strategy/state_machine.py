"""Short-reversion strategy state machine — PHASE 3 STUB.

States:
    IDLE -> IMPULSE_DETECTED -> OBSERVING -> CONFIRMED -> ENTERED
                                         \-> REJECTED -> IDLE

Detection rules (placeholders, to be tuned in phase 6):
  - impulse: price up >= impulse_pct over impulse_window seconds
  - observation: 180-300s with no further new high
  - confirmation: at least one of {lower-high, fading volume, failed retest}
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class State(str, Enum):
    IDLE = "idle"
    IMPULSE_DETECTED = "impulse_detected"
    OBSERVING = "observing"
    CONFIRMED = "confirmed"
    ENTERED = "entered"
    REJECTED = "rejected"


@dataclass
class StrategyParams:
    impulse_pct: float = 0.025  # 2.5% over the impulse window
    impulse_window_seconds: int = 60
    observation_seconds: int = 240  # 4 minutes — middle of the 3-5min spec
    min_observation_seconds: int = 180
    max_observation_seconds: int = 300
    trail_distance_pct: float = 0.012
    hard_stop_pct: float = 0.025
    max_leverage: float = 3.0


class StrategyEngine:
    """Per-symbol short-reversion state machine.

    PHASE 3: implement detection and transitions, paper executor wiring.
    """

    def __init__(self, symbol: str, params: StrategyParams) -> None:
        self.symbol = symbol
        self.params = params
        self.state = State.IDLE

    def on_tick(self, *args, **kwargs) -> None:
        raise NotImplementedError("StrategyEngine.on_tick — phase 3")
