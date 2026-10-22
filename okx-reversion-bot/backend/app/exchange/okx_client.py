"""OKX v5 REST client.

PHASE 1 STUB. The class shape mirrors what the strategy/execution engines
will need. Methods raise NotImplementedError so callers fail loudly rather
than silently no-op.

When implemented this will wrap OKX v5 endpoints:
  - GET /api/v5/account/balance
  - POST /api/v5/trade/order
  - POST /api/v5/trade/order-algo (trailing stop)
  - GET /api/v5/account/positions
  - GET /api/v5/market/ticker
"""

from __future__ import annotations

from dataclasses import dataclass

from app.config import TradeMode
from app.secrets import CredentialPayload


@dataclass
class TickerSnapshot:
    symbol: str
    last: str
    mark: str
    bid: str
    ask: str


class OKXClient:
    BASE_URL_LIVE = "https://www.okx.com"
    BASE_URL_DEMO = "https://www.okx.com"  # demo uses same host with x-simulated-trading header

    def __init__(self, creds: CredentialPayload | None, mode: TradeMode) -> None:
        self.creds = creds
        self.mode = mode

    @property
    def is_authenticated(self) -> bool:
        return self.creds is not None and bool(self.creds.fields.get("api_key"))

    async def test_connection(self) -> dict[str, str]:
        """Lightweight credential check.

        TODO(phase 2): GET /api/v5/account/balance and confirm the response
        signature. For now we report config-level reachability only.
        """
        if not self.is_authenticated:
            return {"status": "no_credentials", "detail": "No OKX credentials saved."}
        return {
            "status": "stub_ok",
            "detail": "Credentials present. Live verification arrives in phase 2.",
            "environment": self.creds.environment if self.creds else "unknown",
            "mode": self.mode.value,
        }

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        raise NotImplementedError("OKXClient.get_ticker — phase 2")

    async def place_order(self, **kwargs) -> dict:
        raise NotImplementedError("OKXClient.place_order — phase 4")

    async def place_trailing_stop(self, **kwargs) -> dict:
        raise NotImplementedError("OKXClient.place_trailing_stop — phase 4")

    async def get_positions(self) -> list[dict]:
        raise NotImplementedError("OKXClient.get_positions — phase 2")
