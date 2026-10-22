"""OKX v5 WebSocket adapter — PHASE 2 STUB.

Will subscribe to public channels (tickers, candle1m, trades) and the
private orders/positions channel for fill notifications.
"""

from __future__ import annotations


class OKXWebSocket:
    PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
    PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"

    async def start(self) -> None:
        raise NotImplementedError("OKXWebSocket.start — phase 2")

    async def stop(self) -> None:  # pragma: no cover - stub
        return
