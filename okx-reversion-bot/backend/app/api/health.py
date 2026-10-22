from fastapi import APIRouter

from app import __version__
from app.bot_state import bot_state
from app.config import settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "trade_mode": bot_state.trade_mode.value,
        "paused": bot_state.paused,
        "database": settings.database_url.split("://", 1)[0],
    }
