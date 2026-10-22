from fastapi import APIRouter
from pydantic import BaseModel

from app.bot_state import bot_state
from app.config import TradeMode

router = APIRouter(prefix="/bot", tags=["bot"])


class BotStatusOut(BaseModel):
    paused: bool
    trade_mode: TradeMode
    last_change_at: str
    last_change_by: str


def _status() -> BotStatusOut:
    return BotStatusOut(
        paused=bot_state.paused,
        trade_mode=bot_state.trade_mode,
        last_change_at=bot_state.last_change_at.isoformat(),
        last_change_by=bot_state.last_change_by,
    )


@router.get("/status", response_model=BotStatusOut)
def status() -> BotStatusOut:
    return _status()


@router.post("/pause", response_model=BotStatusOut)
def pause() -> BotStatusOut:
    bot_state.pause()
    return _status()


@router.post("/resume", response_model=BotStatusOut)
def resume() -> BotStatusOut:
    bot_state.resume()
    return _status()
