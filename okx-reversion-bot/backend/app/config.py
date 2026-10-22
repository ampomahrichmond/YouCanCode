from enum import Enum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TradeMode(str, Enum):
    PAPER = "paper"
    DEMO = "demo"
    LIVE = "live"


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bot_trade_mode: TradeMode = TradeMode.PAPER
    database_url: str = f"sqlite:///{PROJECT_ROOT / 'data' / 'bot.db'}"
    master_key_path: Path = PROJECT_ROOT / "data" / ".master.key"
    frontend_origin: str = "http://localhost:5173"
    log_level: str = "INFO"

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT


settings = Settings()
