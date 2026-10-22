from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.api import bot as bot_routes
from app.api import health as health_routes
from app.api import settings_routes
from app.api import trades as trades_routes
from app.config import settings
from app.db.session import init_db
from app.logging_config import configure_logging, get_logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    log = get_logger("startup")
    init_db()
    log.info(
        "app_started",
        version=__version__,
        trade_mode=settings.bot_trade_mode.value,
        database=settings.database_url.split("://", 1)[0],
    )
    yield
    log.info("app_stopped")


app = FastAPI(
    title="OKX Reversion Bot",
    version=__version__,
    lifespan=lifespan,
    description="Short-reversion engine for OKX futures. Phase 1 scaffold.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_PREFIX = "/api"
app.include_router(health_routes.router, prefix=API_PREFIX)
app.include_router(settings_routes.router, prefix=API_PREFIX)
app.include_router(bot_routes.router, prefix=API_PREFIX)
app.include_router(trades_routes.router, prefix=API_PREFIX)


@app.get("/")
def root() -> dict:
    return {
        "name": "okx-reversion-bot",
        "version": __version__,
        "docs": "/docs",
    }
