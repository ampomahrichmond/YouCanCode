"""Settings page endpoints — credentials + bot config.

Credential endpoints accept plaintext on POST, encrypt at rest, and never
return plaintext on GET (only a key hint). Test-connection currently uses
the OKXClient.test_connection stub; in phase 2 it performs a real signed
balance check.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.config import TradeMode
from app.db.session import get_session
from app.exchange.okx_client import OKXClient
from app.bot_state import bot_state
from app.secrets import vault

router = APIRouter(prefix="/settings", tags=["settings"])


# ----------------------------------------------------------- credentials


class OKXCredentialIn(BaseModel):
    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)
    passphrase: str = Field(min_length=1)
    environment: str = Field(default="live", pattern="^(live|demo)$")


class PerplexityCredentialIn(BaseModel):
    api_key: str = Field(min_length=1)


class CredentialOut(BaseModel):
    provider: str
    configured: bool
    key_hint: str = ""
    environment: str = ""


def _credential_status(session: Session, provider: str) -> CredentialOut:
    cred = vault.load(session, provider)
    if cred is None:
        return CredentialOut(provider=provider, configured=False)
    return CredentialOut(
        provider=provider,
        configured=True,
        key_hint=cred.key_hint,
        environment=cred.environment,
    )


@router.get("/credentials", response_model=list[CredentialOut])
def list_credentials(session: Session = Depends(get_session)) -> list[CredentialOut]:
    return [
        _credential_status(session, "okx"),
        _credential_status(session, "perplexity"),
    ]


@router.put("/credentials/okx", response_model=CredentialOut)
def upsert_okx_credentials(
    body: OKXCredentialIn, session: Session = Depends(get_session)
) -> CredentialOut:
    vault.upsert(
        session,
        provider="okx",
        fields={
            "api_key": body.api_key,
            "api_secret": body.api_secret,
            "passphrase": body.passphrase,
        },
        environment=body.environment,
    )
    return _credential_status(session, "okx")


@router.put("/credentials/perplexity", response_model=CredentialOut)
def upsert_perplexity_credentials(
    body: PerplexityCredentialIn, session: Session = Depends(get_session)
) -> CredentialOut:
    vault.upsert(
        session,
        provider="perplexity",
        fields={"api_key": body.api_key},
        environment="live",
    )
    return _credential_status(session, "perplexity")


@router.delete("/credentials/{provider}", status_code=status.HTTP_204_NO_CONTENT)
def delete_credentials(provider: str, session: Session = Depends(get_session)) -> None:
    if provider not in {"okx", "perplexity"}:
        raise HTTPException(status_code=404, detail="Unknown provider")
    vault.delete(session, provider)


@router.post("/credentials/okx/test")
async def test_okx_connection(session: Session = Depends(get_session)) -> dict:
    cred = vault.load(session, "okx")
    client = OKXClient(cred, bot_state.trade_mode)
    return await client.test_connection()


# ----------------------------------------------------------- bot mode


class TradeModeIn(BaseModel):
    mode: TradeMode


class BotModeOut(BaseModel):
    mode: TradeMode
    paused: bool


@router.get("/mode", response_model=BotModeOut)
def get_mode() -> BotModeOut:
    return BotModeOut(mode=bot_state.trade_mode, paused=bot_state.paused)


@router.put("/mode", response_model=BotModeOut)
def set_mode(body: TradeModeIn) -> BotModeOut:
    bot_state.set_mode(body.mode)
    return BotModeOut(mode=bot_state.trade_mode, paused=bot_state.paused)
