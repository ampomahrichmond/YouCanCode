"""Encrypted credential vault.

Credentials are stored in the `credentials` table as Fernet-encrypted JSON
blobs. The symmetric key lives in `data/.master.key` (gitignored). On first
launch the key is generated automatically and persisted with restrictive
permissions.

Losing the master key means losing access to stored credentials — re-enter
them through the Settings page. This is by design: never derive the key from
something predictable.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from app.config import settings
from app.db import models
from app.logging_config import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class CredentialPayload:
    """In-memory view of a decrypted credential set."""

    provider: str
    fields: dict[str, str]
    environment: str = "live"

    @property
    def key_hint(self) -> str:
        primary = self.fields.get("api_key") or self.fields.get("key") or ""
        return primary[-4:] if len(primary) >= 4 else ""


def _load_or_create_master_key(path: Path) -> bytes:
    if path.exists():
        return path.read_bytes()

    path.parent.mkdir(parents=True, exist_ok=True)
    key = Fernet.generate_key()
    path.write_bytes(key)
    try:
        if os.name == "posix":
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    log.warning(
        "master_key_generated",
        path=str(path),
        message="A new master key was created. Back it up; losing it means re-entering all credentials.",
    )
    return key


class CredentialVault:
    def __init__(self, key_path: Path | None = None) -> None:
        self._fernet = Fernet(_load_or_create_master_key(key_path or settings.master_key_path))

    def encrypt_payload(self, fields: dict[str, str]) -> bytes:
        return self._fernet.encrypt(json.dumps(fields, sort_keys=True).encode("utf-8"))

    def decrypt_payload(self, blob: bytes) -> dict[str, str]:
        try:
            return json.loads(self._fernet.decrypt(blob).decode("utf-8"))
        except InvalidToken as exc:
            raise RuntimeError(
                "Failed to decrypt credential payload — master key may be wrong or rotated."
            ) from exc

    def upsert(
        self,
        session: Session,
        *,
        provider: str,
        fields: dict[str, str],
        environment: str = "live",
    ) -> models.Credential:
        payload = self.encrypt_payload(fields)
        primary = fields.get("api_key") or fields.get("key") or ""
        hint = primary[-4:] if len(primary) >= 4 else ""

        existing = (
            session.query(models.Credential)
            .filter(models.Credential.provider == provider)
            .one_or_none()
        )
        if existing is None:
            row = models.Credential(
                provider=provider, payload=payload, key_hint=hint, environment=environment
            )
            session.add(row)
        else:
            existing.payload = payload
            existing.key_hint = hint
            existing.environment = environment
            row = existing
        session.commit()
        session.refresh(row)
        return row

    def load(self, session: Session, provider: str) -> CredentialPayload | None:
        row = (
            session.query(models.Credential)
            .filter(models.Credential.provider == provider)
            .one_or_none()
        )
        if row is None:
            return None
        fields = self.decrypt_payload(row.payload)
        return CredentialPayload(provider=row.provider, fields=fields, environment=row.environment)

    def delete(self, session: Session, provider: str) -> bool:
        row = (
            session.query(models.Credential)
            .filter(models.Credential.provider == provider)
            .one_or_none()
        )
        if row is None:
            return False
        session.delete(row)
        session.commit()
        return True


vault = CredentialVault()
