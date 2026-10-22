"""Shared pytest fixtures.

Each test gets an isolated SQLite DB and master key in a tmp dir, so the
suite never touches the real `data/` directory.
"""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def _isolate_data_dir():
    tmp = Path(tempfile.mkdtemp(prefix="okx-bot-test-"))
    os.environ["DATABASE_URL"] = f"sqlite:///{tmp / 'test.db'}"
    os.environ["MASTER_KEY_PATH"] = str(tmp / "test.key")
    yield
