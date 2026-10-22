"""Smoke tests — confirm the app starts and the core surfaces respond."""

from fastapi.testclient import TestClient

from app.main import app


def test_health():
    with TestClient(app) as client:
        r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert body["trade_mode"] in {"paper", "demo", "live"}


def test_bot_pause_resume_round_trip():
    with TestClient(app) as client:
        r = client.post("/api/bot/pause")
        assert r.status_code == 200
        assert r.json()["paused"] is True

        r = client.post("/api/bot/resume")
        assert r.status_code == 200
        assert r.json()["paused"] is False


def test_credentials_round_trip():
    with TestClient(app) as client:
        # initially nothing configured
        r = client.get("/api/settings/credentials")
        assert r.status_code == 200
        assert all(c["configured"] is False for c in r.json())

        # save and confirm key hint, never plaintext
        r = client.put(
            "/api/settings/credentials/okx",
            json={
                "api_key": "test-key-abcd1234",
                "api_secret": "secret",
                "passphrase": "pass",
                "environment": "demo",
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert body["configured"] is True
        assert body["key_hint"] == "1234"
        assert body["environment"] == "demo"

        # listing reflects it
        listing = client.get("/api/settings/credentials").json()
        okx = next(c for c in listing if c["provider"] == "okx")
        assert okx["configured"] is True
        assert okx["key_hint"] == "1234"

        # test-connection stub returns a structured result
        r = client.post("/api/settings/credentials/okx/test")
        assert r.status_code == 200
        assert "status" in r.json()

        # delete clears it
        r = client.delete("/api/settings/credentials/okx")
        assert r.status_code == 204
        listing = client.get("/api/settings/credentials").json()
        okx = next(c for c in listing if c["provider"] == "okx")
        assert okx["configured"] is False
