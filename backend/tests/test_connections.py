from __future__ import annotations

from sqlalchemy import select

from tributary_backend.db import session_factory
from tributary_backend.models import Connection

BODY = {
    "name": "warehouse",
    "type": "postgres",
    "environments": {
        "dev": {"url": "jdbc:postgresql://localhost/dev", "user": "dev", "password": "dev-pass"},
        "prod": {"url": "jdbc:postgresql://db/prod", "user": "svc", "password": "prod-pass"},
    },
}


def test_secrets_are_encrypted_and_masked(client):
    created = client.post("/api/v1/connections", json=BODY)
    assert created.status_code == 201, created.text
    out = created.json()
    assert out["secret_fields"] == ["password"]  # detected by name
    assert out["environments"]["prod"] == {"url": "jdbc:postgresql://db/prod", "user": "svc", "password": "********"}

    with session_factory()() as session:
        row = session.scalars(select(Connection).where(Connection.name == "warehouse")).one()
        assert "prod-pass" not in str(row.settings) + str(row.secrets)
        assert "password" not in row.settings["prod"]


def test_masked_value_round_trips(client):
    client.post("/api/v1/connections", json=BODY)
    read_back = client.get("/api/v1/connections/warehouse").json()
    read_back["environments"]["dev"]["user"] = "dev2"
    updated = client.put("/api/v1/connections/warehouse", json={k: read_back[k] for k in ("name", "type", "environments")})
    assert updated.status_code == 200, updated.text

    from tributary_backend.crypto import decrypt

    with session_factory()() as session:
        row = session.scalars(select(Connection).where(Connection.name == "warehouse")).one()
        assert row.settings["dev"]["user"] == "dev2"
        assert decrypt(row.secrets["dev"]["password"]) == "dev-pass"


def test_mask_without_stored_secret_is_rejected(client):
    body = {**BODY, "environments": {"qa": {"password": "********"}}}
    assert client.post("/api/v1/connections", json=body).status_code == 422


def test_explicit_secret_fields_duplicates_and_delete(client):
    body = {"name": "lake", "type": "file", "environments": {"dev": {"path": "/x"}}, "secret_fields": ["path"]}
    assert client.post("/api/v1/connections", json=body).json()["environments"]["dev"]["path"] == "********"
    assert client.post("/api/v1/connections", json=body).status_code == 409
    assert client.delete("/api/v1/connections/lake").status_code == 204
    assert client.get("/api/v1/connections/lake").status_code == 404
