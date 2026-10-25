from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..crypto import encrypt
from ..db import get_session
from ..models import Connection
from ..schemas import ConnectionIn, ConnectionOut
from ..services.specs import MASK, looks_secret

router = APIRouter(prefix="/connections", tags=["connections"])


def _out(conn: Connection) -> ConnectionOut:
    envs: dict[str, dict[str, Any]] = {}
    secret_fields: set[str] = set()
    for env in sorted(set(conn.settings or {}) | set(conn.secrets or {})):
        secrets = (conn.secrets or {}).get(env) or {}
        secret_fields |= set(secrets)
        envs[env] = {**((conn.settings or {}).get(env) or {}), **{k: MASK for k in secrets}}
    return ConnectionOut(
        name=conn.name,
        type=conn.type,
        description=conn.description,
        environments=envs,
        secret_fields=sorted(secret_fields),
        created_at=conn.created_at,
        updated_at=conn.updated_at,
    )


def _apply(conn: Connection, body: ConnectionIn) -> None:
    """Splits values into plain settings and encrypted secrets. A secret sent back as the
    mask keeps its stored value, so the UI can round-trip a connection it read."""
    old_secrets = conn.secrets or {}
    settings: dict[str, dict[str, Any]] = {}
    secrets: dict[str, dict[str, str]] = {}
    for env, values in body.environments.items():
        settings[env], secrets[env] = {}, {}
        for field, value in values.items():
            secret = field in body.secret_fields if body.secret_fields is not None else looks_secret(field)
            if not secret:
                settings[env][field] = value
            elif value == MASK:
                if field not in (old_secrets.get(env) or {}):
                    raise HTTPException(422, f"{env}.{field}: no stored secret to keep; send the real value")
                secrets[env][field] = old_secrets[env][field]
            elif value is not None:
                secrets[env][field] = encrypt(str(value))
    conn.type = body.type
    conn.description = body.description
    conn.settings = settings
    conn.secrets = secrets


def _get(session: Session, name: str) -> Connection:
    conn = session.scalars(select(Connection).where(Connection.name == name)).first()
    if conn is None:
        raise HTTPException(404, f"connection '{name}' not found")
    return conn


@router.get("", response_model=list[ConnectionOut])
def list_connections(session: Session = Depends(get_session)):
    return [_out(c) for c in session.scalars(select(Connection).order_by(Connection.name)).all()]


@router.post("", response_model=ConnectionOut, status_code=201)
def create_connection(body: ConnectionIn, session: Session = Depends(get_session)):
    if session.scalars(select(Connection.id).where(Connection.name == body.name)).first():
        raise HTTPException(409, f"connection '{body.name}' already exists")
    conn = Connection(name=body.name)
    _apply(conn, body)
    session.add(conn)
    session.commit()
    return _out(conn)


@router.get("/{name}", response_model=ConnectionOut)
def get_connection(name: str, session: Session = Depends(get_session)):
    return _out(_get(session, name))


@router.put("/{name}", response_model=ConnectionOut)
def update_connection(name: str, body: ConnectionIn, session: Session = Depends(get_session)):
    if body.name != name:
        raise HTTPException(422, "renaming a connection is not supported; create a new one")
    conn = _get(session, name)
    _apply(conn, body)
    session.commit()
    return _out(conn)


@router.delete("/{name}", status_code=204)
def delete_connection(name: str, session: Session = Depends(get_session)):
    session.delete(_get(session, name))
    session.commit()
    return Response(status_code=204)
