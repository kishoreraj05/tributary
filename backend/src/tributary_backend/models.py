from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base

Json = JSON().with_variant(JSONB(), "postgresql")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return uuid.uuid4().hex


class Pipeline(Base):
    """A pipeline with a mutable draft (what the canvas edits) and immutable versions."""

    __tablename__ = "pipelines"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    draft_spec: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)
    draft_layout: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)
    latest_version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    versions: Mapped[list[PipelineVersion]] = relationship(
        back_populates="pipeline", cascade="all, delete-orphan", order_by="PipelineVersion.version"
    )


class PipelineVersion(Base):
    __tablename__ = "pipeline_versions"
    __table_args__ = (UniqueConstraint("pipeline_id", "version"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[str] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True)
    version: Mapped[int] = mapped_column(Integer)
    spec: Mapped[dict[str, Any]] = mapped_column(Json)
    ui_layout: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)
    message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    pipeline: Mapped[Pipeline] = relationship(back_populates="versions")


class Connection(Base):
    """Reusable connection settings per environment. Secret fields are stored encrypted."""

    __tablename__ = "connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    type: Mapped[str] = mapped_column(String(100))
    description: Mapped[str | None] = mapped_column(Text)
    settings: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)  # {env: {field: value}}
    secrets: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)  # {env: {field: ciphertext}}
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True)
    version: Mapped[int] = mapped_column(Integer)
    env: Mapped[str] = mapped_column(String(100))
    mode: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20), default="queued", index=True)
    pid: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
    summary: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)
    resolved_spec: Mapped[dict[str, Any]] = mapped_column(Json, default=dict)  # secrets masked
    log_path: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class QualityResult(Base):
    """Every data-quality check result of every run (written by the worker's metrics sink)."""

    __tablename__ = "quality_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    pipeline_id: Mapped[str] = mapped_column(String(200), index=True)  # the spec's pipeline.id
    env: Mapped[str] = mapped_column(String(100))
    node_id: Mapped[str] = mapped_column(String(200))
    quality_id: Mapped[str] = mapped_column(String(200))
    check_id: Mapped[str] = mapped_column(String(200))
    check_type: Mapped[str] = mapped_column(String(100))
    level: Mapped[str] = mapped_column(String(20))
    action: Mapped[str] = mapped_column(String(20))
    total_rows: Mapped[int] = mapped_column(BigInteger)
    failed_rows: Mapped[int | None] = mapped_column(BigInteger)
    observed_value: Mapped[float | None] = mapped_column(Float)
    passed: Mapped[bool] = mapped_column(Boolean)
    batch_id: Mapped[int | None] = mapped_column(BigInteger)
    ts: Mapped[str] = mapped_column(String(64))
