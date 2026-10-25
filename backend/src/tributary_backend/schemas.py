"""Request/response models of the HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Name = Field(..., min_length=1, max_length=200)
ConnectionName = Field(..., pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=200)


class ORM(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- pipelines -------------------------------------------------------------------------

class PipelineCreate(BaseModel):
    name: str = Name
    description: str | None = None
    spec: dict[str, Any] = Field(default_factory=dict, description="Initial draft; may be incomplete")
    ui_layout: dict[str, Any] = Field(default_factory=dict)


class PipelineUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=200)
    description: str | None = None
    spec: dict[str, Any] | None = None
    ui_layout: dict[str, Any] | None = None


class PipelineSummary(ORM):
    id: str
    name: str
    description: str | None
    latest_version: int
    created_at: datetime
    updated_at: datetime


class PipelineOut(PipelineSummary):
    draft_spec: dict[str, Any]
    draft_layout: dict[str, Any]


class VersionCreate(BaseModel):
    message: str | None = None


class VersionSummary(ORM):
    version: int
    message: str | None
    created_at: datetime


class VersionOut(VersionSummary):
    spec: dict[str, Any]
    ui_layout: dict[str, Any]


class ImportRequest(BaseModel):
    name: str = Name
    description: str | None = None
    content: str
    format: Literal["yaml", "json"] = "yaml"


# --- connections -----------------------------------------------------------------------

class ConnectionIn(BaseModel):
    name: str = ConnectionName
    type: str = Field(..., min_length=1, description="e.g. kafka, postgres, file")
    description: str | None = None
    environments: dict[str, dict[str, Any]] = Field(default_factory=dict, description="{env: {field: value}}")
    secret_fields: list[str] | None = Field(
        None, description="Fields stored encrypted and masked in responses; default: detected by name"
    )


class ConnectionOut(BaseModel):
    name: str
    type: str
    description: str | None
    environments: dict[str, dict[str, Any]]
    secret_fields: list[str]
    created_at: datetime
    updated_at: datetime


# --- design-time operations ------------------------------------------------------------

class SpecRequest(BaseModel):
    spec: dict[str, Any]
    env: str | None = None


class ValidateRequest(SpecRequest):
    deep: bool = Field(False, description="Also resolve columns/types with Spark (no data read)")


class PreviewRequest(SpecRequest):
    node_id: str
    limit: int = Field(20, ge=1)
    sample_data: dict[str, str] = Field(default_factory=dict, description="{source_id: sample_id}")
    write: bool = Field(False, description="For sinks: actually write the sample")


class SampleOut(BaseModel):
    sample_id: str
    filename: str
    size: int


# --- runs ------------------------------------------------------------------------------

class RunCreate(BaseModel):
    env: str
    version: int | None = Field(None, description="Defaults to the latest version")


class RunOut(ORM):
    id: str
    pipeline_id: str
    version: int
    env: str
    mode: str
    status: str
    error: str | None
    summary: dict[str, Any]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class QualityOut(ORM):
    run_id: str
    pipeline_id: str
    env: str
    node_id: str
    quality_id: str
    check_id: str
    check_type: str
    level: str
    action: str
    total_rows: int
    failed_rows: int | None
    observed_value: float | None
    passed: bool
    batch_id: int | None
    ts: str
