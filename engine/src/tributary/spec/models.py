"""Static parts of the pipeline spec.

Node models are completed per plugin in ``builder.py``: each plugin contributes a
``type: Literal[...]`` variant whose ``options`` is that plugin's Options model.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
CacheMode = Literal["auto", "always", "never"]
StorageLevelName = Literal["MEMORY_ONLY", "MEMORY_AND_DISK", "MEMORY_AND_DISK_DESER", "DISK_ONLY", "OFF_HEAP"]
QualityAction = Literal["observe", "fail", "drop", "quarantine"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class StreamingSettings(StrictModel):
    checkpoint_location: str | None = Field(None, description="Required to run a streaming pipeline")
    trigger: Literal["processing_time", "available_now", "once"] = "processing_time"
    interval: str = Field("10 seconds", description="Micro-batch interval for trigger 'processing_time'")


class PipelineMeta(StrictModel):
    id: Identifier
    name: str | None = None
    description: str | None = None
    mode: Literal["batch", "streaming"] = "batch"
    streaming: StreamingSettings = Field(default_factory=StreamingSettings)


class EnvironmentSpec(StrictModel):
    vars: dict[str, Any] = Field(default_factory=dict)
    connections: dict[str, dict[str, Any]] = Field(default_factory=dict)


class ResourceSettings(StrictModel):
    profile: Literal["auto", "S", "M", "L", "XL"] = "auto"
    min_executors: int | None = Field(None, ge=0)
    max_executors: int | None = Field(None, ge=1)
    executor_cores: int | None = Field(None, ge=1)
    executor_memory: str | None = None
    driver_memory: str | None = None


class RuntimeSpec(StrictModel):
    master: str | None = Field(None, description="Spark master; defaults to local[*] outside spark-submit")
    app_name: str | None = None
    resources: Literal["auto"] | ResourceSettings = "auto"
    spark_conf: dict[str, str | int | float | bool] = Field(default_factory=dict)


class ExecutionSettings(StrictModel):
    parallel_sinks: bool = Field(False, description="Write independent sinks concurrently")
    max_parallel_sinks: int = Field(4, ge=1)


class InputRef(StrictModel):
    node: Identifier
    alias: Identifier | None = Field(None, alias="as", description="Table name used in SQL; defaults to the node id")

    @property
    def view_name(self) -> str:
        return self.alias or self.node


class NodeBase(StrictModel):
    id: Identifier
    description: str | None = None


class DataNodeBase(NodeBase):
    cache: CacheMode = "auto"
    storage_level: StorageLevelName = "MEMORY_AND_DISK"


class DummySettings(StrictModel):
    """Stand-in data for the ``local`` environment (ignored in every other environment)."""

    enabled: bool = Field(False, description="In the local environment, use dummy data instead of the real system")
    data: str | list[dict[str, Any]] | None = Field(
        None, description="Sources: the records, as JSON Lines (one JSON object per line, up to 1000) or a list"
    )


class SourceNodeBase(DataNodeBase):
    connection: str | None = None
    stream: bool | None = Field(None, description="Marks the streaming source when a streaming pipeline has several")
    dummy: DummySettings | None = None


class TransformNodeBase(DataNodeBase):
    inputs: list[InputRef] = Field(..., min_length=1)
    connection: str | None = Field(None, description="For transforms that call an external system (REST enrichment)")

    @field_validator("inputs", mode="before")
    @classmethod
    def _strings_to_refs(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [{"node": v} if isinstance(v, str) else v for v in value]
        return value


class SinkNodeBase(NodeBase):
    input: Identifier
    connection: str | None = None
    dummy: DummySettings | None = Field(None, description="In the local environment: write nothing, preview the rows")


class TargetBase(StrictModel):
    """An inline sink/metrics target inside a quality block (no id, no input)."""

    connection: str | None = None


class QualityBase(StrictModel):
    id: Identifier
    description: str | None = None
    attach_to: Identifier | None = Field(None, description="Check the output of this source/transform")
    before: Identifier | None = Field(None, description="Check only the data flowing into this sink")
    action: QualityAction = "observe"


class SpecBase(StrictModel):
    version: Literal[1] = 1
    pipeline: PipelineMeta
    environments: dict[str, EnvironmentSpec] = Field(default_factory=dict)
    runtime: dict[str, RuntimeSpec] = Field(default_factory=dict)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
