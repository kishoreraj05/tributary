from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from .context import RunContext
    from .schema import ResolvedSchema


class BatchReadable(ABC):
    @abstractmethod
    def read(self, ctx: RunContext, opts: Any, schema: ResolvedSchema | None) -> DataFrame: ...


class StreamReadable(ABC):
    @abstractmethod
    def read_stream(self, ctx: RunContext, opts: Any, schema: ResolvedSchema | None) -> DataFrame: ...


class Source(Plugin):
    """Base for sources. Combine with BatchReadable and/or StreamReadable."""

    category = "source"

    @classmethod
    def capabilities(cls) -> list[str]:
        caps = []
        if issubclass(cls, BatchReadable):
            caps.append("batch")
        if issubclass(cls, StreamReadable):
            caps.append("streaming")
        return caps

    def read_preview(self, ctx: RunContext, opts: Any, schema: ResolvedSchema | None, limit: int) -> DataFrame:
        """A small, bounded sample. Streaming-only sources must override this."""
        if isinstance(self, BatchReadable):
            return self.read(ctx, opts, schema).limit(limit)
        raise NotImplementedError(f"source '{self.type_name}' does not support preview")

    def output_schema(self, opts: Any, schema: ResolvedSchema | None) -> StructType | None:
        """Schema of the produced DataFrame if known without reading data (used by analysis)."""
        return schema.struct if schema else None

    def estimate_input_bytes(self, opts: Any) -> int | None:
        """Best-effort input size, used for automatic resource sizing. None = unknown."""
        return None

    def commit_read(self, ctx: RunContext, opts: Any) -> None:
        """Called once every sink of a batch run (or of a streaming micro-batch, for the
        static sources read in it) has written. A source that tracks its own progress, such
        as an incremental read that should continue where this run stopped, records it here
        and nowhere earlier: a run that fails before its sinks write must read the same data
        again. Never called for previews."""
        return None
