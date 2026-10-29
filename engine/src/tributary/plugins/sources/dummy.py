"""The local environment's stand-in for any source: a node with ``dummy.enabled`` becomes
this source when the pipeline runs (or previews) in the ``local`` environment, so it hands
its uploaded records downstream without connecting to anything."""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any

from pydantic import Field, field_validator

from ...core import BatchReadable, PluginOptions, Source, StreamReadable
from .._dummy import MAX_DUMMY_RECORDS, dummy_frame, parse_records


class DummySourceOptions(PluginOptions):
    records: list[dict[str, Any]] = Field(default_factory=list, description=f"Up to {MAX_DUMMY_RECORDS} records")
    stands_for: str = Field("", description="The type of the node these records stand in for")

    @field_validator("records", mode="before")
    @classmethod
    def _parse(cls, value: Any) -> list[dict[str, Any]]:
        return parse_records(value)


class DummySource(Source, BatchReadable, StreamReadable):
    """Records uploaded in the editor, standing in for a real source in the local environment."""

    type_name = "dummy"
    display_name = "Dummy data"
    icon = "box"
    hidden = True
    Options = DummySourceOptions

    def read(self, ctx, opts: DummySourceOptions, schema):
        ctx.log.info("%s source uses %d dummy records (local environment)", opts.stands_for or "dummy", len(opts.records))
        return dummy_frame(ctx.spark, opts.records, schema.struct if schema else None)

    def read_stream(self, ctx, opts: DummySourceOptions, schema):
        """The records as one micro-batch: spooled to a temporary directory read as a file
        stream (removed when the run ends)."""
        batch = self.read(ctx, opts, schema)
        spool = tempfile.mkdtemp(prefix="tributary-dummy-")
        batch.coalesce(1).write.mode("overwrite").json(os.path.join(spool, "data"))
        ctx.defer(lambda: shutil.rmtree(spool, ignore_errors=True))
        return ctx.spark.readStream.schema(batch.schema).json(os.path.join(spool, "data"))

    def describe(self, opts: DummySourceOptions) -> str:
        return f"{len(opts.records)} dummy records instead of {opts.stands_for or 'the source'}"
