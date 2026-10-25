from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Literal

from pydantic import Field

from ...core import BatchReadable, PluginOptions, Source, StreamReadable
from .._util import OptionValue, spark_options


class FileSourceOptions(PluginOptions):
    path: str = Field(..., description="File, directory or glob (local, s3a://, gs://, abfss://, hdfs://)")
    format: Literal["csv", "json", "parquet", "orc", "text", "avro"] = "parquet"
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict, description="Spark reader options, e.g. header: true"
    )
    max_files_per_trigger: int | None = Field(None, ge=1, description="Streaming only")


class FileSource(Source, BatchReadable, StreamReadable):
    """Files on local disk or any Hadoop-compatible object store."""

    type_name = "file"
    display_name = "Files (local / S3 / GCS / ADLS)"
    icon = "file"
    Options = FileSourceOptions

    def required_packages(self, opts: FileSourceOptions) -> list[str]:
        return ["org.apache.spark:spark-avro_{scala_version}:{spark_version}"] if opts.format == "avro" else []

    def _configure(self, reader, opts: FileSourceOptions, schema):
        reader = reader.format(opts.format).options(**spark_options(opts.read_options))
        if schema is not None and schema.struct is not None:
            reader = reader.schema(schema.struct)
        return reader

    def read(self, ctx, opts: FileSourceOptions, schema):
        return self._configure(ctx.spark.read, opts, schema).load(opts.path)

    def read_stream(self, ctx, opts: FileSourceOptions, schema):
        reader = self._configure(ctx.spark.readStream, opts, schema)
        if opts.max_files_per_trigger:
            reader = reader.option("maxFilesPerTrigger", opts.max_files_per_trigger)
        return reader.load(opts.path)

    def estimate_input_bytes(self, opts: FileSourceOptions) -> int | None:
        path = opts.path.removeprefix("file://")
        if "://" in path:
            return None  # remote listing needs a Spark session; size stays unknown
        matches = glob.glob(path) if any(ch in path for ch in "*?[") else [path]
        total = 0
        found = False
        for match in matches:
            p = Path(match)
            if p.is_file():
                total += p.stat().st_size
                found = True
            elif p.is_dir():
                for root, _, files in os.walk(p):
                    total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
                found = True
        return total if found else None
