from __future__ import annotations

from typing import Literal

from pydantic import Field

from ...core import PluginOptions, Sink
from .._util import OptionValue, spark_options


class FileSinkOptions(PluginOptions):
    path: str = Field(..., description="Output directory (local, s3a://, gs://, abfss://, hdfs://)")
    format: Literal["parquet", "csv", "json", "orc", "avro", "text"] = "parquet"
    mode: Literal["append", "overwrite", "error", "ignore"] = "append"
    partition_by: list[str] = Field(default_factory=list)
    coalesce: int | None = Field(None, ge=1, description="Reduce the number of output files")
    write_options: dict[str, OptionValue] = Field(default_factory=dict)


class FileSink(Sink):
    """Writes files to local disk or any Hadoop-compatible object store."""

    type_name = "file"
    display_name = "Files (local / S3 / GCS / ADLS)"
    icon = "file"
    Options = FileSinkOptions

    def required_packages(self, opts: FileSinkOptions) -> list[str]:
        return ["org.apache.spark:spark-avro_{scala_version}:{spark_version}"] if opts.format == "avro" else []

    def write(self, ctx, df, opts: FileSinkOptions) -> None:
        if opts.coalesce:
            df = df.coalesce(opts.coalesce)
        writer = df.write.format(opts.format).mode(opts.mode).options(**spark_options(opts.write_options))
        if opts.partition_by:
            writer = writer.partitionBy(*opts.partition_by)
        writer.save(opts.path)

    def describe(self, opts: FileSinkOptions) -> str:
        return f"{opts.format} -> {opts.path} ({opts.mode})"
