"""Apache Iceberg tables, written through a Spark catalog.

The catalog settings, shared with the Iceberg source, live in ``tributary.plugins.iceberg``
(see there for how they reach the Spark session). This module adds what a write needs:
the mode, merge keys, table creation settings and the snapshot tagging that keeps
streaming appends idempotent.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

from ...core import Sink
from ...errors import TributaryError
from .._util import OptionValue, column_errors, format_errors, spark_options
from ..iceberg import (  # noqa: F401 - re-exported: callers used to import these from here
    CATALOG_TYPES,
    ICEBERG_PACKAGE,
    ICEBERG_VERSION,
    REST_ONLY,
    SPARK_CATALOG_IMPL,
    URI_APPLIES,
    URI_REQUIRED,
    WAREHOUSE_REQUIRED,
    CatalogType,
    IcebergCatalogOptions,
    configure_catalog,
    when,
)

# Snapshot summary keys: which pipeline/run/micro-batch produced a commit.
PIPELINE_PROP = "tributary.pipeline"
RUN_PROP = "tributary.run"
BATCH_PROP = "tributary.batch"

WriteMode = Literal["append", "overwrite", "overwrite_partitions", "merge"]

# A column, or one of Iceberg's partition transforms: years/months/days/hours(col), bucket/truncate(N, col).
_PARTITION = re.compile(
    r"^\s*(?:[A-Za-z_]\w*|(?:years|months|days|hours)\(\s*[A-Za-z_]\w*\s*\)"
    r"|(?:bucket|truncate)\(\s*\d+\s*,\s*[A-Za-z_]\w*\s*\))\s*$"
)


class IcebergSinkOptions(IcebergCatalogOptions):
    # --- write (the catalog and table fields come from IcebergCatalogOptions) ----------------
    mode: WriteMode = Field(
        "append",
        description="append: add rows. overwrite: replace the whole table. overwrite_partitions: replace only "
        "the partitions present in the data. merge: upsert on merge_keys",
    )
    merge_keys: list[str] = Field(
        default_factory=list,
        validate_default=True,
        description="Columns that identify a row; matching rows are updated, others inserted",
        json_schema_extra=when("mode", applies=("merge",), required=("merge",)),
    )
    create_if_missing: bool = Field(True, description="Create the namespace and the table on first write")
    partition_by: list[str] = Field(
        default_factory=list,
        description="Partitioning when the table is created: columns or Iceberg transforms such as "
        "days(ts), months(ts), bucket(16, id), truncate(10, name). Ignored for an existing table",
    )
    table_properties: dict[str, str] = Field(
        default_factory=dict,
        description="Iceberg table properties applied when the table is created (format-version, "
        "write.target-file-size-bytes, ...)",
    )
    write_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Options for the Iceberg writer, e.g. merge-schema=true to add new columns on write",
    )
    skip_replayed_batches: bool = Field(
        True,
        description="Streaming appends are tagged with their micro-batch id, and a batch the table already "
        "has is skipped, so a restart never duplicates rows. Turn off when replaying a stream from a fresh "
        "checkpoint into an existing table",
    )

    @field_validator("partition_by")
    @classmethod
    def _valid_partitions(cls, value: list[str]) -> list[str]:
        bad = [p for p in value if not _PARTITION.match(p)]
        if bad:
            raise ValueError(
                f"invalid partition field(s): {', '.join(bad)}; use a column or years(col), months(col), "
                "days(col), hours(col), bucket(N, col), truncate(N, col)"
            )
        return [p.strip() for p in value]

    @field_validator("merge_keys", mode="after")
    @classmethod
    def _merge_keys_with_merge(cls, value: list[str], info: ValidationInfo) -> list[str]:
        mode = info.data.get("mode", "append")
        if mode == "merge" and not value:
            raise ValueError("required for mode 'merge'")
        if mode != "merge" and value:
            raise ValueError("only applies to mode 'merge'")
        return value


class IcebergSink(Sink):
    """Apache Iceberg tables: append, overwrite, dynamic partition overwrite or merge (upsert)."""

    type_name = "iceberg"
    display_name = "Apache Iceberg"
    icon = "table"
    Options = IcebergSinkOptions
    spark_packages = (ICEBERG_PACKAGE,)

    def required_conf(self, opts: IcebergSinkOptions) -> dict[str, str]:
        return opts.catalog_conf()

    def describe(self, opts: IcebergSinkOptions) -> str:
        return f"iceberg {opts.catalog_type} -> {opts.catalog}.{opts.table} ({opts.mode}) at {opts.location}"

    def validate_input(self, ctx, schema, opts: IcebergSinkOptions) -> list[tuple[str, str]]:
        """The merge keys and the columns inside the partition transforms must be in the input."""
        return column_errors(schema, {
            "merge_keys": opts.merge_keys,
            "partition_by": [partition_column(p) for p in opts.partition_by],
        })

    # -- write -----------------------------------------------------------------------------

    def write(self, ctx, df: DataFrame, opts: IcebergSinkOptions) -> None:
        if errors := self.validate_input(ctx, df.schema, opts):
            raise TributaryError(f"iceberg sink {opts.table}: {format_errors(errors)}")
        spark = ctx.spark
        configure_catalog(spark, opts)
        # df.writeTo() re-analyses the frame's raw plan, which may refer to temp views that
        # are gone by now (the SQL transform drops its inputs' views). A temp view of our own
        # keeps the analysed plan, so every write below reads the frame through it.
        with temp_view(spark, df) as view:
            source = spark.table(view)
            if not spark.catalog.tableExists(opts.qualified_table()):
                if not opts.create_if_missing:
                    raise TributaryError(
                        f"iceberg table {opts.catalog}.{opts.table} does not exist (create_if_missing is off)"
                    )
                self._create(ctx, df.schema, opts)
                self._writer(ctx, source, opts).append()
            elif opts.mode == "append":
                self._append(ctx, source, opts)
            elif opts.mode == "overwrite":
                self._writer(ctx, source, opts).overwrite(F.lit(True))
            elif opts.mode == "overwrite_partitions":
                self._writer(ctx, source, opts).overwritePartitions()
            else:
                self._merge(ctx, view, df.columns, opts)

    def _create(self, ctx, schema: StructType, opts: IcebergSinkOptions) -> None:
        spark, table = ctx.spark, opts.qualified_table()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {opts.qualified_namespace()}")
        statement = f"CREATE TABLE IF NOT EXISTS {table} ({schema_ddl(schema)}) USING iceberg"
        if opts.partition_by:
            statement += f" PARTITIONED BY ({', '.join(opts.partition_by)})"
        if opts.table_properties:
            props = ", ".join(f"'{_sql_string(k)}'='{_sql_string(v)}'" for k, v in opts.table_properties.items())
            statement += f" TBLPROPERTIES ({props})"
        ctx.log.info("creating iceberg table %s.%s", opts.catalog, opts.table)
        spark.sql(statement)

    def _writer(self, ctx, source: DataFrame, opts: IcebergSinkOptions):
        options = {**spark_options(opts.write_options), f"snapshot-property.{RUN_PROP}": ctx.run_id}
        if ctx.batch_id is not None:
            options[f"snapshot-property.{PIPELINE_PROP}"] = ctx.pipeline_id
            options[f"snapshot-property.{BATCH_PROP}"] = str(ctx.batch_id)
        return source.writeTo(opts.qualified_table()).options(**options)

    def _append(self, ctx, source: DataFrame, opts: IcebergSinkOptions) -> None:
        if opts.skip_replayed_batches and ctx.batch_id is not None and self._has_batch(ctx, opts):
            ctx.log.warning(
                "iceberg table %s.%s already has micro-batch %s of pipeline %s; skipping it",
                opts.catalog, opts.table, ctx.batch_id, ctx.pipeline_id,
            )
            return
        self._writer(ctx, source, opts).append()

    def _has_batch(self, ctx, opts: IcebergSinkOptions) -> bool:
        """Whether the table's latest snapshot was produced by this micro-batch (a replay after a restart)."""
        latest = ctx.spark.sql(
            f"SELECT summary FROM {opts.qualified_table()}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if not latest:
            return False
        summary = latest[0].summary or {}
        return summary.get(PIPELINE_PROP) == ctx.pipeline_id and summary.get(BATCH_PROP) == str(ctx.batch_id)

    def _merge(self, ctx, view: str, columns: list[str], opts: IcebergSinkOptions) -> None:
        condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in opts.merge_keys)
        ctx.spark.sql(
            f"MERGE INTO {opts.qualified_table()} t USING {view} s ON {condition} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )


def partition_column(field: str) -> str:
    """'days(ts)' -> 'ts', 'bucket(16, id)' -> 'id', 'country' -> 'country'."""
    inner = field[field.index("(") + 1:field.rindex(")")] if "(" in field else field
    return inner.split(",")[-1].strip()


@contextmanager
def temp_view(spark, df: DataFrame) -> Iterator[str]:
    name = f"__tributary_iceberg_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(name)
    try:
        yield name
    finally:
        spark.catalog.dropTempView(name)


def schema_ddl(schema: StructType) -> str:
    """Column definitions for CREATE TABLE, nullability included, like Spark's own toDDL."""
    parts = []
    for f in schema.fields:
        null = "" if f.nullable else " NOT NULL"
        parts.append(f"`{f.name.replace('`', '``')}` {f.dataType.simpleString()}{null}")
    return ", ".join(parts)


def _sql_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")
