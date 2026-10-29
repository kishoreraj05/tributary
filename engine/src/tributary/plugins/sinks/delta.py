"""Delta Lake tables, written by path.

Delta refuses to write unless the session catalog is its ``DeltaCatalog``
(``spark.sql.catalog.spark_catalog``). The sink asks for that at session build through
``required_conf`` for runs, and sets it lazily at write time on shared sessions such as
the one the backend uses for previews (``configure_session`` in ``_delta.py``, shared with
the Delta source). The catalog delegates everything that is not a Delta table to Spark's
own, so other pipelines on the session are not affected.

Like the Iceberg sink, it deliberately does not ask for Delta's SQL extension: every
operation here goes through the DataFrame writer and the ``delta-spark`` Python API
(merge, history), which work without it, and the extension would load Delta into every
session state, including sessions that never touch Delta. Users who want Delta SQL
(``MERGE INTO``, ``DESCRIBE HISTORY``, ``VACUUM``, ...) can add
``spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension`` to ``runtime.spark_conf``;
that also unlocks the ``replaceWhere`` writer option (a selective overwrite), which Delta
implements through the extension's analysis rules.
"""

from __future__ import annotations

import re
from typing import Literal

import sqlglot
from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from sqlglot import exp

from ...core import PluginOptions, Sink
from ...errors import TributaryError
from .._delta import (  # noqa: F401 - re-exported: the backend and the tests import them from here
    DELTA_CATALOG,
    DELTA_PACKAGE,
    DELTA_PACKAGES,
    DELTA_STORAGE_PACKAGE,
    DELTA_VERSION,
    SESSION_CATALOG_KEY,
    configure_session,
    is_delta_table,
    table_version,
)
from .._util import OptionValue, spark_options, when

WriteMode = Literal["append", "overwrite", "overwrite_partitions", "merge"]
_IDENT = re.compile(r"^[A-Za-z_]\w*$")
TARGET, SOURCE = "t", "s"


class DeltaSinkOptions(PluginOptions):
    path: str = Field(..., description="Table directory (local path, s3a://, gs://, abfss://, hdfs://)")
    mode: WriteMode = Field(
        "append",
        description="append: add rows. overwrite: replace the whole table. overwrite_partitions: replace only "
        "the partitions present in the data. merge: upsert on merge_keys",
    )
    merge_keys: list[str] = Field(
        default_factory=list,
        validate_default=True,
        description="Columns that identify a row; matching rows are updated, others inserted. "
        "Must be unique within the data",
        json_schema_extra=when("mode", applies=("merge",), required=("merge",)),
    )
    # Merge conditions: SQL over the table row (alias t) and the incoming row (alias s).
    update_condition: str | None = Field(
        None,
        validate_default=True,
        description="Only matching rows that meet this condition are updated, e.g. s.updated_at > t.updated_at; "
        "the others are left as they are",
        json_schema_extra=when("mode", applies=("merge",)),
    )
    insert_condition: str | None = Field(
        None,
        validate_default=True,
        description="Only incoming rows without a match that meet this condition are inserted, e.g. s.status <> 'cancelled'",
        json_schema_extra=when("mode", applies=("merge",)),
    )
    delete_condition: str | None = Field(
        None,
        validate_default=True,
        description="A matching row is deleted instead of updated when the incoming row meets this condition, "
        "e.g. s.op = 'D'. Incoming rows meeting it are never inserted",
        json_schema_extra=when("mode", applies=("merge",)),
    )
    partition_by: list[str] = Field(
        default_factory=list,
        description="Partition columns when the table is created. Ignored for an existing table",
    )
    schema_evolution: bool = Field(False, description="Add columns that are in the data but not in the table")
    write_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Options for the Delta writer (append/overwrite), e.g. userMetadata: a note on the commit, or "
        "delta.appendOnly: true to set a table property when the table is created",
    )
    skip_replayed_batches: bool = Field(
        True,
        description="Streaming appends and overwrites are tagged with their micro-batch id (Delta's txnAppId and "
        "txnVersion), and a batch the table already has is skipped, so a restart never duplicates rows. Turn "
        "off when replaying a stream from a fresh checkpoint into an existing table",
    )

    @field_validator("partition_by")
    @classmethod
    def _columns(cls, value: list[str]) -> list[str]:
        bad = [p for p in value if not _IDENT.match(p.strip())]
        if bad:
            raise ValueError(f"invalid partition column(s): {', '.join(bad)}; Delta partitions by plain columns")
        return [p.strip() for p in value]

    # Field validators (not a model validator) so each error is reported on its own field,
    # which is where the UI shows it. ``info.data`` holds the fields declared above.

    @field_validator("merge_keys", mode="after")
    @classmethod
    def _merge_keys_with_merge(cls, value: list[str], info: ValidationInfo) -> list[str]:
        mode = info.data.get("mode", "append")
        if mode == "merge" and not value:
            raise ValueError("required for mode 'merge'")
        if mode != "merge" and value:
            raise ValueError("only applies to mode 'merge'")
        return value

    @field_validator("update_condition", "insert_condition", "delete_condition", mode="after")
    @classmethod
    def _conditions_with_merge(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value and info.data.get("mode", "append") != "merge":
            raise ValueError("only applies to mode 'merge'")
        return value


class DeltaSink(Sink):
    """Delta Lake tables: append, overwrite, dynamic partition overwrite or merge (upsert)."""

    type_name = "delta"
    display_name = "Delta Lake"
    icon = "table"
    Options = DeltaSinkOptions
    spark_packages = DELTA_PACKAGES

    def required_conf(self, opts: DeltaSinkOptions) -> dict[str, str]:
        return {SESSION_CATALOG_KEY: DELTA_CATALOG}

    def describe(self, opts: DeltaSinkOptions) -> str:
        return f"delta -> {opts.path} ({opts.mode})"

    # -- design-time checks ----------------------------------------------------------------

    def validate_input(self, ctx, schema: StructType, opts: DeltaSinkOptions) -> list[tuple[str, str]]:
        """What would fail at write time, found while the pipeline is drawn: key, partition
        and condition columns that the upstream data does not have, and - when the table
        already exists - Delta's schema enforcement (new columns, type changes) and the
        table-side references of the merge conditions."""
        columns = [f.name for f in schema.fields]
        issues: list[tuple[str, str]] = []
        missing = [k for k in opts.merge_keys if k not in columns]
        if missing:
            issues.append(("merge_keys", f"not in the data: {', '.join(missing)} (columns: {', '.join(columns)})"))
        missing = [p for p in opts.partition_by if p not in columns]
        if missing:
            issues.append(("partition_by", f"not in the data: {', '.join(missing)} (columns: {', '.join(columns)})"))

        table = existing_schema(ctx.spark, opts.path)
        table_columns = None if table is None else [f.name for f in table.fields]
        for option in ("update_condition", "insert_condition", "delete_condition"):
            condition = getattr(opts, option)
            if condition:
                issues += [(option, m) for m in condition_issues(condition, columns, table_columns)]
        if table is not None:
            issues += [(option, m) for option, m in compatibility_issues(schema, table, opts)]
        return issues

    # -- write -----------------------------------------------------------------------------

    def write(self, ctx, df: DataFrame, opts: DeltaSinkOptions) -> None:
        configure_session(ctx.spark)
        exists = is_delta_table(ctx.spark, opts.path)
        if opts.mode == "merge" and exists:
            self._merge(ctx, df, opts)
        else:
            if opts.mode == "merge":
                # A merge into a table that does not exist yet creates it: every row is "not
                # matched", so only the rows the merge would insert go in (a change feed's
                # first batch can already carry deletes).
                df = self._insertable(df, opts)
            self._save(ctx, df, opts, exists)

    def _save(self, ctx, df: DataFrame, opts: DeltaSinkOptions, exists: bool) -> None:
        mode = "append" if opts.mode in ("append", "merge") else "overwrite"
        writer = df.write.format("delta").mode(mode).options(**spark_options(opts.write_options))
        if opts.partition_by and not exists:
            writer = writer.partitionBy(*opts.partition_by)
        if opts.schema_evolution:
            writer = writer.option("mergeSchema", "true")
        if opts.mode == "overwrite_partitions":
            writer = writer.option("partitionOverwriteMode", "dynamic")
        tagged = opts.skip_replayed_batches and ctx.batch_id is not None
        if tagged:
            # Delta records (txnAppId, txnVersion) in the table and silently skips a version
            # it has already committed: a micro-batch replayed after a restart is written once.
            writer = writer.option("txnAppId", ctx.pipeline_id).option("txnVersion", ctx.batch_id)
        before = table_version(ctx.spark, opts.path) if tagged and exists else None
        writer.save(opts.path)
        if before is not None and table_version(ctx.spark, opts.path) == before:
            ctx.log.warning(
                "delta table %s already has micro-batch %s of pipeline %s; skipped it", opts.path, ctx.batch_id, ctx.pipeline_id
            )

    @staticmethod
    def _insertable(df: DataFrame, opts: DeltaSinkOptions) -> DataFrame:
        conditions = [f"NOT ({opts.delete_condition})" if opts.delete_condition else None, opts.insert_condition]
        keep = " AND ".join(f"({c})" for c in conditions if c)
        return df.alias(SOURCE).filter(keep) if keep else df

    def _merge(self, ctx, df: DataFrame, opts: DeltaSinkOptions) -> None:
        from delta.tables import DeltaTable

        missing = [k for k in opts.merge_keys if k not in df.columns]
        if missing:
            raise TributaryError(f"merge_keys not in the data: {', '.join(missing)} (columns: {', '.join(df.columns)})")
        condition = " AND ".join(f"{TARGET}.`{k}` = {SOURCE}.`{k}`" for k in opts.merge_keys)
        # A fresh handle every time: one taken before a schema change would carry a stale plan.
        builder = DeltaTable.forPath(ctx.spark, opts.path).alias(TARGET).merge(df.alias(SOURCE), condition)
        if opts.schema_evolution:
            builder = builder.withSchemaEvolution()
        # Clauses apply in order: a matched row meeting the delete condition is deleted, not
        # updated; one meeting neither the delete nor the update condition is left as it is.
        if opts.delete_condition:
            builder = builder.whenMatchedDelete(condition=opts.delete_condition)
        builder = builder.whenMatchedUpdateAll(condition=opts.update_condition)
        insert = [f"NOT ({opts.delete_condition})" if opts.delete_condition else None, opts.insert_condition]
        builder.whenNotMatchedInsertAll(condition=" AND ".join(f"({c})" for c in insert if c) or None).execute()


def existing_schema(spark, path: str) -> StructType | None:
    """The schema of the Delta table at ``path``, or None when there is none (or it cannot be
    read from here, e.g. a remote path without credentials): nothing to compare with."""
    try:
        configure_session(spark)
        if not is_delta_table(spark, path):
            return None
        return spark.read.format("delta").load(path).schema
    except Exception:  # noqa: BLE001 - best effort, design time only
        return None


def condition_issues(condition: str, columns: list[str], table_columns: list[str] | None) -> list[str]:
    """Column references in a merge condition: ``s.x`` must be in the data, ``t.x`` in the
    table (when known), anything else must be one of the two aliases."""
    try:
        tree = sqlglot.parse_one(condition, read="spark")
    except sqlglot.errors.ParseError as exc:
        return [f"cannot parse: {str(exc).splitlines()[0]}"]
    issues: list[str] = []
    for column in tree.find_all(exp.Column):
        alias, name = column.table, column.name
        if alias == SOURCE:
            if name not in columns:
                issues.append(f"{SOURCE}.{name} is not a column of the data (columns: {', '.join(columns)})")
        elif alias == TARGET:
            if table_columns is not None and name not in table_columns:
                issues.append(f"{TARGET}.{name} is not a column of the table (columns: {', '.join(table_columns)})")
        elif alias:
            issues.append(f"unknown alias '{alias}' in {alias}.{name}: use {SOURCE} (incoming row) or {TARGET} (table row)")
        elif name not in columns and (table_columns is None or name not in table_columns):
            issues.append(f"unknown column {name}; prefix it with {SOURCE}. (incoming row) or {TARGET}. (table row)")
    return issues


def compatibility_issues(data: StructType, table: StructType, opts: DeltaSinkOptions) -> list[tuple[str, str]]:
    """Delta's schema enforcement, applied ahead of time: types must match the table's
    exactly, and new columns need schema_evolution (a merge without it ignores them)."""
    issues: list[tuple[str, str]] = []
    table_types = {f.name: f.dataType.simpleString() for f in table.fields}
    data_columns = [f.name for f in data.fields]
    missing = [k for k in opts.merge_keys if k in data_columns and k not in table_types]  # the rest: reported already
    if missing:
        issues.append(("merge_keys", f"not in the table at {opts.path}: {', '.join(missing)} (columns: {', '.join(table_types)})"))
    changed = [
        f"{f.name} is {f.dataType.simpleString()} in the data but {table_types[f.name]} in the table"
        for f in data.fields
        if f.name in table_types and f.dataType.simpleString() != table_types[f.name]
    ]
    if changed:
        issues.append(("", "; ".join(changed) + "; cast it in the SQL upstream (Delta never changes a column's type)"))
    new = [f.name for f in data.fields if f.name not in table_types]
    if new and not opts.schema_evolution and opts.mode != "merge":
        issues.append(("schema_evolution", f"the table has no column(s) {', '.join(new)}; turn this on to add them, or drop them upstream"))
    return issues
