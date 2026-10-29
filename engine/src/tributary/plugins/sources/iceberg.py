"""Apache Iceberg tables, read through a Spark catalog.

The catalog settings are shared with the Iceberg sink (``tributary.plugins.iceberg``), so
one connection of type ``iceberg`` serves both. On top of them the source offers what
Iceberg's reader does: the current table, time travel (a snapshot id, a point in time, a
branch or a tag), incremental reads between two snapshots, and in streaming pipelines a
stream of the table's appends.

``filter`` and ``columns`` narrow the read at the source: Spark pushes the predicate into
Iceberg's scan, which skips whole partitions and files by their statistics, and reads only
the columns asked for. ``metadata_table`` reads one of the table's metadata tables instead
(``snapshots``, ``refs``, ``files``, ...), which is how to find snapshot ids and tags.

The ``schema`` block is optional. The table's own schema is used; when one is given, only
its columns are read, cast to the declared types (Iceberg does not take a reader schema).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Literal

from pydantic import ConfigDict, Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame, functions as F

from ...core import BatchReadable, Source, StreamReadable
from ...errors import TributaryError
from .._util import OptionValue, spark_options
from ..iceberg import (
    ICEBERG_PACKAGE,
    ICEBERG_VERSION,
    IcebergCatalogOptions,
    configure_catalog,
    iceberg_version,
    local_table_bytes,
)

_TIME_HELP = "ISO 8601 (2024-05-01T00:00:00Z; UTC unless an offset is given) or epoch milliseconds"
_TIME_TRAVEL = ("snapshot_id", "as_of_timestamp", "branch", "tag")
_INCREMENTAL = ("start_snapshot_id", "end_snapshot_id")
_COLUMN = r"^[A-Za-z_][A-Za-z0-9_]*$"
# Snapshot ids are 64-bit: as JSON numbers they lose digits past 2**53 in the UI (JavaScript),
# so they travel as strings of digits. YAML ints are coerced (coerce_numbers_to_str below).
_SNAPSHOT_ID = r"^\d+$"
MetadataTable = Literal[
    "snapshots", "history", "refs", "metadata_log_entries", "files", "data_files", "delete_files",
    "manifests", "partitions", "entries", "all_data_files", "all_manifests", "all_entries",
]


def to_millis(value: str) -> int:
    """A point in time as Iceberg wants it: milliseconds since the epoch."""
    text = value.strip()
    if re.fullmatch(r"\d+", text):
        return int(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        raise ValueError(f"not a timestamp: {value!r}; use {_TIME_HELP}") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class IcebergSourceOptions(IcebergCatalogOptions):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, coerce_numbers_to_str=True)

    # --- what to read (the catalog and table fields come from IcebergCatalogOptions) --------
    filter: str | None = Field(
        None,
        description="SQL condition applied at the source, e.g. country = 'IN' AND order_ts >= '2024-01-01'. "
        "Pushed down into Iceberg's scan, which skips whole partitions and files (batch reads; a stream "
        "filters each micro-batch)",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Read only these columns (Iceberg reads nothing else from the files). Default: all",
    )
    # --- time travel and increments ----------------------------------------------------------
    snapshot_id: str | None = Field(
        None,
        pattern=_SNAPSHOT_ID,
        description="Time travel: read the table as it was at this snapshot (ids are listed in the "
        "table's .snapshots metadata table; set metadata_table to see them)",
    )
    as_of_timestamp: str | None = Field(
        None, description=f"Time travel: read the snapshot that was current at this time. {_TIME_HELP}",
    )
    branch: str | None = Field(None, description="Read the head of this branch instead of main")
    tag: str | None = Field(None, description="Read the snapshot this tag points to")
    start_snapshot_id: str | None = Field(
        None,
        pattern=_SNAPSHOT_ID,
        description="Incremental read: only the rows appended after this snapshot (exclusive). "
        "Batch only; a streaming pipeline follows the table's appends by itself",
    )
    end_snapshot_id: str | None = Field(
        None,
        pattern=_SNAPSHOT_ID,
        description="Incremental read: up to this snapshot (inclusive). Defaults to the current snapshot; "
        "needs start_snapshot_id",
    )
    metadata_table: MetadataTable | None = Field(
        None,
        description="Read one of the table's metadata tables instead of its rows: snapshots (ids and "
        "commit times for time travel), refs (branches and tags), history, files, partitions, manifests, ... "
        "Batch only",
    )
    # --- streaming ---------------------------------------------------------------------------
    stream_from_timestamp: str | None = Field(
        None,
        description=f"Streaming only: start with the first snapshot committed at or after this time "
        f"({_TIME_HELP}). By default the stream starts with the table's whole current contents",
    )
    streaming_skip_delete_snapshots: bool = Field(
        False,
        description="Streaming only: skip snapshots that deleted rows instead of failing the stream "
        "(a stream can only replay appends)",
    )
    streaming_skip_overwrite_snapshots: bool = Field(
        False,
        description="Streaming only: skip snapshots that overwrote rows instead of failing the stream",
    )
    max_files_per_trigger: int | None = Field(None, ge=1, description="Streaming only: data files per micro-batch")
    max_rows_per_trigger: int | None = Field(
        None, ge=1, description="Streaming only: rows per micro-batch (a soft cap: whole files are taken)"
    )
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Extra options for the Iceberg reader, e.g. split-size, vectorization-enabled, locality",
    )

    # Field validators so each conflict is reported on the field that introduces it, which is
    # where the UI shows it. ``info.data`` holds the fields declared above.

    @field_validator("columns", mode="after")
    @classmethod
    def _plain_column_names(cls, value: list[str]) -> list[str]:
        value = [c.strip() for c in value]
        bad = [c for c in value if not re.match(_COLUMN, c)]
        if bad:
            raise ValueError(f"not column names: {', '.join(bad)}")
        return value

    @field_validator("as_of_timestamp", "stream_from_timestamp", mode="after")
    @classmethod
    def _parseable_time(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is not None:
            to_millis(value)
            if info.field_name == "as_of_timestamp" and info.data.get("snapshot_id") is not None:
                raise ValueError("set either snapshot_id or as_of_timestamp, not both")
        return value

    @field_validator("branch", "tag", mode="after")
    @classmethod
    def _one_reference(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value:
            others = [f for f in _TIME_TRAVEL if f != info.field_name and info.data.get(f) not in (None, "")]
            if others:
                raise ValueError(f"cannot be combined with {', '.join(others)}")
        return value

    @field_validator("start_snapshot_id", mode="after")
    @classmethod
    def _incremental_from_current_line(cls, value: str | None, info: ValidationInfo) -> str | None:
        # Iceberg reads increments along a branch (main by default), never from a pinned snapshot.
        clashes = [f for f in ("snapshot_id", "as_of_timestamp", "tag") if info.data.get(f) not in (None, "")]
        if value is not None and clashes:
            raise ValueError(f"an incremental read cannot be combined with {', '.join(clashes)}")
        return value

    @field_validator("end_snapshot_id", mode="after")
    @classmethod
    def _end_needs_start(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is not None and info.data.get("start_snapshot_id") is None:
            raise ValueError("needs start_snapshot_id")
        return value

    @field_validator("metadata_table", mode="after")
    @classmethod
    def _metadata_is_current(cls, value: str | None, info: ValidationInfo) -> str | None:
        clashes = [f for f in (*_TIME_TRAVEL, *_INCREMENTAL) if info.data.get(f) not in (None, "")]
        if value and clashes:
            raise ValueError(f"a metadata table is read as it is now; cannot be combined with {', '.join(clashes)}")
        return value

    # -- derived ---------------------------------------------------------------------------

    def batch_read_options(self) -> dict[str, str]:
        options = spark_options(self.read_options)
        if self.snapshot_id is not None:
            options["snapshot-id"] = self.snapshot_id
        if self.as_of_timestamp:
            options["as-of-timestamp"] = str(to_millis(self.as_of_timestamp))
        if self.branch:
            options["branch"] = self.branch
        if self.tag:
            options["tag"] = self.tag
        if self.start_snapshot_id is not None:
            options["start-snapshot-id"] = self.start_snapshot_id
        if self.end_snapshot_id is not None:
            options["end-snapshot-id"] = self.end_snapshot_id
        return options

    def stream_read_options(self) -> dict[str, str]:
        options = spark_options(self.read_options)
        if self.stream_from_timestamp:
            options["stream-from-timestamp"] = str(to_millis(self.stream_from_timestamp))
        if self.streaming_skip_delete_snapshots:
            options["streaming-skip-delete-snapshots"] = "true"
        if self.streaming_skip_overwrite_snapshots:
            options["streaming-skip-overwrite-snapshots"] = "true"
        if self.max_files_per_trigger:
            options["streaming-max-files-per-micro-batch"] = str(self.max_files_per_trigger)
        if self.max_rows_per_trigger:
            options["streaming-max-rows-per-micro-batch"] = str(self.max_rows_per_trigger)
        return options

    def batch_only_settings(self) -> list[str]:
        """Options that pin what is read and so make no sense for a stream of appends."""
        pinning = (*_TIME_TRAVEL, *_INCREMENTAL, "metadata_table")
        return [f for f in pinning if getattr(self, f) not in (None, "")]

    def load_path(self) -> str:
        """What the reader loads: the table, or one of its metadata tables."""
        table = self.qualified_table()
        return f"{table}.{self.metadata_table}" if self.metadata_table else table

    def rate_limits(self) -> list[str]:
        return [f for f in ("max_files_per_trigger", "max_rows_per_trigger") if getattr(self, f)]

    def describe(self) -> str:
        if self.metadata_table:
            target = f"{self.catalog}.{self.table}.{self.metadata_table}"
            return f"iceberg {self.catalog_type} <- {target} at {self.location}"
        what = "current"
        if self.snapshot_id is not None:
            what = f"snapshot {self.snapshot_id}"
        elif self.as_of_timestamp:
            what = f"as of {self.as_of_timestamp}"
        elif self.tag:
            what = f"tag {self.tag}"
        if self.branch:
            what = f"branch {self.branch}"
        if self.start_snapshot_id is not None:
            what += f", appends after snapshot {self.start_snapshot_id}"
            if self.end_snapshot_id is not None:
                what += f" up to {self.end_snapshot_id}"
        return f"iceberg {self.catalog_type} <- {self.catalog}.{self.table} ({what}) at {self.location}"


class IcebergSource(Source, BatchReadable, StreamReadable):
    """Apache Iceberg tables: current data, time travel, incremental reads or a stream of appends."""

    type_name = "iceberg"
    display_name = "Apache Iceberg"
    icon = "table"
    Options = IcebergSourceOptions
    spark_packages = (ICEBERG_PACKAGE,)

    def required_conf(self, opts: IcebergSourceOptions) -> dict[str, str]:
        return opts.catalog_conf()

    # -- read ------------------------------------------------------------------------------

    def read(self, ctx, opts: IcebergSourceOptions, schema) -> DataFrame:
        ctx.log.info("reading %s", opts.describe())
        options = opts.batch_read_options()
        if opts.start_snapshot_id is not None and self._nothing_appended(ctx, opts):
            # Iceberg refuses an incremental scan whose start is already the end ("not a parent
            # ancestor"), which is the normal state of a table between two runs: no rows then.
            ctx.log.info("no snapshot after %s in %s.%s: no rows", opts.start_snapshot_id, opts.catalog, opts.table)
            for key in ("start-snapshot-id", "end-snapshot-id"):
                options.pop(key, None)
            return narrow(self._load(ctx, ctx.spark.read, opts, options).limit(0), opts, schema)
        return narrow(self._load(ctx, ctx.spark.read, opts, options), opts, schema)

    def read_stream(self, ctx, opts: IcebergSourceOptions, schema) -> DataFrame:
        pinned = opts.batch_only_settings()
        if pinned:
            raise TributaryError(
                f"iceberg source: {', '.join(pinned)} only appl{'ies' if len(pinned) == 1 else 'y'} to batch reads; "
                "a stream follows the table's appends (use stream_from_timestamp to choose where it starts)"
            )
        limits = opts.rate_limits()
        if limits and ctx.trigger == "available_now" and iceberg_version() < (1, 10):
            # Spark's available_now asks the source for everything up to the current end, from the
            # first offset; Iceberg runtimes before 1.10 apply their own cap to that request, so the
            # end lands a few files past where the stream *started* and the stream stalls for good.
            raise TributaryError(
                f"iceberg source: {', '.join(limits)} cannot be combined with the available_now trigger on "
                f"Iceberg {ICEBERG_VERSION} (the stream would stall after the first files). Use the "
                "processing_time trigger, drop the limit, or pick Iceberg 1.10+ with TRIBUTARY_ICEBERG_VERSION "
                "(needs Java 11+)"
            )
        ctx.log.info("streaming appends of %s", opts.describe())
        return narrow(self._load(ctx, ctx.spark.readStream, opts, opts.stream_read_options()), opts, schema)

    def _load(self, ctx, reader, opts: IcebergSourceOptions, options: dict[str, str]) -> DataFrame:
        spark = ctx.spark
        configure_catalog(spark, opts)
        table = opts.qualified_table()
        if not spark.catalog.tableExists(table):
            raise TributaryError(
                f"iceberg table {opts.catalog}.{opts.table} does not exist in the {opts.catalog_type} catalog "
                f"at {opts.location}"
            )
        # Iceberg caches table metadata per catalog instance (30 s by default), so a long-lived
        # session such as the backend's preview session would miss commits made elsewhere (a run
        # in its own process, a micro-batch in a cloned session). Always read the latest commit.
        spark.sql(f"REFRESH TABLE {table}")
        self._check_references(ctx, opts)
        return reader.format("iceberg").options(**options).load(opts.load_path())

    def _check_references(self, ctx, opts: IcebergSourceOptions) -> None:
        """Iceberg's own errors for an unknown snapshot, tag or branch are cryptic (a tag
        becomes "table ns.t.tag_x not found"); say what is wrong and what exists instead."""
        spark, table, name = ctx.spark, opts.qualified_table(), f"{opts.catalog}.{opts.table}"
        if opts.as_of_timestamp or any(getattr(opts, f) is not None for f in ("snapshot_id", *_INCREMENTAL)):
            snapshots = spark.sql(
                f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at DESC"
            ).collect()
            known = {str(r.snapshot_id) for r in snapshots}
            for field in ("snapshot_id", *_INCREMENTAL):
                value = getattr(opts, field)
                if value is not None and value not in known:
                    latest = ", ".join(f"{r.snapshot_id} ({_utc(r.committed_at)})" for r in snapshots[:5])
                    raise TributaryError(
                        f"iceberg source: {field} {value} is not a snapshot of {name}; "
                        f"the latest are {latest or 'none, the table is empty'}"
                    )
            if opts.as_of_timestamp:
                wanted = to_millis(opts.as_of_timestamp)
                if not snapshots or to_millis(snapshots[-1].committed_at.isoformat()) > wanted:
                    oldest = f"the oldest is from {_utc(snapshots[-1].committed_at)}" if snapshots else "it is empty"
                    raise TributaryError(
                        f"iceberg source: {name} has no snapshot at or before {opts.as_of_timestamp}; {oldest}"
                    )
        if opts.branch or opts.tag:
            refs = spark.sql(f"SELECT name, type FROM {table}.refs").collect()
            for field, kind, plural in (("branch", "BRANCH", "branches"), ("tag", "TAG", "tags")):
                value = getattr(opts, field)
                if value and not any(r.name == value and r.type == kind for r in refs):
                    listed = ", ".join(sorted(r.name for r in refs if r.type == kind)) or "none"
                    raise TributaryError(f"iceberg source: {name} has no {field} '{value}' ({plural}: {listed})")

    def _nothing_appended(self, ctx, opts: IcebergSourceOptions) -> bool:
        """Whether the incremental read's end (its end_snapshot_id, else the head of the branch
        it follows) is the start snapshot itself."""
        if opts.end_snapshot_id is not None:
            return opts.end_snapshot_id == opts.start_snapshot_id
        configure_catalog(ctx.spark, opts)
        table = opts.qualified_table()
        if not ctx.spark.catalog.tableExists(table):
            return False  # _load reports the missing table
        ctx.spark.sql(f"REFRESH TABLE {table}")
        head = ctx.spark.sql(
            f"SELECT snapshot_id FROM {table}.refs WHERE name = :branch", args={"branch": opts.branch or "main"}
        ).collect()
        return bool(head) and str(head[0].snapshot_id) == opts.start_snapshot_id

    # -- metadata --------------------------------------------------------------------------

    def estimate_input_bytes(self, opts: IcebergSourceOptions) -> int | None:
        return local_table_bytes(opts)


def _utc(committed_at: datetime) -> str:
    """Snapshot times as the metadata tables give them, UTC in this project's sessions."""
    return committed_at.strftime("%Y-%m-%dT%H:%M:%SZ")


def narrow(df: DataFrame, opts: IcebergSourceOptions, schema) -> DataFrame:
    """The source-level filter and column list, then the declared schema if there is one."""
    if opts.filter:
        df = df.filter(F.expr(opts.filter))
    if opts.columns:
        df = df.select(*[F.col(f"`{c}`") for c in opts.columns])
    return project(df, schema)


def project(df: DataFrame, schema) -> DataFrame:
    """Applies a declared schema: the listed columns only, cast to the declared types."""
    if schema is None or schema.struct is None:
        return df
    return df.select(*[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in schema.struct.fields])
