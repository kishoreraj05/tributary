"""Apache Hudi tables, read with the Hudi Spark data source.

A Hudi table is a directory (``<base_path>/<database>/<table>``): no catalog is needed to
read one. The storage root usually comes from a connection of type ``hudi``, the same one
the Hudi sink uses; the node names the table and how it is read (``query_type``):

- ``snapshot``: the table as it is now (for merge-on-read tables, base files merged with
  their log files), or as it was at ``as_of_instant`` (time travel).
- ``read_optimized``: merge-on-read base files only, skipping the log files: faster, but
  rows written since the last compaction are missing. Same as snapshot on copy-on-write.
- ``incremental``: only the rows changed by the commits after ``begin_instant`` (up to
  ``end_instant``): their latest state, or with ``incremental_format: cdc`` one change
  row per record (``op``, ``ts_ms``, ``before``, ``after``) for tables written with
  ``hoodie.table.cdc.enabled``. Incremental reads are how a pipeline processes only
  what is new since its previous run. Hudi serves them from the files the commits in
  range wrote; when ``begin_instant`` is older than the table's active timeline (those
  commits were archived) it scans the whole table and keeps the rows whose latest commit
  is in range instead, rather than returning an incomplete result. ``earliest`` is every
  commit, that is the table itself (as of ``end_instant`` when given).

With a ``checkpoint_location``, a batch incremental read continues where the previous
successful run stopped: the read is pinned to the latest commit present when it starts
and that commit is recorded in ``commit_read``, once every sink has written. In a
streaming pipeline, the table is read as a stream of its commits: every micro-batch holds
the rows changed by new commits, starting from ``start_offset`` when there is no
checkpoint yet (the pipeline's own checkpoint tracks the progress). Previews show a
snapshot, or what the next run would read, and record nothing.

Reads need nothing from the Spark session beyond the Hudi bundle: Kryo serialization is
only required for writes, so a pipeline that reads Hudi and writes elsewhere does not
need it.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

from ...core import BatchReadable, PluginOptions, Source, StreamReadable
from ...errors import TributaryError
from .._hudi import (
    BASE_PATH_DESCRIPTION,
    HUDI_PACKAGE,
    META_COLUMN_PREFIX,
    TABLE_PATTERN,
    latest_instant,
    read_file,
    split_table,
    table_exists,
    table_location,
    table_properties,
    table_schema,
    to_instant,
    write_file,
)
from .._util import OptionValue, missing_columns, spark_options, when

QueryType = Literal["snapshot", "read_optimized", "incremental"]
IncrementalFormat = Literal["latest_state", "cdc"]

EARLIEST = "earliest"
LATEST = "latest"
# Hudi compares instants as strings: this one precedes every real commit.
EARLIEST_INSTANT = "000"

SNAPSHOT_ONLY = ("snapshot",)
INCREMENTAL_ONLY = ("incremental",)

# Reader settings (Hudi 0.13+ names; still accepted by the 1.x line).
QUERY_TYPE = "hoodie.datasource.query.type"
BEGIN_INSTANT = "hoodie.datasource.read.begin.instanttime"
END_INSTANT = "hoodie.datasource.read.end.instanttime"
INCREMENTAL_FORMAT = "hoodie.datasource.query.incremental.format"
# Incremental reads list the files the commits in range wrote, which misses commits that
# were archived out of the active timeline; with this on, such a range is served by a
# full scan filtered on commit time instead of silently incomplete results.
FULL_SCAN_IF_ARCHIVED = "hoodie.datasource.read.incr.fallback.fulltablescan.enable"
# Hudi 1.x has a second incremental implementation (for its table version 8) whose start
# bound is inclusive on a commit's completion time and which, in 1.0.2, also returns rows of
# untouched commits in open-ended reads; asking for the version-6 one keeps the 0.x
# semantics (the commits after begin_instant, by commit time) on both lines. 0.x ignores
# the setting.
INCREMENTAL_RELATION_VERSION = "hoodie.datasource.read.incr.table.version"
STREAM_RELATION_VERSION = "hoodie.datasource.read.streaming.table.version"  # 1.x streams: a start instant is inclusive
AS_OF_INSTANT = "as.of.instant"
STREAM_START = "hoodie.datasource.streaming.startOffset"


class HudiSourceOptions(PluginOptions):
    # --- storage: typically provided by a connection of type 'hudi' ---------------------
    base_path: str = Field(..., description=BASE_PATH_DESCRIPTION)
    # --- table ---------------------------------------------------------------------------
    table: str = Field(
        ...,
        pattern=TABLE_PATTERN,
        description="database.table (or just table, in database 'default'), e.g. sales.orders: the table "
        "directory under base_path (<base_path>/sales/orders)",
    )
    query_type: QueryType = Field(
        "snapshot",
        description="snapshot: the table as it is now (or was at as_of_instant). read_optimized: merge-on-read "
        "base files only, without the log files written since the last compaction; faster, possibly stale. "
        "incremental: only the rows changed by the commits after begin_instant, up to end_instant",
    )
    as_of_instant: str | None = Field(
        None,
        description="Time travel: read the table as it was at this commit. A Hudi instant (as in "
        "_hoodie_commit_time, e.g. 20240604080000123), a timestamp (2024-06-04 08:00:00) or a date",
        json_schema_extra=when("query_type", applies=SNAPSHOT_ONLY),
    )
    begin_instant: str | None = Field(
        None,
        validate_default=True,
        description="Rows changed by the commits after this instant (exclusive): a Hudi instant, a timestamp, "
        "a date, or 'earliest' for every commit. The _hoodie_commit_time column of a previous read (keep it "
        "with include_metadata_columns) is the usual value; a variable such as ${env.since} makes it a setting",
        json_schema_extra=when("query_type", applies=INCREMENTAL_ONLY, required=INCREMENTAL_ONLY),
    )
    end_instant: str | None = Field(
        None,
        description="Stop at this commit (inclusive); the latest commit when empty",
        json_schema_extra=when("query_type", applies=INCREMENTAL_ONLY),
    )
    incremental_format: IncrementalFormat = Field(
        "latest_state",
        description="latest_state: the changed rows as they are now, in the table's columns. cdc: one row per "
        "change, with op (i, u or d), ts_ms (the commit) and the row before and after the change as structs "
        "of the table's columns; needs a table written with hoodie.table.cdc.enabled=true (a Hudi sink's "
        "write_options)",
        json_schema_extra=when("query_type", applies=INCREMENTAL_ONLY),
    )
    checkpoint_location: str | None = Field(
        None,
        description="Batch incremental reads: a directory (local, s3a://, ...) where the source records the commit "
        "each run read up to, so the next run continues from there; begin_instant then only applies to the first "
        "run. Recorded once every sink has written, never by a preview. A streaming pipeline tracks its progress "
        "in its own checkpoint instead",
        json_schema_extra=when("query_type", applies=INCREMENTAL_ONLY),
    )
    include_metadata_columns: bool = Field(
        False,
        description="Keep Hudi's _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, "
        "_hoodie_partition_path and _hoodie_file_name columns",
    )
    start_offset: str = Field(
        EARLIEST,
        description="Streaming only: where the stream starts when there is no checkpoint yet. earliest: every "
        "commit. latest: only commits made after the stream starts. Or a Hudi instant, timestamp or date",
    )
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Any other hoodie.* reader setting, applied on top of the ones derived above: "
        "hoodie.datasource.read.incr.path.glob, hoodie.schema.on.read.enable, hoodie.enable.data.skipping, ...",
    )
    # --- connection settings the source has no use for ------------------------------------
    # A connection of type 'hudi' also describes the metastore Hudi sinks sync with. Those
    # settings are accepted here so the same connection serves sources and sinks; reading
    # goes through storage and never touches the metastore.
    catalog_sync: str | None = Field(None, description="Not used when reading: accepted from a shared hudi connection")
    catalog_uri: str | None = Field(None, description="Not used when reading: accepted from a shared hudi connection")
    catalog_user: str | None = Field(None, description="Not used when reading: accepted from a shared hudi connection")
    catalog_password: str | None = Field(None, description="Not used when reading: accepted from a shared hudi connection")

    # Field validators so each error is reported on its own field, where the UI shows it.

    @field_validator("as_of_instant", mode="after")
    @classmethod
    def _as_of_for_snapshots(cls, value: str | None, info: ValidationInfo) -> str | None:
        if not value or not value.strip():
            return None
        query_type = info.data.get("query_type", "snapshot")
        if query_type != "snapshot":
            raise ValueError(
                f"time travel is a snapshot read, not '{query_type}' (a read_optimized view has no point in time; "
                "for commits up to an instant use end_instant)"
            )
        to_instant(value)  # normalised when the reader options are built
        return value.strip()

    @field_validator("begin_instant", mode="after")
    @classmethod
    def _begin_when_incremental(cls, value: str | None, info: ValidationInfo) -> str | None:
        query_type = info.data.get("query_type", "snapshot")
        if not value or not value.strip():
            if query_type == "incremental":
                raise ValueError("required for query_type 'incremental' (a Hudi instant, a timestamp, a date or 'earliest')")
            return None
        if query_type != "incremental":
            raise ValueError(f"only incremental reads start at an instant (query_type is '{query_type}')")
        value = value.strip()
        if value.lower() != EARLIEST:
            to_instant(value)
        return value

    @field_validator("end_instant", mode="after")
    @classmethod
    def _end_when_incremental(cls, value: str | None, info: ValidationInfo) -> str | None:
        if not value or not value.strip():
            return None
        query_type = info.data.get("query_type", "snapshot")
        if query_type != "incremental":
            raise ValueError(f"only incremental reads stop at an instant (query_type is '{query_type}'; for a snapshot use as_of_instant)")
        to_instant(value)
        return value.strip()

    @field_validator("start_offset", mode="after")
    @classmethod
    def _valid_start(cls, value: str) -> str:
        value = value.strip()
        if value.lower() in (EARLIEST, LATEST):
            return value.lower()
        to_instant(value)
        return value

    # -- derived ---------------------------------------------------------------------------

    @property
    def database(self) -> str | None:
        return split_table(self.table)[0]

    @property
    def table_name(self) -> str:
        return split_table(self.table)[1]

    @property
    def location(self) -> str:
        return table_location(self.base_path, self.table)

    def hoodie_options(self, *, begin: str | None = None, end: str | None = None) -> dict[str, str]:
        """The batch reader options derived from the fields, then ``read_options`` on top.
        ``begin`` and ``end`` replace ``begin_instant`` / ``end_instant`` (a recorded
        checkpoint, the commit a read is pinned to)."""
        begin = begin or self.begin_instant or ""
        end = end or self.end_instant
        from_earliest = self.query_type == "incremental" and begin.lower() == EARLIEST
        opts: dict[str, Any] = {QUERY_TYPE: self.query_type}
        if self.query_type == "snapshot" and self.as_of_instant:
            opts[AS_OF_INSTANT] = to_instant(self.as_of_instant)
        if from_earliest and self.incremental_format == "latest_state":
            # Every commit, in its latest state: that is the table itself (as of end_instant
            # when given). An incremental read from before the first commit would go through
            # Hudi's fallback scan instead, which reads merge-on-read base files without
            # their log files (0.15 and 1.0.2 alike).
            opts[QUERY_TYPE] = "snapshot"
            if end:
                opts[AS_OF_INSTANT] = to_instant(end)
        elif self.query_type == "incremental":
            opts[BEGIN_INSTANT] = EARLIEST_INSTANT if from_earliest else to_instant(begin)
            if end:
                opts[END_INSTANT] = to_instant(end)
            opts[INCREMENTAL_FORMAT] = self.incremental_format
            opts[FULL_SCAN_IF_ARCHIVED] = True
            opts[INCREMENTAL_RELATION_VERSION] = 6
        opts.update(self.read_options)
        return spark_options(opts)

    def stream_options(self) -> dict[str, str]:
        """The streaming reader options: a stream is always incremental, from ``start_offset``."""
        start = self.start_offset if self.start_offset in (EARLIEST, LATEST) else to_instant(self.start_offset)
        opts: dict[str, Any] = {STREAM_START: start, INCREMENTAL_FORMAT: self.incremental_format, STREAM_RELATION_VERSION: 6}
        opts.update(self.read_options)
        return spark_options(opts)


class HudiSource(Source, BatchReadable, StreamReadable):
    """Apache Hudi tables: snapshot, read-optimized, incremental or time-travel reads, and streams of commits."""

    type_name = "hudi"
    display_name = "Apache Hudi"
    icon = "table"
    Options = HudiSourceOptions
    spark_packages = (HUDI_PACKAGE,)

    # -- read ------------------------------------------------------------------------------

    def read(self, ctx, opts: HudiSourceOptions, schema) -> DataFrame:
        check_table(ctx.spark, opts, ctx.log)
        begin = end = None
        if opts.query_type == "incremental" and opts.checkpoint_location:
            begin, end = self._window(ctx, opts)
        df = ctx.spark.read.format("hudi").options(**opts.hoodie_options(begin=begin, end=end)).load(opts.location)
        if opts.query_type == "incremental" and opts.incremental_format == "cdc":
            df = parse_cdc(ctx.spark, df, opts)
        return self._project(df, opts, schema)

    def _window(self, ctx, opts: HudiSourceOptions) -> tuple[str | None, str | None]:
        """The commits this run reads when progress is tracked: from the recorded commit
        (``begin_instant`` on the first run) up to the latest one there is right now, so a
        commit landing during the run is left for the next one. The end is noted as pending
        under this run id; ``commit_read`` promotes it once the sinks have written."""
        progress = Progress(ctx.spark, opts.checkpoint_location)
        committed = progress.committed()
        latest = latest_instant(ctx.spark, opts.location)
        end = opts.end_instant or latest
        if committed and opts.end_instant and to_instant(opts.end_instant) <= committed:
            ctx.log.info("hudi table %s: end_instant %s is already recorded as read; nothing new", opts.table, opts.end_instant)
        if not ctx.preview and end:
            progress.note_pending(end, ctx)
        ctx.log.info(
            "hudi table %s: reading commits after %s up to %s (%s)", opts.table, committed or opts.begin_instant, end or "the latest",
            "recorded progress" if committed else "no progress recorded yet: starting at begin_instant",
        )
        return committed, end

    def commit_read(self, ctx, opts: HudiSourceOptions) -> None:
        if opts.query_type == "incremental" and opts.checkpoint_location:
            Progress(ctx.spark, opts.checkpoint_location).commit(ctx, opts.table)

    def read_stream(self, ctx, opts: HudiSourceOptions, schema) -> DataFrame:
        check_table(ctx.spark, opts, ctx.log)
        df = ctx.spark.readStream.format("hudi").options(**opts.stream_options()).load(opts.location)
        if opts.incremental_format == "cdc":
            df = parse_cdc(ctx.spark, df, opts)
        return self._project(df, opts, schema)

    def _project(self, df: DataFrame, opts: HudiSourceOptions, schema) -> DataFrame:
        """The table's own columns, or exactly the ones a ``schema`` block names (cast to its
        types); Hudi's metadata columns only when asked for or named."""
        struct = schema.struct if schema is not None else None
        if struct is not None:
            missing = missing_columns(df.schema, [f.name for f in struct.fields])
            if missing:
                raise TributaryError(
                    f"hudi source {opts.table}: schema names column{'s' if len(missing) > 1 else ''} "
                    f"{', '.join(repr(m) for m in missing)} that the table does not have "
                    f"(columns: {', '.join(df.columns)})"
                )
            return df.select(*[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in struct.fields])
        if opts.include_metadata_columns:
            return df
        return df.drop(*[c for c in df.columns if c.startswith(META_COLUMN_PREFIX)])

    def output_schema(self, opts: HudiSourceOptions, schema) -> StructType | None:
        # Without a schema block the columns come from the table itself (read_preview with
        # limit 0 during analysis: Hudi takes the schema from the commit metadata).
        return schema.struct if schema is not None else None

    def estimate_input_bytes(self, opts: HudiSourceOptions) -> int | None:
        location = opts.location.removeprefix("file://")
        if "://" in location or not os.path.isdir(location):
            return None  # remote listing needs a Spark session; size stays unknown
        total = 0
        for root, dirs, files in os.walk(location):
            dirs[:] = [d for d in dirs if d != ".hoodie"]  # data files only, not the timeline
            total += sum(os.path.getsize(os.path.join(root, f)) for f in files if not f.startswith("."))
        return total


class Progress:
    """Where a batch incremental read stopped, kept in ``checkpoint_location``.

    Two files: ``pending.json`` is written when a run reads (the commit it reads up to and
    the run id); ``offset.json`` is the committed progress, and only ``commit`` moves it
    forward, from a pending note of the same run. A run that fails before its sinks write
    leaves ``offset.json`` as it was, so the next run reads the same commits again; two
    runs reading at once cannot commit each other's window (the later one's pending note
    wins, the other is logged and left to read again)."""

    COMMITTED = "offset.json"
    PENDING = "pending.json"

    def __init__(self, spark, location: str):
        self.spark = spark
        self.location = location.rstrip("/")

    def _read(self, name: str) -> dict[str, Any] | None:
        text = read_file(self.spark, f"{self.location}/{name}")
        if text is None:
            return None
        try:
            record = json.loads(text)
        except ValueError as exc:
            raise TributaryError(f"{self.location}/{name} is not a progress record written by the hudi source: {exc}") from None
        if not isinstance(record, dict) or not isinstance(record.get("instant"), str):
            raise TributaryError(f"{self.location}/{name} is not a progress record written by the hudi source")
        return record

    def committed(self) -> str | None:
        """The instant the last successful run read up to, or None before the first."""
        record = self._read(self.COMMITTED)
        return record["instant"] if record else None

    def note_pending(self, instant: str, ctx) -> None:
        write_file(self.spark, f"{self.location}/{self.PENDING}", json.dumps({
            "instant": instant, "run_id": ctx.run_id, "pipeline": ctx.pipeline_id, "batch_id": ctx.batch_id,
            "noted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, indent=2))

    def commit(self, ctx, table: str) -> None:
        pending = self._read(self.PENDING)
        if pending is None:
            return  # nothing was read (a table without commits)
        if pending.get("run_id") != ctx.run_id:
            ctx.log.warning(
                "hudi table %s: progress up to %s was noted by run %s, not this one (%s); not recorded, so the next "
                "run reads these commits again", table, pending["instant"], pending.get("run_id"), ctx.run_id,
            )
            return
        previous = self.committed()
        if previous and pending["instant"] < previous:
            ctx.log.warning("hudi table %s: progress already past %s (at %s); keeping it", table, pending["instant"], previous)
            return
        write_file(self.spark, f"{self.location}/{self.COMMITTED}", json.dumps({
            "instant": pending["instant"], "table": table, "run_id": ctx.run_id, "pipeline": ctx.pipeline_id,
            "committed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, indent=2))
        ctx.log.info("hudi table %s: progress recorded up to commit %s", table, pending["instant"])


def parse_cdc(spark, df: DataFrame, opts: HudiSourceOptions) -> DataFrame:
    """Hudi's change rows carry the row before and after the change as JSON strings (a
    missing side is null, or the text 'null'; inserts from a table's first commit also
    include the metadata fields). Parsed into structs of the table's columns, so SQL can
    say ``after.amount`` and a missing side is always null."""
    if not {"before", "after"} <= set(df.columns):
        return df  # not a change feed (a stream on Hudi 0.x, for instance)
    columns = table_schema(spark, opts.location)

    def parsed(name: str):
        col = F.col(name)
        return F.when(col.isNull() | (col == "null"), F.lit(None).cast(columns)).otherwise(F.from_json(col, columns))

    return df.withColumn("before", parsed("before")).withColumn("after", parsed("after"))


def check_table(spark, opts: HudiSourceOptions, log=None) -> dict[str, str]:
    """The table's ``hoodie.properties``, once the read is known to make sense for it.

    Hudi's own error for a missing table is a stack trace about a missing path or a
    malformed ``.hoodie`` folder, and a change feed from a table that records no changes
    comes back empty without a word; both are said up front. A read_optimized view of a
    copy-on-write table is simply its snapshot, which is worth a log line rather than an
    error: the same node can read either kind of table."""
    exists = table_exists(spark, opts.location)  # None: not checkable (Spark Connect)
    if exists is False:
        raise TributaryError(
            f"hudi table {opts.table} not found at {opts.location} (no .hoodie/hoodie.properties there); "
            "check base_path and table"
        )
    props = table_properties(spark, opts.location) if exists else {}
    table_type = props.get("hoodie.table.type", "").lower() or "unknown type"
    if opts.query_type == "incremental" and opts.incremental_format == "cdc" and props:
        if props.get("hoodie.table.cdc.enabled", "false").lower() != "true":
            raise TributaryError(
                f"hudi table {opts.table} does not record changes (hoodie.table.cdc.enabled is off), so there is no "
                "change feed to read: write it with hoodie.table.cdc.enabled=true (a Hudi sink's write_options), "
                "or read the latest state with incremental_format 'latest_state'"
            )
    if log is not None:
        note = " (the snapshot: a copy-on-write table has no log files to skip)" if (
            opts.query_type == "read_optimized" and table_type == "copy_on_write") else ""
        log.info("reading hudi table %s (%s) as %s%s at %s", opts.table, table_type, opts.query_type, note, opts.location)
    return props
