"""MongoDB collections, written through the official MongoDB Spark Connector.

Every row becomes one document. Spark types map onto BSON the way the connector defines
(strings, numbers, booleans, timestamps -> dates, decimals -> Decimal128, structs ->
embedded documents, arrays -> arrays). A column named ``_id`` becomes the document id;
without one, MongoDB assigns an ObjectId to every inserted document.

Modes:

- ``append``: inserts the rows.
- ``overwrite``: drops the collection first, then inserts the rows.
- ``upsert``: matches documents on ``merge_keys``. A match is replaced by the incoming row
  (``on_match: replace``) or has the row's columns set on it (``on_match: update``, the
  other fields of the document are kept); rows without a match are inserted. Re-running a
  pipeline, or replaying a micro-batch after a restart, then never duplicates rows.

Appends are idempotent by default (``deterministic_ids``): each document gets an id derived
from the pipeline, the micro-batch (or run) and the row, and is written as an upsert, so a
micro-batch replayed after a restart, or a write retried after a network drop, rewrites the
same documents instead of adding copies. Transient connection errors are retried with
backoff (``retries``); the retry is safe because every mode is then idempotent.

The connector only needs its jar (and the MongoDB Java driver it pulls in): no catalog, no
SQL extension and no session setting, so the sink works on any session, including the
shared one the backend uses for previews. Everything is passed as writer options.
"""

from __future__ import annotations

import time
from typing import Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StructType

from ...core import PluginOptions, Sink
from ...errors import TributaryError
from .._mongodb import (
    CONNECTOR_PACKAGE,
    CONNECTOR_VERSION,
    FORMAT,
    SINGLE_PARTITION,
    URI_DESCRIPTION,
    design_time_uri,
    effective_uri,
    field_name_issue,
    host_of,
    is_transient,
    type_family,
    valid_collection,
    valid_database,
    valid_uri,
)
from .._util import OptionValue, spark_options, when

__all__ = ["CONNECTOR_PACKAGE", "CONNECTOR_VERSION", "MongoDbSink", "MongoDbSinkOptions", "field_name_issue"]

WriteMode = Literal["append", "overwrite", "upsert"]
OnMatch = Literal["replace", "update"]

_ID_FIELD = "_id"
# Connector options the sink's own fields stand for; write_options cannot set them.
_OWNED_OPTIONS = {
    "connection.uri": "connection_uri",
    "database": "database",
    "collection": "collection",
    "operationType": "mode",
    "idFieldList": "merge_keys",
    "upsertDocument": "mode",
    "maxBatchSize": "max_batch_size",
    "ordered": "ordered",
    "ignoreNullValues": "ignore_null_values",
}


class MongoDbSinkOptions(PluginOptions):
    # --- connection: typically provided by a connection of type 'mongodb' ----------------
    connection_uri: str = Field(..., description=URI_DESCRIPTION)
    username: str | None = Field(None, description="Added to the URI when it has no credentials of its own")
    password: str | None = Field(None, description="Use ${secret:...}; added to the URI with username")
    # --- target --------------------------------------------------------------------------
    database: str = Field(..., min_length=1, description="Database; created on first write")
    collection: str = Field(..., min_length=1, description="Collection; created on first write")
    mode: WriteMode = Field(
        "append",
        description="append: insert the rows. overwrite: drop the collection, then insert. "
        "upsert: replace or update the documents matching merge_keys, insert the others",
    )
    merge_keys: list[str] = Field(
        default_factory=list,
        validate_default=True,
        description="Columns that identify a document (e.g. order_id, or _id when the data has one). "
        "Must be unique within the data; an index on them keeps upserts fast",
        json_schema_extra=when("mode", applies=("upsert",), required=("upsert",)),
    )
    on_match: OnMatch = Field(
        "replace",
        validate_default=True,
        description="replace: the matching document becomes the incoming row. update: only the row's "
        "columns are set on the document ($set); its other fields are kept",
        json_schema_extra=when("mode", applies=("upsert",)),
    )
    deterministic_ids: bool = Field(
        True,
        validate_default=True,
        description="Give appended documents an id derived from the pipeline, the micro-batch (or run) and the row, "
        "and write them as upserts: a micro-batch replayed after a restart, or a write retried after a network "
        "drop, rewrites the same documents instead of adding copies. A column named _id is used as is. Off: "
        "MongoDB assigns ObjectIds and every replay adds copies",
        json_schema_extra=when("mode", applies=("append",)),
    )
    retries: int = Field(
        3,
        ge=0,
        description="How many times a write is retried after a transient error (server unreachable, connection "
        "dropped, primary stepping down). Every mode is idempotent, so a retry never adds copies; turn "
        "deterministic_ids on for appends",
    )
    retry_backoff_seconds: float = Field(5.0, ge=0, description="Wait before the first retry; doubles on each further one")
    ignore_null_values: bool = Field(False, description="Leave null columns out of the documents instead of storing them as null")
    max_batch_size: int = Field(512, ge=1, description="Documents per bulk write")
    ordered: bool = Field(
        True,
        description="Ordered bulk writes stop at the first error; unordered ones go on with the remaining documents",
    )
    write_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Other MongoDB Spark Connector write options, e.g. writeConcern.w: majority, "
        "convertJson: true (parse JSON strings into documents), comment: a note for the server logs",
    )

    # Field validators (not a model validator) so each error is reported on its own field,
    # which is where the UI shows it. ``info.data`` holds the fields declared above.

    _uri = field_validator("connection_uri")(valid_uri)
    _database = field_validator("database")(valid_database)
    _collection = field_validator("collection")(valid_collection)

    @field_validator("write_options")
    @classmethod
    def _no_owned_options(cls, value: dict[str, OptionValue]) -> dict[str, OptionValue]:
        owned = [f"{k} (use {_OWNED_OPTIONS[k]})" for k in value if k in _OWNED_OPTIONS]
        if owned:
            raise ValueError(f"set through the sink's own fields: {', '.join(owned)}")
        return value

    @field_validator("merge_keys", mode="after")
    @classmethod
    def _merge_keys_with_upsert(cls, value: list[str], info: ValidationInfo) -> list[str]:
        value = [k.strip() for k in value]
        mode = info.data.get("mode", "append")
        if mode == "upsert" and not value:
            raise ValueError("required for mode 'upsert'")
        if mode != "upsert" and value:
            raise ValueError("only applies to mode 'upsert'")
        if any(not k for k in value):
            raise ValueError("must not contain empty names")
        return value

    # -- derived ---------------------------------------------------------------------------

    def effective_uri(self) -> str:
        return effective_uri(self.connection_uri, self.username, self.password)

    def host(self) -> str:
        return host_of(self.connection_uri)

    def writer_options(self) -> dict[str, str]:
        options: dict[str, str] = {
            "connection.uri": self.effective_uri(),
            "database": self.database,
            "collection": self.collection,
            "maxBatchSize": str(self.max_batch_size),
            "ordered": str(self.ordered).lower(),
            "ignoreNullValues": str(self.ignore_null_values).lower(),
        }
        if self.mode == "upsert":
            options["operationType"] = self.on_match
            options["idFieldList"] = ",".join(self.merge_keys)
            options["upsertDocument"] = "true"
        else:
            options["operationType"] = "insert"
        return {**spark_options(self.write_options), **options}


class MongoDbSink(Sink):
    """MongoDB collections: append, overwrite or upsert (replace/update on key columns)."""

    type_name = "mongodb"
    display_name = "MongoDB"
    icon = "database"
    Options = MongoDbSinkOptions
    spark_packages = (CONNECTOR_PACKAGE,)

    def describe(self, opts: MongoDbSinkOptions) -> str:
        target = f"{opts.database}.{opts.collection}"
        how = f"{opts.mode} on {', '.join(opts.merge_keys)}" if opts.mode == "upsert" else opts.mode
        return f"mongodb -> {target} ({how}) at {opts.host()}"

    # -- design-time checks ----------------------------------------------------------------

    def validate_input(self, ctx, schema: StructType, opts: MongoDbSinkOptions) -> list[tuple[str, str]]:
        """What would fail at write time, found while the pipeline is drawn: merge keys the
        data does not have, and column names MongoDB refuses as field names."""
        columns = [f.name for f in schema.fields]
        issues: list[tuple[str, str]] = []
        missing = [k for k in opts.merge_keys if k not in columns]
        if missing:
            hint = " (add a column named _id upstream, or choose another key)" if _ID_FIELD in missing else ""
            issues.append(("merge_keys", f"not in the data: {', '.join(missing)}{hint} (columns: {', '.join(columns)})"))
        bad = [m for m in (field_name_issue(c) for c in columns) if m]
        if bad:
            issues.append(("", "; ".join(bad) + "; rename them in the SQL upstream"))
        existing = existing_schema(ctx.spark, opts)
        if existing is not None:
            issues += compatibility_issues(schema, existing, opts)
        return issues

    # -- write -----------------------------------------------------------------------------

    def write(self, ctx, df: DataFrame, opts: MongoDbSinkOptions) -> None:
        missing = [k for k in opts.merge_keys if k not in df.columns]
        if missing:
            raise TributaryError(f"merge_keys not in the data: {', '.join(missing)} (columns: {', '.join(df.columns)})")
        # Spark's overwrite is the connector's truncate: the collection is dropped, then
        # written. Upserts are appends from Spark's point of view; the operation type
        # decides what happens to each document.
        mode = "overwrite" if opts.mode == "overwrite" else "append"
        options = opts.writer_options()
        idempotent = opts.mode != "append" or opts.deterministic_ids
        if opts.mode == "append" and opts.deterministic_ids:
            df = with_deterministic_ids(ctx, df)
            options.update({"operationType": "replace", "idFieldList": _ID_FIELD, "upsertDocument": "true"})
        self._write_with_retries(ctx, df, mode, options, opts, retry=idempotent)
        if ctx.batch_id is not None:
            ctx.log.info("mongodb %s.%s: micro-batch %s written (%s)", opts.database, opts.collection, ctx.batch_id, opts.mode)

    def _write_with_retries(self, ctx, df: DataFrame, mode: str, options: dict[str, str], opts: MongoDbSinkOptions, *, retry: bool) -> None:
        """Retries transient failures (server gone, connection dropped, failover) with
        exponential backoff. Only when the write is idempotent: a plain append without
        deterministic ids could have landed part of the batch, and a retry would copy it."""
        attempt = 0
        while True:
            try:
                df.write.format(FORMAT).mode(mode).options(**options).save()
                return
            except Exception as exc:  # noqa: BLE001 - classified below
                if not retry or attempt >= opts.retries or not is_transient(exc):
                    raise
                delay = opts.retry_backoff_seconds * (2**attempt)
                attempt += 1
                ctx.log.warning(
                    "mongodb %s.%s: write failed with a transient error (%s); retry %d/%d in %.0fs",
                    opts.database, opts.collection, summarize(exc), attempt, opts.retries, delay,
                )
                time.sleep(delay)



def with_deterministic_ids(ctx, df: DataFrame) -> DataFrame:
    """An ``_id`` that the same row gets again when the same micro-batch (or, in batch
    mode, the same run) is written again: sha1 of the scope, the row's content and the
    row's rank among identical rows in the batch, so genuine duplicates within one batch
    stay distinct while a replay maps onto the documents already there. A column named
    ``_id`` already identifies the row and is kept."""
    if _ID_FIELD in df.columns:
        return df
    scope = f"{ctx.pipeline_id}/batch/{ctx.batch_id}" if ctx.batch_id is not None else f"{ctx.pipeline_id}/run/{ctx.run_id}"
    content = F.sha1(F.to_json(F.struct(*[F.col(c) for c in df.columns])))
    rank = F.row_number().over(Window.partitionBy("__content").orderBy("__content"))
    return (
        df.withColumn("__content", content)
        .withColumn(_ID_FIELD, F.sha1(F.concat(F.lit(scope + ":"), F.col("__content"), F.lit(":"), rank.cast("string"))))
        .drop("__content")
        .select(_ID_FIELD, *df.columns)
    )


def existing_schema(spark, opts: MongoDbSinkOptions) -> StructType | None:
    """The schema the connector infers from a sample of the collection, or None when there
    is no collection yet or the server cannot be reached from here (best effort, design
    time only: a short server selection timeout keeps validation snappy)."""
    try:
        options = {
            "connection.uri": design_time_uri(opts.effective_uri()),
            "database": opts.database,
            "collection": opts.collection,
            "partitioner": SINGLE_PARTITION,
            "sampleSize": "200",
        }
        schema = spark.read.format(FORMAT).options(**options).load().schema
        return schema if schema.fields else None
    except Exception:  # noqa: BLE001 - nothing to compare with
        return None


def compatibility_issues(data: StructType, existing: StructType, opts: MongoDbSinkOptions) -> list[tuple[str, str]]:
    """Type drift, found while the pipeline is drawn: MongoDB accepts any type in any
    field, so a column whose type no longer matches the documents already there would
    silently make the collection mixed (and an upsert on such a key would never match its
    existing documents, duplicating them instead). New columns are fine: documents are
    schemaless. Compared by family (int/long, float/double, ...) since BSON has fewer types
    than Spark."""
    existing_types = {f.name: f.dataType for f in existing.fields}
    drifted = [
        (f.name, f.dataType.simpleString(), existing_types[f.name].simpleString())
        for f in data.fields
        if f.name in existing_types and type_family(f.dataType) != type_family(existing_types[f.name])
    ]
    issues: list[tuple[str, str]] = []
    keys = [d for d in drifted if d[0] in opts.merge_keys]
    if keys:
        issues.append(("merge_keys", "; ".join(
            f"{name} is {existing} in the collection but {data} in the data: upserts would not match the existing "
            f"documents and would add copies" for name, data, existing in keys
        ) + "; cast it in the SQL upstream"))
    rest = [d for d in drifted if d[0] not in opts.merge_keys]
    if rest:
        issues.append(("", "; ".join(
            f"{name} is {existing} in the collection (sampled) but {data} in the data" for name, data, existing in rest
        ) + "; cast it in the SQL upstream, or the field ends up with mixed types"))
    return issues


def summarize(exc: Exception) -> str:
    """The first line that names a MongoDB error, for logs."""
    for line in str(exc).splitlines():
        if "com.mongodb." in line:
            return line.strip()[:200]
    return str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__
