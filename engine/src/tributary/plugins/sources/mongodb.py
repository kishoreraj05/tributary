"""MongoDB collections, read through the official MongoDB Spark Connector.

Every document becomes a row. Without a ``schema:`` block the connector infers the schema
from a sample of the collection (embedded documents become structs, arrays stay arrays,
dates become timestamps, ObjectIds strings); a ``schema`` of kind ``ddl`` or ``static``
pins the columns and their types instead. An aggregation pipeline can filter or reshape
the documents on the server before Spark sees them.

Dummy data: with ``use_dummy_data`` on, the node never talks to a server. It hands the
records given in ``dummy_data`` (a JSON array of up to 100 documents) to the downstream
nodes exactly as a collection holding those documents would, so a pipeline can be drawn,
tested and run before the real collection is reachable, or to stand in for one during a
demo. The schema comes from the declared ``schema:`` when there is one, otherwise it is
inferred from the records the way Spark infers JSON (whole numbers -> bigint, decimals ->
double, nested objects -> structs; ISO timestamps stay strings unless a schema types them).
"""

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame

from ...core import BatchReadable, PluginOptions, Source
from ...errors import TributaryError
from .._mongodb import (
    CONNECTOR_PACKAGE,
    FORMAT,
    URI_DESCRIPTION,
    effective_uri,
    host_of,
    valid_collection,
    valid_database,
    valid_uri,
)
from .._util import OptionValue, spark_options, when

MAX_DUMMY_RECORDS = 100
# Connector options the source's own fields stand for; read_options cannot set them.
_OWNED_OPTIONS = {
    "connection.uri": "connection_uri",
    "database": "database",
    "collection": "collection",
    "aggregation.pipeline": "pipeline",
    "mode": "on_malformed",
}
OnMalformed = Literal["null", "drop", "fail"]
_PARSE_MODES = {"null": "PERMISSIVE", "drop": "DROPMALFORMED", "fail": "FAILFAST"}
REAL, DUMMY = (False,), (True,)


class MongoDbSourceOptions(PluginOptions):
    # Declared first: the validators of the fields below look at it (pydantic validates in
    # declaration order and only exposes the fields already validated).
    use_dummy_data: bool = Field(
        False,
        description="Do not connect to MongoDB: hand the records in dummy_data to the downstream nodes "
        "instead, as if they were the collection. For drawing and testing a pipeline before the "
        "collection is reachable",
    )
    # Typed Any (a JSON string from the UI, a list from YAML) so that every problem is reported
    # on this field rather than on a union branch; the schema says "JSON text" for the form.
    dummy_data: Any = Field(
        None,
        validate_default=True,
        description=f"A JSON array of up to {MAX_DUMMY_RECORDS} documents, e.g. "
        '[{"order_id": 1, "amount": 25.5, "country": "IN"}]. Declare a schema to type the columns '
        "(timestamps, decimals); without one the types are inferred from the values",
        json_schema_extra={**when("use_dummy_data", applies=DUMMY, required=DUMMY), "type": "string", "format": "json"},
    )
    # --- connection: typically provided by a connection of type 'mongodb' ----------------
    connection_uri: str | None = Field(
        None,
        validate_default=True,
        description=URI_DESCRIPTION,
        json_schema_extra=when("use_dummy_data", applies=REAL, required=REAL),
    )
    username: str | None = Field(
        None,
        description="Added to the URI when it has no credentials of its own",
        json_schema_extra=when("use_dummy_data", applies=REAL),
    )
    password: str | None = Field(
        None,
        description="Use ${secret:...}; added to the URI with username",
        json_schema_extra=when("use_dummy_data", applies=REAL),
    )
    # --- what to read ----------------------------------------------------------------------
    database: str | None = Field(
        None, validate_default=True, description="Database", json_schema_extra=when("use_dummy_data", applies=REAL, required=REAL)
    )
    collection: str | None = Field(
        None, validate_default=True, description="Collection", json_schema_extra=when("use_dummy_data", applies=REAL, required=REAL)
    )
    pipeline: str | None = Field(
        None,
        description="MongoDB aggregation pipeline run on the server before the documents reach Spark, as a JSON "
        'array of stages, e.g. [{"$match": {"status": "active"}}, {"$project": {"_id": 0}}]',
        json_schema_extra={**when("use_dummy_data", applies=REAL), "format": "json"},
    )
    on_malformed: OnMalformed = Field(
        "null",
        description="With a declared schema, what to do with a document whose field does not fit its type "
        "(a string where the schema says double, ...): null: read that field as null. drop: leave the document "
        "out. fail: stop the run. Without a schema nothing is malformed: a field with mixed types is read as string",
    )
    sample_size: int = Field(
        1000,
        ge=1,
        description="Documents sampled to infer the schema when none is declared",
        json_schema_extra=when("use_dummy_data", applies=REAL),
    )
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Other MongoDB Spark Connector read options, e.g. partitioner, schemaHints, outputExtendedJson",
        json_schema_extra=when("use_dummy_data", applies=REAL),
    )

    # Field validators (not a model validator) so each error is reported on its own field,
    # which is where the UI shows it. ``info.data`` holds the fields declared above.

    @field_validator("dummy_data")
    @classmethod
    def _records(cls, value: Any, info: ValidationInfo) -> list[dict[str, Any]] | None:
        """The UI sends the array as text; YAML can give the list itself."""
        if isinstance(value, str):
            value = value.strip() or None
            if value is not None:
                try:
                    value = json.loads(value)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})") from None
        if not info.data.get("use_dummy_data", False):
            if value:
                raise ValueError("only applies when use_dummy_data is on")
            return None
        if value is None or (isinstance(value, list) and not value):
            raise ValueError("required when use_dummy_data is on: at least one document")
        if not isinstance(value, list):
            raise ValueError("must be a JSON array of documents, e.g. [{\"id\": 1}]")
        if len(value) > MAX_DUMMY_RECORDS:
            raise ValueError(f"at most {MAX_DUMMY_RECORDS} documents (got {len(value)})")
        bad = [i for i, record in enumerate(value) if not isinstance(record, dict)]
        if bad:
            raise ValueError(f"every element must be a JSON object (document); element {bad[0]} is not")
        return value

    @field_validator("connection_uri", mode="after")
    @classmethod
    def _uri_unless_dummy(cls, value: str | None, info: ValidationInfo) -> str | None:
        if info.data.get("use_dummy_data", False):
            return value  # ignored; a connection may still supply it
        if not value:
            raise ValueError("required unless use_dummy_data is on")
        return valid_uri(value)

    @field_validator("database", mode="after")
    @classmethod
    def _database_unless_dummy(cls, value: str | None, info: ValidationInfo) -> str | None:
        if info.data.get("use_dummy_data", False):
            return value
        if not value:
            raise ValueError("required unless use_dummy_data is on")
        return valid_database(value)

    @field_validator("collection", mode="after")
    @classmethod
    def _collection_unless_dummy(cls, value: str | None, info: ValidationInfo) -> str | None:
        if info.data.get("use_dummy_data", False):
            return value
        if not value:
            raise ValueError("required unless use_dummy_data is on")
        return valid_collection(value)

    @field_validator("pipeline")
    @classmethod
    def _pipeline_is_a_json_array(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        try:
            stages = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})") from None
        if not isinstance(stages, list) or not all(isinstance(s, dict) for s in stages):
            raise ValueError("must be a JSON array of stages, e.g. [{\"$match\": {...}}]")
        return value

    @field_validator("read_options")
    @classmethod
    def _no_owned_options(cls, value: dict[str, OptionValue]) -> dict[str, OptionValue]:
        owned = [f"{k} (use {_OWNED_OPTIONS[k]})" for k in value if k in _OWNED_OPTIONS]
        if owned:
            raise ValueError(f"set through the source's own fields: {', '.join(owned)}")
        return value

    # -- derived ---------------------------------------------------------------------------

    @property
    def records(self) -> list[dict[str, Any]]:
        return self.dummy_data if isinstance(self.dummy_data, list) else []

    def host(self) -> str:
        return host_of(self.connection_uri) if self.connection_uri else "?"

    def reader_options(self) -> dict[str, str]:
        options = {
            "connection.uri": effective_uri(self.connection_uri or "", self.username, self.password),
            "database": self.database or "",
            "collection": self.collection or "",
            "sampleSize": str(self.sample_size),
        }
        if self.pipeline:
            options["aggregation.pipeline"] = self.pipeline
        options["mode"] = _PARSE_MODES[self.on_malformed]
        return {**spark_options(self.read_options), **options}


class MongoDbSource(Source, BatchReadable):
    """MongoDB collections, or a dump of dummy documents that stands in for one."""

    type_name = "mongodb"
    display_name = "MongoDB"
    icon = "database"
    Options = MongoDbSourceOptions
    spark_packages = (CONNECTOR_PACKAGE,)

    def required_packages(self, opts: MongoDbSourceOptions) -> list[str]:
        # Dummy data needs no connector at all: a run of such a pipeline stays offline.
        return [] if opts.use_dummy_data else list(self.spark_packages)

    def read(self, ctx, opts: MongoDbSourceOptions, schema) -> DataFrame:
        if opts.use_dummy_data:
            return dummy_frame(ctx.spark, opts.records, schema, _PARSE_MODES[opts.on_malformed])
        reader = ctx.spark.read.format(FORMAT).options(**opts.reader_options())
        if schema is not None and schema.struct is not None:
            reader = reader.schema(schema.struct)
        return reader.load()

    def read_preview(self, ctx, opts: MongoDbSourceOptions, schema, limit: int) -> DataFrame:
        # All records go in even for a limit of 0 (analysis): the columns are inferred from them.
        return self.read(ctx, opts, schema).limit(limit)

    def output_schema(self, opts: MongoDbSourceOptions, schema):
        # Declared schema or nothing: dummy data and collections are both inferred by reading.
        return schema.struct if schema else None

    def estimate_input_bytes(self, opts: MongoDbSourceOptions) -> int | None:
        return len(json.dumps(opts.records)) if opts.use_dummy_data else None

    def describe(self, opts: MongoDbSourceOptions) -> str:
        if opts.use_dummy_data:
            return f"mongodb (dummy data: {len(opts.records)} documents)"
        return f"mongodb <- {opts.database}.{opts.collection} at {opts.host()}"


def dummy_frame(spark, records: list[dict[str, Any]], schema, parse_mode: str = "PERMISSIVE") -> DataFrame:
    """The records as a DataFrame, typed by the declared schema or inferred like JSON.

    Goes through Spark's JSON reader rather than ``createDataFrame(dicts)`` so that nested
    documents, arrays and mixed null/non-null values come out the way a real collection's
    inference would give them."""
    struct = schema.struct if schema is not None else None
    try:
        lines = [json.dumps(record) for record in records]
    except (TypeError, ValueError) as exc:
        raise TributaryError(f"dummy_data is not JSON-serialisable: {exc}") from None
    if struct is not None:
        # Spark's JSON reader has the same three parse modes as the connector.
        reader = spark.read.schema(struct).option("mode", parse_mode)
        if not lines:
            return spark.createDataFrame([], struct)
    else:
        reader = spark.read
        if not lines:
            raise TributaryError("dummy_data is empty and no schema is declared: nothing to infer the columns from")
    return reader.json(spark.sparkContext.parallelize(lines, 1))
