"""MongoDB collections, read through the official MongoDB Spark Connector.

Every document becomes a row. Without a ``schema:`` block the connector infers the schema
from a sample of the collection (embedded documents become structs, arrays stay arrays,
dates become timestamps, ObjectIds strings); a ``schema`` of kind ``ddl`` or ``static``
pins the columns and their types instead. An aggregation pipeline can filter or reshape
the documents on the server before Spark sees them.

To work without a server, turn on the node's dummy data in the ``local`` environment (see
``plugins/sources/dummy.py``).
"""

from __future__ import annotations

import json
from typing import Literal

from pydantic import Field, field_validator
from pyspark.sql import DataFrame

from ...core import BatchReadable, PluginOptions, Source
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


class MongoDbSourceOptions(PluginOptions):
    # --- connection: typically provided by a connection of type 'mongodb' ----------------
    connection_uri: str = Field(..., description=URI_DESCRIPTION)
    username: str | None = Field(None, description="Added to the URI when it has no credentials of its own")
    password: str | None = Field(None, description="Use ${secret:...}; added to the URI with username")
    # --- what to read ----------------------------------------------------------------------
    database: str = Field(..., description="Database")
    collection: str = Field(..., description="Collection")
    pipeline: str | None = Field(
        None,
        description="MongoDB aggregation pipeline run on the server before the documents reach Spark, as a JSON "
        'array of stages, e.g. [{"$match": {"status": "active"}}, {"$project": {"_id": 0}}]',
        json_schema_extra={"format": "json"},
    )
    on_malformed: OnMalformed = Field(
        "null",
        description="With a declared schema, what to do with a document whose field does not fit its type "
        "(a string where the schema says double, ...): null: read that field as null. drop: leave the document "
        "out. fail: stop the run. Without a schema nothing is malformed: a field with mixed types is read as string",
    )
    sample_size: int = Field(1000, ge=1, description="Documents sampled to infer the schema when none is declared")
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Other MongoDB Spark Connector read options, e.g. partitioner, schemaHints, outputExtendedJson",
    )

    # Field validators (not a model validator) so each error is reported on its own field,
    # which is where the UI shows it.

    @field_validator("connection_uri")
    @classmethod
    def _uri(cls, value: str) -> str:
        return valid_uri(value)

    @field_validator("database")
    @classmethod
    def _database(cls, value: str) -> str:
        return valid_database(value)

    @field_validator("collection")
    @classmethod
    def _collection(cls, value: str) -> str:
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

    def host(self) -> str:
        return host_of(self.connection_uri)

    def reader_options(self) -> dict[str, str]:
        options = {
            "connection.uri": effective_uri(self.connection_uri, self.username, self.password),
            "database": self.database,
            "collection": self.collection,
            "sampleSize": str(self.sample_size),
        }
        if self.pipeline:
            options["aggregation.pipeline"] = self.pipeline
        options["mode"] = _PARSE_MODES[self.on_malformed]
        return {**spark_options(self.read_options), **options}


class MongoDbSource(Source, BatchReadable):
    """MongoDB collections."""

    type_name = "mongodb"
    display_name = "MongoDB"
    icon = "database"
    Options = MongoDbSourceOptions
    spark_packages = (CONNECTOR_PACKAGE,)

    def read(self, ctx, opts: MongoDbSourceOptions, schema) -> DataFrame:
        reader = ctx.spark.read.format(FORMAT).options(**opts.reader_options())
        if schema is not None and schema.struct is not None:
            reader = reader.schema(schema.struct)
        return reader.load()

    def output_schema(self, opts: MongoDbSourceOptions, schema):
        # Declared schema or nothing: a collection's columns are inferred by reading it.
        return schema.struct if schema else None

    def describe(self, opts: MongoDbSourceOptions) -> str:
        return f"mongodb <- {opts.database}.{opts.collection} at {opts.host()}"

