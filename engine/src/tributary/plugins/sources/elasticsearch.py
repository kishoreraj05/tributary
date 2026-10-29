"""Elasticsearch indices, read through the official ES-Hadoop Spark connector.

The connector (``org.elasticsearch:elasticsearch-spark-30``) turns an index, alias or
index pattern into a DataFrame: one Spark partition per shard, the documents' ``_source``
as columns, with the schema taken from the index mapping. A schema declared on the node
selects and casts those columns (and lets the editor know them without a cluster round
trip). Reads are batch only (the connector has no Structured Streaming reader); in a streaming
pipeline an Elasticsearch index can still be a static lookup source.

Every option maps to one ``es.*`` connector setting (see ``reader_options``), and anything
the form does not cover can be passed verbatim in ``read_options``.

With ``use_dummy_data`` on, the node stands in for the index: it emits the documents given
in ``dummy_data`` (a JSON array, 100 at most) in previews and runs alike, shaped the way
the connector would shape them, and never contacts a cluster. That is how a pipeline is
drawn and tested before the index (or access to it) exists.
"""

from __future__ import annotations

import json
import os
from typing import Any

from pydantic import Field, ValidationInfo, field_validator

from ...core import BatchReadable, PluginOptions, Source
from ...errors import TributaryError
from .._util import OptionValue, has_placeholder, spark_options, when

# 8.13.2 is the last ES-Hadoop release published to Maven Central (later ones live in
# Elastic's own repository); it reads from Elasticsearch 7.x and 8.x clusters. Override
# with TRIBUTARY_ES_HADOOP_VERSION when another repository is configured.
ES_HADOOP_VERSION = os.environ.get("TRIBUTARY_ES_HADOOP_VERSION", "8.13.2")
ES_HADOOP_PACKAGE = f"org.elasticsearch:elasticsearch-spark-30_{{scala_version}}:{ES_HADOOP_VERSION}"
# The connector's POM drags in Spark 3.3 modules and an old Scala. Spark's package resolver
# already drops most of its own modules; these are the ones it lets through, and a Spark
# session must never see a second copy of them.
ES_HADOOP_EXCLUDES = (
    "org.apache.spark:spark-yarn_2.12",
    "org.apache.spark:spark-yarn_2.13",
    "org.scala-lang:scala-library",
    "org.scala-lang:scala-reflect",
)
ES_FORMAT = "org.elasticsearch.spark.sql"
METADATA_COLUMN = "_metadata"
DUMMY_MAX_DOCUMENTS = 100
_DUMMY_ID = "__tributary_dummy_id"  # carries a document's _id through the JSON reader


class ElasticsearchSourceOptions(PluginOptions):
    # Declared first: the cluster fields below are only mandatory when this is off, and
    # pydantic validates fields in declaration order (the validators read it from info.data).
    use_dummy_data: bool = Field(
        False,
        description="Stand in for the index with the documents in dummy_data (previews and runs alike) and "
        "never contact Elasticsearch. For drawing and testing a pipeline before the index, or access to it, exists",
    )
    dummy_data: list[dict[str, Any]] | str | None = Field(
        None,
        validate_default=True,
        description=f'JSON array of documents, {DUMMY_MAX_DOCUMENTS} at most, e.g. [{{"order_id": 1, "amount": 9.5}}]. '
        "Types are inferred as from an index (numbers, strings, booleans, nested objects, arrays); declare a "
        "schema on the node to fix them. A document's \"_id\" becomes _metadata._id when include_metadata is on",
        json_schema_extra={**when("use_dummy_data", applies=(True,), required=(True,)), "format": "json"},
    )
    # --- cluster: typically provided by a connection of type 'elasticsearch' ------------
    nodes: str | None = Field(
        None,
        validate_default=True,
        description="Elasticsearch hosts, comma separated (host or host:port, or http(s)://host:port). "
        "The port defaults to 9200",
        json_schema_extra=when("use_dummy_data", required=(False,)),
    )
    port: int | None = Field(None, ge=1, le=65535, description="Default port for nodes listed without one")
    user: str | None = Field(None, description="Basic auth user")
    password: str | None = Field(None, description="Basic auth password. Use ${secret:...}")
    ssl: bool = Field(False, description="Connect over HTTPS")
    ssl_allow_self_signed: bool = Field(
        False, description="Accept the cluster's self-signed certificate (what Elasticsearch 8 generates by default)"
    )
    wan_only: bool = Field(
        True,
        description="Talk only to the nodes listed above, never to the addresses the cluster publishes "
        "(needed with Docker, cloud and proxied clusters). Turn off inside the cluster's network so "
        "every shard is read from its own node",
    )
    # --- what to read ------------------------------------------------------------------
    index: str | None = Field(
        None,
        validate_default=True,
        description="Index, alias or pattern, e.g. orders, orders-*, logs-2024.*",
        json_schema_extra=when("use_dummy_data", required=(False,)),
    )
    query: str | None = Field(
        None,
        description="Limits the documents read: a URI query (?q=country:us) or a Query DSL body "
        '({"query": {"range": {"amount": {"gte": 10}}}}). Everything when empty',
    )
    fields: list[str] = Field(
        default_factory=list,
        description="Read only these fields of _source (a.b selects nested fields, a.* a whole object). All when empty",
    )
    array_fields: list[str] = Field(
        default_factory=list,
        description="Fields holding arrays (a.b for nested ones). Mappings do not say which fields are arrays, "
        "so a field not listed here is read as a scalar, and the read fails on the first document where it "
        "is an array ('JListWrapper is not a valid external type')",
    )
    include_metadata: bool = Field(
        False, description=f"Add a {METADATA_COLUMN} map column with the document's _index, _id and _score"
    )
    scroll_size: int = Field(1000, ge=1, le=10000, description="Documents per scroll request, per shard")
    read_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Extra connector settings, with or without the es. prefix (es.read.field.exclude, "
        "es.net.proxy.http.host, es.mapping.date.rich, ...)",
    )

    @field_validator("dummy_data")
    @classmethod
    def _documents(cls, value: Any, info: ValidationInfo) -> list[dict[str, Any]] | None:
        """A JSON array (the form sends text, YAML a list) of at most DUMMY_MAX_DOCUMENTS objects;
        mandatory in dummy mode, kept (and ignored) otherwise."""
        if isinstance(value, str):
            if has_placeholder(value):
                return None if not value.strip() else value  # resolved later; nothing to check yet
            if not value.strip():
                value = None
            else:
                try:
                    value = json.loads(value)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"not valid JSON: {exc.msg} (line {exc.lineno})") from None
        if value is None:
            if info.data.get("use_dummy_data"):
                raise ValueError("required when use_dummy_data is on")
            return None
        if not isinstance(value, list):
            raise ValueError("must be a JSON array of documents")
        if not value:
            raise ValueError("must hold at least one document")
        if len(value) > DUMMY_MAX_DOCUMENTS:
            raise ValueError(f"at most {DUMMY_MAX_DOCUMENTS} documents ({len(value)} given)")
        bad = [i for i, doc in enumerate(value) if not isinstance(doc, dict)]
        if bad:
            raise ValueError(f"every item must be a JSON object (document); item {bad[0]} is not")
        return value

    @field_validator("nodes", "index")
    @classmethod
    def _cluster_field(cls, value: str | None, info: ValidationInfo) -> str | None:
        value = (value or "").strip()
        if not value:
            if info.data.get("use_dummy_data"):
                return None  # nothing is contacted in dummy mode
            raise ValueError("required unless use_dummy_data is on")
        return value

    @field_validator("query")
    @classmethod
    def _query_is_uri_or_json(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        value = value.strip()
        if value.startswith("?") or has_placeholder(value):
            return value
        if value.startswith("{"):
            try:
                json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"not valid JSON: {exc.msg} (line {exc.lineno})") from None
            return value
        raise ValueError("must be a URI query starting with '?' (e.g. ?q=country:us) or a JSON Query DSL body")


class ElasticsearchSource(Source, BatchReadable):
    """Elasticsearch indices (documents as rows, schema from the mapping or declared)."""

    type_name = "elasticsearch"
    display_name = "Elasticsearch"
    icon = "search"
    Options = ElasticsearchSourceOptions
    spark_packages = (ES_HADOOP_PACKAGE,)
    spark_conf = {"spark.jars.excludes": ",".join(ES_HADOOP_EXCLUDES)}

    # A dummy-data pipeline needs neither the connector nor Maven.
    def required_packages(self, opts: ElasticsearchSourceOptions) -> list[str]:
        return [] if opts.use_dummy_data else list(self.spark_packages)

    def required_conf(self, opts: ElasticsearchSourceOptions) -> dict[str, str]:
        return {} if opts.use_dummy_data else dict(self.spark_conf)

    def read(self, ctx, opts: ElasticsearchSourceOptions, schema):
        if opts.use_dummy_data:
            df = dummy_frame(ctx, opts)
        else:
            df = ctx.spark.read.format(ES_FORMAT).options(**reader_options(opts)).load(opts.index)
        declared = self.output_schema(opts, schema)
        if declared is None:
            return df
        # The connector parses documents by the index mapping whatever schema it is handed
        # (a declared LONG over an integer field fails on the first row), so a declared
        # schema is applied on top: its columns, in its order, cast to its types. A column
        # the index does not have is an analysis error, reported on the node.
        from pyspark.sql import functions as F

        return df.select(*[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in declared.fields])

    def output_schema(self, opts: ElasticsearchSourceOptions, schema):
        """Known without touching the cluster only when the node declares a schema; the
        metadata column, when requested, comes after the declared fields."""
        if schema is None or schema.struct is None:
            return None
        if not opts.include_metadata:
            return schema.struct
        from pyspark.sql.types import MapType, StringType, StructField, StructType

        fields = [f for f in schema.struct.fields if f.name != METADATA_COLUMN]
        return StructType([*fields, StructField(METADATA_COLUMN, MapType(StringType(), StringType()))])


def reader_options(opts: ElasticsearchSourceOptions) -> dict[str, str]:
    """The connector settings for these options. ``read_options`` come last, so a setting
    given there wins over the form's, which keeps every ``es.*`` knob reachable."""
    settings: dict[str, Any] = {
        "es.nodes": opts.nodes,
        "es.nodes.wan.only": opts.wan_only,
        "es.net.ssl": opts.ssl,
        "es.read.metadata": opts.include_metadata,
        "es.scroll.size": opts.scroll_size,
    }
    if opts.port is not None:
        settings["es.port"] = opts.port
    if opts.user:
        settings["es.net.http.auth.user"] = opts.user
    if opts.password:
        settings["es.net.http.auth.pass"] = opts.password
    if opts.ssl_allow_self_signed:
        settings["es.net.ssl.cert.allow.self.signed"] = True
    if opts.query:
        settings["es.query"] = opts.query
    if opts.fields:
        settings["es.read.field.include"] = ",".join(opts.fields)
    if opts.array_fields:
        settings["es.read.field.as.array.include"] = ",".join(opts.array_fields)
    for key, value in opts.read_options.items():
        settings[key if key.startswith("es.") else f"es.{key}"] = value
    return spark_options(settings)


def dummy_frame(ctx, opts: ElasticsearchSourceOptions):
    """The dummy documents as the connector would deliver them: one row each, columns from
    the documents' fields in alphabetical order with types inferred from the values, and
    the ``_metadata`` map (index, id, score) when asked for. Nothing is contacted."""
    from pyspark.sql import functions as F

    documents = opts.dummy_data
    if not isinstance(documents, list):  # a placeholder that was never resolved
        raise TributaryError("dummy_data must be a JSON array of documents when use_dummy_data is on")
    ctx.log.warning(
        "elasticsearch source uses dummy data (%d documents); no cluster is contacted", len(documents)
    )
    lines = []
    for position, document in enumerate(documents, start=1):
        fields = {k: v for k, v in document.items() if k != "_id"}
        fields[_DUMMY_ID] = str(document.get("_id", position))
        lines.append(json.dumps(fields))
    # Through the JSON reader so the values get the types an index mapping would give them.
    df = ctx.spark.read.json(ctx.spark.sparkContext.parallelize(lines, 1))
    columns = sorted(c for c in df.columns if c != _DUMMY_ID)
    if not opts.include_metadata:
        return df.select(*columns)
    metadata = F.create_map(
        F.lit("_index"), F.lit(opts.index or "dummy"), F.lit("_id"), F.col(_DUMMY_ID), F.lit("_score"), F.lit("1.0")
    )
    return df.select(*columns, metadata.alias(METADATA_COLUMN))
