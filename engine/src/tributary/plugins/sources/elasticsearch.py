"""Elasticsearch indices, read through the official ES-Hadoop Spark connector.

The connector (``org.elasticsearch:elasticsearch-spark-30``) turns an index, alias or
index pattern into a DataFrame: one Spark partition per shard, the documents' ``_source``
as columns, with the schema taken from the index mapping. A schema declared on the node
selects and casts those columns (and lets the editor know them without a cluster round
trip). Reads are batch only (the connector has no Structured Streaming reader); in a streaming
pipeline an Elasticsearch index can still be a static lookup source.

Every option maps to one ``es.*`` connector setting (see ``reader_options``), and anything
the form does not cover can be passed verbatim in ``read_options``.

To work without a cluster, turn on the node's dummy data in the ``local`` environment (see
``plugins/sources/dummy.py``).
"""

from __future__ import annotations

import json
import os
from typing import Any

from pydantic import Field, field_validator

from ...core import BatchReadable, PluginOptions, Source
from .._util import OptionValue, has_placeholder, spark_options

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


class ElasticsearchSourceOptions(PluginOptions):
    # --- cluster: typically provided by a connection of type 'elasticsearch' ------------
    nodes: str = Field(
        ...,
        description="Elasticsearch hosts, comma separated (host or host:port, or http(s)://host:port). "
        "The port defaults to 9200",
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
    index: str = Field(..., description="Index, alias or pattern, e.g. orders, orders-*, logs-2024.*")
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

    @field_validator("nodes", "index")
    @classmethod
    def _cluster_field(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("required")
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

    def read(self, ctx, opts: ElasticsearchSourceOptions, schema):
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

