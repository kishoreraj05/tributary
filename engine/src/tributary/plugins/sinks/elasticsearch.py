"""Elasticsearch indices, written through the official Spark connector (ES-Hadoop).

Every row becomes one JSON document sent with the bulk API. With an ``id_column`` the
document id is that column's value, so writing the same rows again replaces the documents
instead of adding copies; that is also what makes a replayed streaming micro-batch
harmless. Without it Elasticsearch generates the ids.

Rows go out in bulk requests of ``batch_size_entries`` documents per task. What happens
when the cluster pushes back, as checked against a real cluster in the tests:

- Documents rejected with 429 (a shard's write queue is full) are retried in place, only
  those, after ``retry_wait``, up to ``retry_count`` times; nothing is indexed twice.
- A whole request refused with 429 (the coordinating node's queue is full) is not retried
  by the connector: the task fails, and Spark's task retry (or a re-run) writes the
  partition again. The documents of the bulk requests accepted before the failure are
  already in the index, so that is only safe with an ``id_column``.
- Documents the index refuses for good (a value that does not fit the field's mapping,
  a new field on a ``dynamic: strict`` index) fail the task after the rest of their bulk
  request was indexed, unless ``rejected_documents`` is ``skip``. The same partial-write
  rule applies: re-runs converge with an ``id_column``.

The connector needs no session settings, only its jar. Its POM declares Spark and Scala
artifacts as ordinary (not provided) dependencies, so resolving it with
``spark.jars.packages`` would also pull Spark 3.3 jars into a Spark 3.5 session; the
sink excludes those with ``spark.jars.excludes`` (the jar itself is self-contained).
"""

from __future__ import annotations

import base64
import json
import os
import re
import ssl
import urllib.error
import urllib.request
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    MapType,
    NumericType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from ...core import PluginOptions, Sink
from .._util import OptionValue, spark_options, when

# ES-Hadoop 8.x talks to Elasticsearch 7.x and 8.x clusters; the Spark 3 artifact runs on
# Spark 3.0-3.5. Override with TRIBUTARY_ELASTICSEARCH_VERSION to pick another release.
ELASTICSEARCH_VERSION = os.environ.get("TRIBUTARY_ELASTICSEARCH_VERSION", "8.13.2")
ELASTICSEARCH_PACKAGE = f"org.elasticsearch:elasticsearch-spark-30_{{scala_version}}:{ELASTICSEARCH_VERSION}"
# Declared by the connector's POM but already part of every Spark distribution (in older
# versions, even: spark-yarn 3.3.3, scala-reflect 2.12.8). Kept out of the session; an
# exclusion that matches nothing (the other Scala line) is harmless.
ELASTICSEARCH_EXCLUDES = (
    "org.apache.spark:spark-yarn_2.12",
    "org.apache.spark:spark-yarn_2.13",
    "org.scala-lang:scala-reflect",
    "org.slf4j:slf4j-api",
    "commons-logging:commons-logging",
    "javax.xml.bind:jaxb-api",
    "com.google.protobuf:protobuf-java",
)
EXCLUDES_KEY = "spark.jars.excludes"
FORMAT = "org.elasticsearch.spark.sql"

WriteMode = Literal["append", "overwrite", "upsert"]
# Spark SaveMode and ES-Hadoop's es.write.operation for each mode.
_OPERATIONS: dict[str, tuple[str, str]] = {
    "append": ("append", "index"),
    "overwrite": ("overwrite", "index"),
    "upsert": ("append", "upsert"),
}
ISO_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
REJECTED_LOGGER = "tributary.elasticsearch.rejected"
_DURATION = re.compile(r"^\d+(ms|s|m|h)?$")

# Index names: lowercase, none of \ / * ? " < > | space , # :, not starting with - _ +,
# not . or ..; between the literal parts, {column} or {column|date pattern} placeholders.
_PLACEHOLDER = re.compile(r"\{([^{}|]+)(?:\|[^{}]*)?\}")
_FORBIDDEN = re.compile(r'[\\/*?"<>| ,#:]')


class ElasticsearchSinkOptions(PluginOptions):
    # --- cluster: typically provided by a connection of type 'elasticsearch' -------------
    nodes: str = Field(
        ...,
        description="Nodes to connect to, comma separated (host or host:port), e.g. localhost:9200 or "
        "my-deployment.es.eu-west-1.aws.found.io:9243. The port defaults to 'port'",
    )
    port: int = Field(9200, ge=1, le=65535, description="Port for the nodes that do not name one")
    user: str | None = Field(None, description="Basic authentication user")
    password: str | None = Field(None, description="Basic authentication password. Use ${secret:...}")
    api_key: str | None = Field(
        None, description="API key (the base64 'encoded' value), sent instead of user/password. Use ${secret:...}"
    )
    ssl: bool = Field(False, description="Connect over HTTPS")
    ssl_allow_self_signed: bool = Field(
        False,
        description="Accept the cluster's certificate even when it is self-signed (the default of the Elasticsearch "
        "Docker image). Leave off for a certificate signed by a known CA",
        json_schema_extra=when("ssl", applies=(True,)),
    )
    wan_only: bool = Field(
        True,
        description="Talk only to the nodes listed above instead of discovering the cluster's data nodes and "
        "writing to them directly. Needed when the nodes' own addresses are not reachable from Spark (Docker, "
        "Elastic Cloud, a cluster behind a proxy); turn off for a cluster Spark can reach node by node",
    )

    # --- what and how to write ------------------------------------------------------------
    index: str = Field(
        ...,
        description="Index (or alias, or data stream) to write to, lowercase. May route rows by a column's value, "
        "which must be lowercase too: logs-{country}, or events-{event_ts|yyyy.MM.dd} for a timestamp",
    )
    mode: WriteMode = Field(
        "append",
        validate_default=True,
        description="append: index every row (a row whose id exists replaces the document). overwrite: delete every "
        "document in the index first (a fixed index name, not a pattern). upsert: update the documents whose id "
        "exists with the columns given, add the others; needs id_column",
    )
    id_column: str | None = Field(
        None,
        validate_default=True,
        description="Column used as the document _id, so writing a row again replaces its document rather than "
        "adding a copy (re-runs and replayed micro-batches stay idempotent). Without it Elasticsearch makes up ids",
        json_schema_extra=when("mode", required=("upsert",)),
    )
    timestamp_format: Literal["iso", "epoch_millis"] = Field(
        "iso",
        description="How TIMESTAMP and DATE columns are sent. iso: as text such as 2024-05-01T12:00:00.000Z, which "
        "Elasticsearch maps to a date field on its own. epoch_millis: as numbers, the connector's native form, "
        "for an index whose mapping already declares those fields as dates",
    )
    auto_create_index: bool = Field(
        True,
        description="Let Elasticsearch create the index with a dynamic mapping on the first write. Turn off to fail "
        "instead when the index (or its template) has not been set up",
    )
    batch_size_entries: int = Field(1000, ge=1, description="Documents per bulk request (per task)")
    retry_count: int = Field(
        3,
        ge=-1,
        description="How many times the documents a bulk request rejects with 429 (too many requests) are sent again "
        "before the task fails; -1 retries until they get in. Only the rejected documents are resent",
    )
    retry_wait: str = Field("10s", description="Pause before the rejected documents are sent again, e.g. 10s, 500ms, 1m")
    rejected_documents: Literal["fail", "skip"] = Field(
        "fail",
        description="What to do with a document the index refuses for good (a value that does not fit the field's "
        "mapping, a new field on a strict mapping). fail: the task fails; the other documents of that bulk request "
        "are already indexed, so re-run with an id_column. skip: log the document and carry on without it",
    )
    write_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Any other connector setting, e.g. es.mapping.routing: customer_id, es.batch.size.bytes: 5mb, "
        "es.net.proxy.http.host: proxy. The es. prefix may be omitted",
    )

    @field_validator("nodes")
    @classmethod
    def _nodes(cls, value: str) -> str:
        nodes = [n.strip() for n in value.split(",") if n.strip()]
        if not nodes:
            raise ValueError("at least one node is required")
        return ",".join(nodes)

    @field_validator("retry_wait")
    @classmethod
    def _duration(cls, value: str) -> str:
        value = value.strip()
        if not _DURATION.match(value):
            raise ValueError("a duration such as 10s, 500ms or 1m")
        return value

    @field_validator("index")
    @classmethod
    def _index(cls, value: str) -> str:
        value = value.strip()
        literal = _PLACEHOLDER.sub("", value)
        if not value or value in (".", ".."):
            raise ValueError("an index name is required")
        if literal != literal.lower():
            raise ValueError("index names must be lowercase")
        if _FORBIDDEN.search(literal):
            raise ValueError('an index name cannot contain \\ / * ? " < > | space , # :')
        if literal[:1] in ("-", "_", "+"):
            raise ValueError("an index name cannot start with -, _ or +")
        if "{" in literal or "}" in literal:
            raise ValueError("unbalanced { } in the index name; write placeholders as {column} or {column|yyyy.MM.dd}")
        return value

    @field_validator("mode", mode="after")
    @classmethod
    def _overwrite_needs_a_fixed_index(cls, value: str, info: ValidationInfo) -> str:
        # The connector's overwrite empties the resource named, which for a pattern is nothing:
        # the rows would just be added. Say so rather than let "overwrite" duplicate documents.
        if value == "overwrite" and _PLACEHOLDER.search(info.data.get("index") or ""):
            raise ValueError("overwrite needs a fixed index name, not a pattern; use append or upsert with an id_column")
        return value

    @field_validator("id_column", mode="after")
    @classmethod
    def _id_column_with_upsert(cls, value: str | None, info: ValidationInfo) -> str | None:
        if info.data.get("mode", "append") == "upsert" and not value:
            raise ValueError("required for mode 'upsert'")
        return value

    # -- derived ---------------------------------------------------------------------------

    @property
    def is_pattern(self) -> bool:
        return bool(_PLACEHOLDER.search(self.index))

    def base_url(self) -> str:
        """http(s)://host:port of the first node, for the design-time checks."""
        node = self.nodes.split(",")[0]
        if "://" not in node:
            node = ("https://" if self.ssl else "http://") + node
        scheme, rest = node.split("://", 1)
        if ":" not in rest.rsplit("]", 1)[-1]:  # no port (IPv6 literals aside)
            rest = f"{rest}:{self.port}"
        return f"{scheme}://{rest}"

    def auth_header(self) -> str | None:
        if self.api_key:
            return f"ApiKey {self.api_key}"
        if self.user:
            return "Basic " + base64.b64encode(f"{self.user}:{self.password or ''}".encode()).decode()
        return None

    def index_columns(self) -> list[str]:
        """Top-level columns the index pattern reads (``{a.b|fmt}`` reads ``a``)."""
        return [m.group(1).strip().split(".")[0] for m in _PLACEHOLDER.finditer(self.index)]

    def connector_options(self) -> dict[str, str]:
        """The ``es.*`` settings for the connector, the user's write_options last."""
        options: dict[str, str] = {
            "es.nodes": self.nodes,
            "es.port": str(self.port),
            "es.nodes.wan.only": str(self.wan_only).lower(),
            "es.net.ssl": str(self.ssl).lower(),
            "es.index.auto.create": str(self.auto_create_index).lower(),
            "es.batch.size.entries": str(self.batch_size_entries),
            "es.batch.write.retry.count": str(self.retry_count),
            "es.batch.write.retry.wait": self.retry_wait,
            "es.write.operation": _OPERATIONS[self.mode][1],
        }
        if self.rejected_documents == "skip":
            options["es.write.rest.error.handlers"] = "log"
            options["es.write.rest.error.handler.log.logger.name"] = REJECTED_LOGGER
            options["es.write.rest.error.handler.log.logger.level"] = "WARN"
        if self.ssl and self.ssl_allow_self_signed:
            options["es.net.ssl.cert.allow.self.signed"] = "true"
        if self.api_key:
            options["es.net.http.header.Authorization"] = f"ApiKey {self.api_key}"
        elif self.user:
            options["es.net.http.auth.user"] = self.user
            options["es.net.http.auth.pass"] = self.password or ""
        if self.id_column:
            options["es.mapping.id"] = self.id_column
        for key, value in spark_options(self.write_options).items():
            options[key if key.startswith("es.") else f"es.{key}"] = value
        return options


class ElasticsearchSink(Sink):
    """Elasticsearch indices: one document per row, by id or with generated ids."""

    type_name = "elasticsearch"
    display_name = "Elasticsearch"
    icon = "search"
    Options = ElasticsearchSinkOptions
    spark_packages = (ELASTICSEARCH_PACKAGE,)
    spark_conf = {EXCLUDES_KEY: ",".join(ELASTICSEARCH_EXCLUDES)}

    def describe(self, opts: ElasticsearchSinkOptions) -> str:
        return f"elasticsearch -> {opts.index} at {opts.nodes} ({opts.mode})"

    def validate_input(self, ctx, schema: StructType, opts: ElasticsearchSinkOptions) -> list[tuple[str, str]]:
        columns = [f.name for f in schema.fields]
        issues: list[tuple[str, str]] = []
        if opts.id_column and opts.id_column not in columns:
            issues.append(("id_column", f"not in the data: {opts.id_column} (columns: {', '.join(columns)})"))
        missing = [c for c in opts.index_columns() if c not in columns]
        if missing:
            issues.append(("index", f"refers to column(s) not in the data: {', '.join(missing)} (columns: {', '.join(columns)})"))
        unsupported = [f"{f.name} ({f.dataType.simpleString()})" for f in schema.fields if not supported(f.dataType)]
        if unsupported:
            issues.append((
                "",
                f"the connector cannot send {', '.join(unsupported)}: cast decimals to double or string and "
                "timestamp_ntz to timestamp in the SQL upstream",
            ))
        if not opts.is_pattern:
            issues += target_issues(schema, opts, describe_target(opts))
        return issues

    # -- write -----------------------------------------------------------------------------

    def write(self, ctx, df: DataFrame, opts: ElasticsearchSinkOptions) -> None:
        if opts.timestamp_format == "iso":
            df = with_iso_timestamps(df)
        save_mode = _OPERATIONS[opts.mode][0]
        df.write.format(FORMAT).options(**opts.connector_options()).mode(save_mode).save(opts.index)


def supported(data_type: DataType) -> bool:
    """Whether the connector can serialise a column of this type (nested types included)."""
    if isinstance(data_type, (DecimalType, TimestampNTZType)):
        return False
    if isinstance(data_type, StructType):
        return all(supported(f.dataType) for f in data_type.fields)
    element = getattr(data_type, "elementType", None)
    if element is not None:  # arrays
        return supported(element)
    if hasattr(data_type, "valueType"):  # maps
        return supported(data_type.keyType) and supported(data_type.valueType)
    return True


def with_iso_timestamps(df: DataFrame) -> DataFrame:
    """Top-level TIMESTAMP columns as ISO-8601 text (with the session time zone's offset)
    and DATE columns as yyyy-MM-dd: both are dates to Elasticsearch's dynamic mapping,
    whereas the connector's own epoch milliseconds would be mapped as long."""
    columns = []
    for f in df.schema.fields:
        column = F.col(f"`{f.name}`")
        if isinstance(f.dataType, TimestampType):
            column = F.date_format(column, ISO_TIMESTAMP).alias(f.name)
        elif isinstance(f.dataType, DateType):
            column = column.cast("string").alias(f.name)
        columns.append(column)
    return df.select(*columns)


# -- design time: what the cluster says about the target ------------------------------------

# Elasticsearch field types by what they accept; anything else (ip, geo_point, binary, ...) is not checked.
_ES_KINDS = {
    **{t: "object" for t in ("object", "nested")},
    **{t: "number" for t in ("long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long")},
    **{t: "date" for t in ("date", "date_nanos")},
    **{t: "text" for t in ("text", "keyword", "constant_keyword", "wildcard", "match_only_text", "search_as_you_type")},
    "boolean": "boolean",
}


def cluster_request(opts: ElasticsearchSinkOptions, method: str, path: str, timeout: float = 3.0) -> tuple[int, Any]:
    """One request to the first node with the sink's credentials: (status, parsed body)."""
    request = urllib.request.Request(opts.base_url() + path, method=method, headers={"Accept": "application/json"})
    auth = opts.auth_header()
    if auth:
        request.add_header("Authorization", auth)
    context = None
    if opts.ssl and opts.ssl_allow_self_signed:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read()
        return exc.code, json.loads(body) if body else None


def describe_target(opts: ElasticsearchSinkOptions) -> dict[str, Any] | None:
    """What ``index`` resolves to: ``{"indices": {name: {"mappings": ...}}, "write_index": name | None}``
    for an index or an alias, ``{"indices": {}}`` when there is nothing by that name, and None
    when the cluster cannot be asked from here (unreachable, wrong credentials, ...)."""
    try:
        status, body = cluster_request(opts, "GET", f"/{opts.index}/_mapping")
        if status == 404:
            return {"indices": {}, "write_index": None}
        if status != 200 or not isinstance(body, dict):
            return None
        indices = {name: {"mappings": entry.get("mappings", {})} for name, entry in body.items()}
        status, aliases = cluster_request(opts, "GET", f"/_alias/{opts.index}")
        if status == 200 and isinstance(aliases, dict) and aliases:
            writers = [name for name, entry in aliases.items() if entry.get("aliases", {}).get(opts.index, {}).get("is_write_index")]
            write_index = writers[0] if writers else (next(iter(aliases)) if len(aliases) == 1 else None)
            return {"indices": indices, "write_index": write_index, "alias": True}
        return {"indices": indices, "write_index": opts.index}
    except Exception:  # noqa: BLE001 - best effort, design time only
        return None


def target_issues(schema: StructType, opts: ElasticsearchSinkOptions, target: dict[str, Any] | None) -> list[tuple[str, str]]:
    if target is None:
        return []
    indices = target["indices"]
    if not indices:
        if not opts.auto_create_index:
            return [("index", f"'{opts.index}' does not exist and auto_create_index is off: create the index (for "
                               "rollover, the first index with the write alias) or turn auto_create_index on")]
        return []
    if target.get("alias") and target["write_index"] is None:
        return [("index", f"'{opts.index}' is an alias of {', '.join(sorted(indices))} and none of them is its write "
                          "index: writes are refused. Give one is_write_index: true, or write to an index")]
    name = target["write_index"] or next(iter(indices))
    return [("", message) for message in mapping_issues(schema, indices[name]["mappings"], opts, name)]


def mapping_issues(schema: StructType, mappings: dict[str, Any], opts: ElasticsearchSinkOptions, name: str) -> list[str]:
    """Elasticsearch's checks on each document, applied ahead of time to the schema: a value
    must fit its field's type (a field's type never changes, and numbers, text, dates and
    booleans are the kinds that refuse each other), and a ``dynamic: strict`` mapping
    refuses fields it does not know, while ``dynamic: false`` keeps them unsearchable."""
    properties: dict[str, Any] = mappings.get("properties") or {}
    issues: list[str] = []
    for f in schema.fields:
        if f.name not in properties:
            continue
        mapped = properties[f.name]
        es_type = mapped.get("type", "object" if "properties" in mapped else "")
        es_kind, data_kind = _ES_KINDS.get(es_type), _data_kind(f.dataType, opts)
        if es_kind is None or data_kind is None or es_kind == data_kind:
            continue
        if es_kind == "object" or data_kind == "object":
            conflict = True  # a value where a sub-document goes, or the other way round
        elif data_kind == "text" or es_kind == "text":
            conflict = False  # Elasticsearch parses text into numbers, booleans and dates when it can, and stores any value as text
        else:
            conflict = not (data_kind == "number" and es_kind == "date")  # epoch milliseconds are a date
        if conflict:
            issues.append(f"{f.name} is {f.dataType.simpleString()} in the data but {es_type} in index {name}")
    if issues:
        issues = ["; ".join(issues) + "; cast it in the SQL upstream or reindex with another mapping (Elasticsearch never changes a field's type)"]
    dynamic = str(mappings.get("dynamic", "true")).lower()
    new = [f.name for f in schema.fields if f.name not in properties]
    if new and dynamic == "strict":
        issues.append(f"index {name} has no field(s) {', '.join(new)} and refuses new ones (dynamic: strict): add them to the mapping or drop them upstream")
    elif new and dynamic == "false":
        issues.append(f"index {name} has no field(s) {', '.join(new)} and does not index new ones (dynamic: false): they would be stored but not searchable")
    return issues


def _data_kind(data_type: DataType, opts: ElasticsearchSinkOptions) -> str | None:
    """The kind of JSON value a column produces, in Elasticsearch's terms (None: not checked)."""
    if isinstance(data_type, ArrayType):  # every field is multi-valued in Elasticsearch
        return _data_kind(data_type.elementType, opts)
    if isinstance(data_type, (StructType, MapType)):
        return "object"
    if isinstance(data_type, BooleanType):
        return "boolean"
    if isinstance(data_type, NumericType):
        return "number"
    if isinstance(data_type, (TimestampType, DateType)):
        # As ISO text the value is a date to a date field and nothing to a numeric one; as
        # epoch milliseconds it is a number, which a date field accepts too.
        return "date" if opts.timestamp_format == "iso" else "number"
    if isinstance(data_type, (StringType, BinaryType)):
        return "text"
    return None
