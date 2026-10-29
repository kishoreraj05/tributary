"""What the MongoDB source and sink share: the connector package, URI handling and the
naming rules MongoDB enforces on databases and collections."""

from __future__ import annotations

import os
import re
from urllib.parse import quote

# 10.x is the connector line for Spark 3.x (Data Source V2). The default can be moved with
# TRIBUTARY_MONGODB_CONNECTOR_VERSION, for example to try a newer release.
CONNECTOR_VERSION = os.environ.get("TRIBUTARY_MONGODB_CONNECTOR_VERSION", "10.5.0")
CONNECTOR_PACKAGE = f"org.mongodb.spark:mongo-spark-connector_{{scala_version}}:{CONNECTOR_VERSION}"
FORMAT = "mongodb"

SCHEMES = ("mongodb://", "mongodb+srv://")
# MongoDB refuses a database name with these characters; collection names may have dots,
# but no "$" and cannot be empty or start with "system.".
_BAD_DATABASE_CHARS = re.compile(r'[/\\. "$*<>:|?]')

URI_DESCRIPTION = (
    "mongodb://host:27017 or mongodb+srv://cluster.example.net, with any client settings as query "
    "parameters (replicaSet, authSource, tls, retryWrites, ...). Credentials can be in the URI or in "
    "username/password"
)


def valid_uri(value: str) -> str:
    value = value.strip()
    if not value.startswith(SCHEMES):
        raise ValueError("must start with mongodb:// or mongodb+srv://")
    return value


def valid_database(value: str) -> str:
    value = value.strip()
    if not value or _BAD_DATABASE_CHARS.search(value):
        raise ValueError('must not be empty or contain any of / \\ . " $ * < > : | ? or spaces')
    return value


def valid_collection(value: str) -> str:
    value = value.strip()
    if not value or "$" in value or value.startswith("system."):
        raise ValueError("must not be empty, contain $ or start with system.")
    return value


def split_uri(uri: str) -> tuple[str, str, str]:
    """(scheme, authority, rest): ``mongodb://u:p@h1,h2:27017/db?x=y`` gives
    ``("mongodb", "u:p@h1,h2:27017", "/db?x=y")``."""
    scheme, remainder = uri.split("://", 1)
    cut = min((i for i in (remainder.find("/"), remainder.find("?")) if i >= 0), default=len(remainder))
    return scheme, remainder[:cut], remainder[cut:]


def effective_uri(uri: str, username: str | None, password: str | None) -> str:
    """The URI the connector gets: the given one, with username/password spliced in when
    they are set and the URI carries no credentials itself (those win)."""
    scheme, authority, rest = split_uri(uri)
    if not (username or password) or "@" in authority:
        return uri
    return f"{scheme}://{quote(username or '', safe='')}:{quote(password or '', safe='')}@{authority}{rest}"


def host_of(uri: str) -> str:
    """The hosts of the URI without credentials, path or settings: safe for logs."""
    scheme, authority, _ = split_uri(uri)
    return f"{scheme}://{authority.rsplit('@', 1)[-1]}"


def field_name_issue(name: str) -> str | None:
    """MongoDB field names cannot be empty, contain a dot (a path separator) or start with
    a dollar sign (an operator)."""
    if name == "":
        return "a column has an empty name"
    if "." in name:
        return f"column '{name}' has a dot in its name"
    if name.startswith("$"):
        return f"column '{name}' starts with $"
    return None

# One partition: a read that never asks the server for collection statistics (used at
# design time and in tests; a real run lets the connector pick its partitioner).
SINGLE_PARTITION = "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"

# How long design-time checks wait for a server before giving up (the driver's default of
# 30 s would freeze validation while a pipeline is drawn against an unreachable host).
DESIGN_TIME_TIMEOUT_MS = 3000


def design_time_uri(uri: str) -> str:
    """The URI with short timeouts unless the user set their own."""
    scheme, authority, rest = split_uri(uri)
    path, _, query = rest.partition("?")
    params = [p for p in query.split("&") if p]
    keys = {p.split("=", 1)[0].lower() for p in params}
    for key in ("serverSelectionTimeoutMS", "connectTimeoutMS"):
        if key.lower() not in keys:
            params.append(f"{key}={DESIGN_TIME_TIMEOUT_MS}")
    return f"{scheme}://{authority}{path or '/'}?{'&'.join(params)}"


# Java driver exceptions and server error codes that mean "try again": the server or the
# network was unavailable for a moment, or a replica set is electing a new primary.
_TRANSIENT_EXCEPTIONS = re.compile(
    r"com\.mongodb\.Mongo(?:Socket\w*|Timeout|NotPrimary|NodeIsRecovering|ServerUnavailable|Interrupted"
    r"|ConnectionPoolCleared|Incompatible)Exception"
)
_TRANSIENT_CODES = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436, 50}
_ERROR_CODE = re.compile(r"(?:Command failed with error|MongoCommandException: Command failed with error) (\d+)")
_TRANSIENT_TEXT = re.compile(r"Connection (?:refused|reset)|Broken pipe|Timed out after \d+ ms|Prematurely reached end of stream", re.IGNORECASE)


def is_transient(exc: BaseException) -> bool:
    """Whether a write failure looks like a transient connection problem. Py4J puts the
    Java stack trace, cause chain included, in the exception text."""
    seen: list[str] = []
    current: BaseException | None = exc
    while current is not None and current not in seen:
        seen.append(str(current))
        current = current.__cause__ or current.__context__
    text = "\n".join(seen)
    if _TRANSIENT_EXCEPTIONS.search(text) or _TRANSIENT_TEXT.search(text):
        return True
    return any(int(code) in _TRANSIENT_CODES for code in _ERROR_CODE.findall(text))


def type_family(data_type) -> str:
    """Spark types as BSON sees them: int32/int64 are both whole numbers, float/double are
    doubles, every decimal is Decimal128, and nested types compare by kind only."""
    name = data_type.simpleString()
    if name in ("tinyint", "smallint", "int", "bigint"):
        return "integer"
    if name in ("float", "double"):
        return "double"
    if name.startswith("decimal"):
        return "decimal"
    if name in ("timestamp", "timestamp_ntz", "date"):
        return "datetime"
    for kind in ("struct", "array", "map"):
        if name.startswith(kind):
            return kind
    return name
