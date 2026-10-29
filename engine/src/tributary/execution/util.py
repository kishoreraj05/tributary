from __future__ import annotations

import base64
import datetime as dt
import decimal
from pathlib import Path
from typing import Any

from ..core import ResolvedSchema, RunContext
from ..errors import TributaryError


def resolve_source_schema(ctx: RunContext, node: Any) -> ResolvedSchema | None:
    spec = getattr(node, "schema_", None)
    if spec is None:
        return None
    return ctx.registry.create("schema", spec.kind).resolve(ctx, spec, node.options)


def read_sample(spark, path: str):
    """Sample data uploaded by a user, standing in for a source during preview."""
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    if suffix in (".json", ".jsonl", ".ndjson"):
        return spark.read.json(path)
    if suffix == ".parquet":
        return spark.read.parquet(path)
    raise TributaryError(f"unsupported sample file '{path}' (use .csv, .json/.jsonl or .parquet)")


# JSON numbers are doubles to a browser: integers past 2**53 (Iceberg snapshot ids, for one)
# would be rounded in the UI, so previews carry them as strings of digits.
_EXACT_INT = 2**53


def to_jsonable(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and abs(value) > _EXACT_INT:
        return str(value)
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode()
    return value
