"""Dummy data for the local environment: records that stand in for a source, parsed from
JSON Lines (what the editor uploads) or a list (what YAML gives)."""

from __future__ import annotations

import json
from typing import Any

from ..errors import TributaryError

LOCAL_ENV = "local"
MAX_DUMMY_RECORDS = 1000


def parse_records(value: Any) -> list[dict[str, Any]]:
    """JSON Lines text (one object per line, blank lines ignored), a JSON array as text, or
    a list of objects -> the records. Raises ValueError with the offending line number."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"not a JSON array: {exc.msg} (line {exc.lineno})") from None
        else:
            value = []
            for number, line in enumerate(text.splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    value.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(f"line {number} is not JSON: {exc.msg}") from None
    if not isinstance(value, list):
        raise ValueError("expected JSON Lines (one JSON object per line) or a list of records")
    for position, record in enumerate(value, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"record {position} is not a JSON object")
    if len(value) > MAX_DUMMY_RECORDS:
        raise ValueError(f"at most {MAX_DUMMY_RECORDS} records ({len(value)} given)")
    return value


def dummy_frame(spark, records: list[dict[str, Any]], struct=None):
    """The records as a DataFrame: typed by the node's declared schema, or inferred the way
    Spark reads JSON (nested objects become structs, ISO timestamps stay strings)."""
    lines = [json.dumps(record, default=str) for record in records]
    if struct is not None:
        if not lines:
            return spark.createDataFrame([], struct)
        return spark.read.schema(struct).json(spark.sparkContext.parallelize(lines, 1))
    if not lines:
        raise TributaryError("no dummy records and no schema declared: nothing to infer the columns from")
    return spark.read.json(spark.sparkContext.parallelize(lines, 1))
