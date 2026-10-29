from __future__ import annotations

import json
from typing import Any

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ...core import BatchReadable, ResolvedSchema, Source
from ...errors import TributaryError
from .._rest import HttpClient, Pagination, RestOptionsBase, parse_records
from ..schemas.basic import parse_ddl


class RestApiSourceOptions(RestOptionsBase):
    """A REST endpoint read on the driver, page by page, into a DataFrame of its records."""

    pagination: Pagination = Field(default_factory=Pagination)


class RestApiSource(Source, BatchReadable):
    """A REST API returning JSON: one table of its records, with pagination; or a dummy data stand-in."""

    type_name = "rest_api"
    display_name = "REST API"
    icon = "globe"
    Options = RestApiSourceOptions

    def read(self, ctx, opts: RestApiSourceOptions, schema):
        return self._frame(ctx, opts, schema, limit=None)

    def read_preview(self, ctx, opts: RestApiSourceOptions, schema, limit: int):
        # limit=0 is deep validation asking for the columns only: no request is made when
        # a schema or dummy records can answer that.
        return self._frame(ctx, opts, schema, limit=limit).limit(limit)

    def _frame(self, ctx, opts: RestApiSourceOptions, schema, limit: int | None) -> DataFrame:
        struct = record_struct(ctx.spark, schema, opts.parsing.record_schema)
        if opts.use_dummy_data or (limit == 0 and opts.dummy_data):
            records = opts.dummy_records()  # pretending: the API is never contacted
        elif limit == 0 and struct is not None:
            return ctx.spark.createDataFrame([], struct)
        else:
            records = fetch_records(opts, wanted=limit)
            ctx.log.info("rest_api: %d records from %s", len(records), opts.full_url)
        return records_frame(ctx.spark, records, struct)

    def estimate_input_bytes(self, opts: RestApiSourceOptions) -> int | None:
        return None


def record_struct(spark, schema: ResolvedSchema | None, ddl: str | None) -> StructType | None:
    """The declared record schema: the node's ``schema:`` block wins over ``parsing.recordSchema``."""
    if schema is not None and schema.struct is not None:
        return schema.struct
    if ddl:
        try:
            return parse_ddl(spark, ddl)
        except Exception as exc:  # noqa: BLE001 - a Spark parse error, reworded for the field
            raise TributaryError(f"parsing.recordSchema is not valid Spark DDL: {exc}") from None
    return None


def records_frame(spark, records: list[dict[str, Any]], struct: StructType | None) -> DataFrame:
    """Records as a DataFrame, parsed by Spark's JSON reader (with inference when no schema)."""
    if not records and struct is None:
        return spark.createDataFrame([], StructType([]))
    if struct is not None and not records:
        return spark.createDataFrame([], struct)
    lines = [json.dumps(r, default=str) for r in records]
    rdd = spark.sparkContext.parallelize(lines, max(1, min(len(lines) // 1000, 64)))
    return spark.read.json(rdd, schema=struct) if struct is not None else spark.read.json(rdd)


def fetch_records(opts: RestApiSourceOptions, wanted: int | None = None) -> list[dict[str, Any]]:
    """Calls the endpoint (following ``pagination``) until the records run out, ``maxPages``
    is reached, or ``wanted`` records are in hand (previews stop early)."""
    paging = opts.pagination
    parsing = opts.parsing_config()
    client = HttpClient(opts.http())
    records: list[dict[str, Any]] = []
    try:
        if paging.type == "none":
            return parse_records(client.fetch(opts.full_url, _params(opts), opts.endpoint.body), parsing, opts.full_url)
        cursor = paging.first
        for _ in range(paging.max_pages):
            params = _params(opts, {paging.page_param: cursor, **({paging.size_param: paging.page_size} if paging.size_param else {})})
            page = parse_records(client.fetch(opts.full_url, params, opts.endpoint.body), parsing, f"page {cursor}")
            records.extend(page)
            last = not page or (paging.size_param is not None and len(page) < paging.page_size)
            if last or (wanted is not None and len(records) >= wanted):
                break
            cursor += 1 if paging.type == "page" else paging.page_size
        return records
    finally:
        client.close()


def _params(opts: RestApiSourceOptions, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in opts.endpoint.query_params.items()}
    params.update(extra or {})
    return params
