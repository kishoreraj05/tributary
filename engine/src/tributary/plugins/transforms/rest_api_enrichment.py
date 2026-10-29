"""REST API calls driven by a DataFrame: one call per input row (or per batch of rows), the
records of each response exploded next to the row that produced them.

Concurrency is physical: the input is repartitioned to ``maxConcurrentCalls`` partitions and
every partition is one Spark task that calls the API sequentially through one keep-alive
session, paced by a token bucket (``rateLimitPerExecutor``)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Iterator

from pydantic import Field
from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from ...core import Transform
from ...errors import TributaryError
from .._rest import (
    PATH_PLACEHOLDER,
    Concurrency,
    DynamicInput,
    HttpClient,
    HttpConfig,
    ParsingConfig,
    RestApiError,
    RestOptionsBase,
    body_as_json,
    param_text,
    parse_records,
    render_template,
    render_url,
    template_columns,
)
from ..sources.rest_api import record_struct, records_frame

_RECORDS = "_rest_records_"  # the response of a row, as a JSON array of records
_RECORD = "_rest_record_"


class RestApiEnrichmentOptions(RestOptionsBase):
    dynamic_input: DynamicInput = Field(..., alias="dynamicInput")
    concurrency: Concurrency = Field(default_factory=Concurrency)


@dataclass(frozen=True)
class _CallSpec:
    """Everything a task needs to turn rows into requests. Plain data, pickled with the task."""

    http: HttpConfig
    parsing: ParsingConfig
    url: str
    path_params: dict[str, str]
    query_params: dict[str, str]
    static_params: dict[str, Any]
    body_template: Any
    bulk: bool
    batch_size: int
    match: tuple[str, str] | None  # (response field, input column)
    fail_on_error: bool
    dummy: list[dict[str, Any]] | None
    columns: tuple[str, ...] = field(default_factory=tuple)


class RestApiEnrichment(Transform):
    """Calls a REST API for each row (or batch of rows) of its input and adds the response records."""

    type_name = "rest_api_enrichment"
    display_name = "REST API enrichment"
    icon = "globe"
    Options = RestApiEnrichmentOptions
    accepts_connections = ("rest_api",)

    def validate(self, opts: RestApiEnrichmentOptions, input_aliases: list[str]) -> list[str]:
        issues = []
        ref = opts.dynamic_input.input_dataframe_ref
        if ref is not None and ref not in input_aliases:
            issues.append(f"dynamicInput.inputDataFrameRef '{ref}' is not an input of this node (inputs: {', '.join(input_aliases) or 'none'})")
        elif ref is None and len(input_aliases) != 1:
            issues.append("connect exactly one input, or name the one that drives the calls in dynamicInput.inputDataFrameRef")
        mapping = opts.dynamic_input.parameter_mapping
        if opts.endpoint is not None and opts.endpoint.method in ("GET", "DELETE") and mapping.body_template is not None:
            issues.append(f"bodyTemplate is set but the method is {opts.endpoint.method}; use POST, PUT or PATCH")
        return issues

    def apply(self, ctx, inputs: dict[str, DataFrame], opts: RestApiEnrichmentOptions) -> DataFrame:
        ref = opts.dynamic_input.input_dataframe_ref
        driving = inputs[ref] if ref is not None else next(iter(inputs.values()))
        if ctx.preview:
            driving = driving.limit(ctx.preview_limit)  # a test of the node must not hit the API for every row
        columns = tuple(driving.columns)
        struct = record_struct(ctx.spark, None, opts.parsing.record_schema)
        self._check_columns(opts, columns)

        # Deep validation runs the plan on empty frames (preview with limit 0): there are
        # no rows to call the API with, so the columns must come from somewhere else.
        analysing = ctx.preview and ctx.preview_limit == 0
        if struct is None and (opts.use_dummy_data or (analysing and opts.dummy_data)):
            struct = records_frame(ctx.spark, opts.dummy_records(), None).schema
        if struct is None and analysing:
            raise TributaryError(
                "the columns of the API records are not known without calling the API: "
                "set parsing.recordSchema or give dummyData (deep validation uses it for the columns)"
            )

        spec = self._call_spec(opts, columns)
        partitions = opts.concurrency.max_concurrent_calls
        raw = driving.repartition(partitions).rdd.mapPartitions(lambda rows: _call_partition(rows, spec))
        if struct is None:
            # Inference reads the responses; cache them so the API is called once, not twice.
            raw = raw.persist(StorageLevel.MEMORY_AND_DISK)
            ctx.defer(raw.unpersist)
            struct = ctx.spark.read.json(raw.map(lambda t: t[-1])).schema
        # Bulk responses nobody claimed come back with null input columns, so every field is nullable.
        raw_schema = StructType(
            [StructField(f.name, f.dataType, True, f.metadata) for f in driving.schema.fields]
            + [StructField(_RECORDS, StringType())]
        )
        frame = ctx.spark.createDataFrame(raw, raw_schema)
        return self._assemble(frame, columns, struct, opts.dynamic_input)

    @staticmethod
    def _check_columns(opts: RestApiEnrichmentOptions, columns: tuple[str, ...]) -> None:
        mapping = opts.dynamic_input.parameter_mapping
        needed = {
            **{f"pathParams.{k}": v for k, v in mapping.path_params.items()},
            **{f"queryParams.{k}": v for k, v in mapping.query_params.items()},
            **{f"bodyTemplate ${{{c}}}": c for c in template_columns(mapping.body_template)},
            **{f"URL {{{n}}}": mapping.path_params.get(n, n) for n in PATH_PLACEHOLDER.findall(opts.full_url)},
        }
        if opts.dynamic_input.bulk_response_match:
            needed["bulkResponseMatch.inputColumn"] = opts.dynamic_input.bulk_response_match.input_column
        missing = sorted({f"{where} -> '{col}'" for where, col in needed.items() if col not in columns})
        if missing:
            raise TributaryError(
                f"the input has no column for {', '.join(missing)} (input columns: {', '.join(columns) or 'none'})"
            )

    @staticmethod
    def _call_spec(opts: RestApiEnrichmentOptions, columns: tuple[str, ...]) -> _CallSpec:
        dyn, conc = opts.dynamic_input, opts.concurrency
        http = replace(opts.http(), pool_size=conc.connection_pool_size, rate_limit=conc.rate_limit_per_executor)
        match = dyn.bulk_response_match
        return _CallSpec(
            http=http,
            parsing=opts.parsing_config(),
            url=opts.full_url,
            path_params=dict(dyn.parameter_mapping.path_params),
            query_params=dict(dyn.parameter_mapping.query_params),
            static_params={k: param_text(v) for k, v in (opts.endpoint.query_params if opts.endpoint else {}).items()},
            body_template=dyn.parameter_mapping.body_template,
            bulk=dyn.bulk_request_support,
            batch_size=dyn.bulk_batch_size if dyn.bulk_request_support else 1,
            match=(match.response_field, match.input_column) if (dyn.bulk_request_support and match) else None,
            fail_on_error=dyn.on_error == "fail",
            dummy=opts.dummy_records() if opts.use_dummy_data else None,
            columns=columns,
        )

    @staticmethod
    def _assemble(frame: DataFrame, columns: tuple[str, ...], struct: StructType, dyn: DynamicInput) -> DataFrame:
        prefix = dyn.response_column_prefix or ""
        kept = list(columns) if dyn.include_input_columns else []
        clashes = sorted({prefix + f.name for f in struct.fields} & set(kept))
        if clashes:
            raise TributaryError(
                f"record column(s) {', '.join(clashes)} clash with input columns; "
                "set dynamicInput.responseColumnPrefix or turn includeInputColumns off"
            )
        selected = [F.col(f"`{c}`") for c in kept]
        if struct.fields:
            frame = frame.withColumn(_RECORD, F.explode_outer(F.from_json(F.col(_RECORDS), ArrayType(struct))))
            selected += [F.col(f"`{_RECORD}`.`{f.name}`").alias(prefix + f.name) for f in struct.fields]
        return frame.select(*selected)


# --- inside the Spark task -------------------------------------------------------------


def _call_partition(rows: Iterable[Any], spec: _CallSpec) -> Iterator[tuple]:
    """One task: rows in, (row values..., JSON array of records) out, one HTTP session."""
    client = None if spec.dummy is not None else HttpClient(spec.http)
    log = logging.getLogger("tributary.rest")
    try:
        for batch in _batches(rows, spec.batch_size):
            if spec.dummy is not None:
                dummy = json.dumps(spec.dummy)
                for row in batch:
                    yield (*row, dummy)
            elif spec.bulk:
                yield from _bulk_call(client, spec, batch, log)
            else:
                for row in batch:
                    records = _guarded(client, spec, log, lambda: _single_call(client, spec, row))
                    yield (*row, json.dumps(records))
    finally:
        if client is not None:
            client.close()


def _batches(rows: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _values(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True)


def _guarded(client: HttpClient, spec: _CallSpec, log: logging.Logger, call) -> list[dict[str, Any]]:
    try:
        return call()
    except RestApiError as exc:
        if spec.fail_on_error:
            raise
        log.warning("rest_api_enrichment: %s (onError: null)", exc)
        return []


def _request(spec: _CallSpec, values: dict[str, Any]) -> tuple[str, dict[str, str], Any]:
    url = render_url(spec.url, values, spec.path_params)
    params = {**spec.static_params, **{p: param_text(values[c]) for p, c in spec.query_params.items()}}
    body = body_as_json(render_template(spec.body_template, values)) if spec.body_template is not None else None
    return url, params, body


def _single_call(client: HttpClient, spec: _CallSpec, row: Any) -> list[dict[str, Any]]:
    url, params, body = _request(spec, _values(row))
    return parse_records(client.fetch(url, params, body), spec.parsing, url)


def _bulk_call(client: HttpClient, spec: _CallSpec, batch: list[Any], log: logging.Logger) -> Iterator[tuple]:
    """One call for the batch; ${column} placeholders expand to the batch's values. Records
    go back to their row through ``bulkResponseMatch``; without it they are emitted with
    null input columns (the input rows are then not repeated)."""
    dicts = [_values(row) for row in batch]
    values = {c: [d.get(c) for d in dicts] for c in spec.columns}
    records = _guarded(client, spec, log, lambda: _bulk_records(client, spec, values))
    if spec.match is None:
        for record in records:
            yield (*([None] * len(spec.columns)), json.dumps([record]))
        return
    response_field, input_column = spec.match
    by_key: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_key.setdefault(param_text(record.get(response_field)), []).append(record)
    for row, d in zip(batch, dicts):
        yield (*row, json.dumps(by_key.pop(param_text(d.get(input_column)), [])))
    for leftover in by_key.values():  # records no row claimed
        yield (*([None] * len(spec.columns)), json.dumps(leftover))


def _bulk_records(client: HttpClient, spec: _CallSpec, values: dict[str, Any]) -> list[dict[str, Any]]:
    url, params, body = _request(spec, values)
    return parse_records(client.fetch(url, params, body), spec.parsing, url)
