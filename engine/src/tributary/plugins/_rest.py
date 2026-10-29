"""What the REST API source and the REST API enrichment transform share.

The options vocabulary (endpoint, parsing, concurrency, dummy data), the HTTP client with
its retries, keep-alive pool and token-bucket pacing, the response parser (response type,
record extraction path, explode) and the ``${column}`` / ``{param}`` templating used to
build a request from a row.

Option keys are camelCase, as in the REST source specification (``responseType``,
``maxConcurrentCalls``); the snake_case spelling is accepted as well.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import logging
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Literal

import requests
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from pydantic.fields import FieldInfo
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from ..core import PluginOptions
from ..errors import TributaryError
from ._util import OptionValue, when

log = logging.getLogger("tributary.rest")

ResponseType = Literal["JSONArray", "JSONObject", "JSONLines"]
Method = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
AuthType = Literal["none", "bearer", "basic", "api_key"]

# Placeholders a plugin fills in at runtime from a row: ${column} in body templates.
# (Compile-time ones, ${env.X} and ${secret:X}, were resolved before the options got here.)
COLUMN_PLACEHOLDER = re.compile(r"\$\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}")
# URL path variables: {roll_no} in https://api/students/{roll_no}/grades.
PATH_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

_RETRY_STATUSES = (429, 500, 502, 503, 504)
# The dummy dump is typed into a form and shipped inside the spec: keep it a sample, not a dataset.
MAX_DUMMY_RECORDS = 100


class RestApiError(TributaryError):
    """A request failed for good (after the retries) or the response could not be parsed."""


def _title(name: str, info: FieldInfo) -> str:
    """'recordExtractionPath' -> 'Record extraction path' (what the UI form shows as label)."""
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", info.alias or name).replace("_", " ").split()
    return " ".join([words[0].capitalize(), *(w.lower() for w in words[1:])]) if words else name


class Section(BaseModel):
    """A nested block of REST options."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, field_title_generator=_title)


# --- options ---------------------------------------------------------------------------


class Endpoint(Section):
    model_config = ConfigDict(title="Endpoint")

    url: str = Field(
        ...,
        description="Absolute URL, or a path under baseUrl. {name} placeholders are filled from pathParams",
    )
    method: Method = "GET"
    headers: dict[str, str] = Field(default_factory=dict, description="Static headers; values may use ${secret:...}")
    query_params: dict[str, OptionValue] = Field(
        default_factory=dict, alias="queryParams", description="Static query string parameters"
    )
    body: Any = Field(
        None, description="Static JSON body sent with POST/PUT/PATCH", json_schema_extra={"format": "json"}
    )
    timeout_seconds: float = Field(30, alias="timeoutSeconds", gt=0)
    max_retries: int = Field(
        3, alias="maxRetries", ge=0, description="Retries on 429 and 5xx responses and on connection errors"
    )
    retry_backoff_seconds: float = Field(
        1.0, alias="retryBackoffSeconds", ge=0, description="Exponential backoff base between retries"
    )
    verify_ssl: bool = Field(True, alias="verifySsl")


class Parsing(Section):
    model_config = ConfigDict(title="Response parsing")

    response_type: ResponseType = Field(
        "JSONArray",
        alias="responseType",
        description="JSONArray: [{...}, {...}]; JSONObject: {data: [...], status: 200}; JSONLines: one JSON object per line",
    )
    record_extraction_path: str | None = Field(
        None,
        alias="recordExtractionPath",
        description="JSONPath to the records inside the response, e.g. $.data.semesters ($.a.b, $['a'], $[0] and $[*] are supported)",
    )
    explode_array: bool = Field(
        True,
        alias="explodeArray",
        description="One row per element of the extracted array (Spark explode); off: one row per response",
    )
    record_schema: str | None = Field(
        None,
        alias="recordSchema",
        description="Spark DDL of one record, e.g. 'id INT, name STRING'. Inferred from the responses when omitted",
    )


class Pagination(Section):
    model_config = ConfigDict(title="Pagination")

    type: Literal["none", "page", "offset"] = Field(
        "none", description="page: a page number parameter (1, 2, ...); offset: a record offset parameter (0, 100, ...)"
    )
    page_param: str = Field(
        "page", alias="pageParam", description="Query parameter carrying the page number or the offset",
        json_schema_extra=when("type", ("page", "offset")),
    )
    size_param: str | None = Field(
        None, alias="sizeParam", description="Query parameter carrying the page size, if the API takes one",
        json_schema_extra=when("type", ("page", "offset")),
    )
    page_size: int = Field(
        100, alias="pageSize", ge=1, description="Records per page (the offset step; sent as sizeParam when set)",
        json_schema_extra=when("type", ("page", "offset")),
    )
    start: int | None = Field(
        None, description="First page number (default 1) or first offset (default 0)",
        json_schema_extra=when("type", ("page", "offset")),
    )
    max_pages: int = Field(
        1000, alias="maxPages", ge=1, description="Safety cap on the number of requests",
        json_schema_extra=when("type", ("page", "offset")),
    )

    @property
    def first(self) -> int:
        if self.start is not None:
            return self.start
        return 1 if self.type == "page" else 0


class ParameterMapping(Section):
    model_config = ConfigDict(title="Parameter mapping")

    path_params: dict[str, str] = Field(
        default_factory=dict, alias="pathParams", description="URL placeholder -> input column, e.g. roll_no: student_roll_num"
    )
    query_params: dict[str, str] = Field(
        default_factory=dict, alias="queryParams", description="Query parameter -> input column, e.g. student_id: student_roll_num"
    )
    body_template: Any = Field(
        None,
        alias="bodyTemplate",
        description='JSON body with ${column} placeholders, e.g. {"request_id": "${roll_num}", "action": "fetch"}; a value that is exactly one placeholder keeps the column\'s type',
        json_schema_extra={"format": "json"},
    )


class BulkMatch(Section):
    model_config = ConfigDict(title="Bulk response match")

    response_field: str = Field(..., alias="responseField", description="Field of a record holding the id it belongs to")
    input_column: str = Field(..., alias="inputColumn", description="Input column holding that id")


class DynamicInput(Section):
    model_config = ConfigDict(title="Dynamic input")

    input_strategy: Literal["DataFrameDriven"] = Field(
        "DataFrameDriven", alias="inputStrategy", description="The rows of the input drive the API calls (enrichment)"
    )
    input_dataframe_ref: str | None = Field(
        None,
        alias="inputDataFrameRef",
        description="Which input drives the calls (its table name); defaults to the node's only input",
    )
    parameter_mapping: ParameterMapping = Field(default_factory=ParameterMapping, alias="parameterMapping")
    bulk_request_support: bool = Field(
        False,
        alias="bulkRequestSupport",
        description="Send several rows per call: ${column} in the body template expands to the list of the batch's values (comma-separated in the URL)",
    )
    bulk_batch_size: int = Field(
        100, alias="bulkBatchSize", ge=1, description="Rows per call",
        json_schema_extra=when("bulkRequestSupport", (True,)),
    )
    bulk_response_match: BulkMatch | None = Field(
        None,
        alias="bulkResponseMatch",
        description="How records of a bulk response are matched back to input rows; without it, records are emitted with null input columns",
        json_schema_extra=when("bulkRequestSupport", (True,)),
    )
    include_input_columns: bool = Field(
        True, alias="includeInputColumns", description="Keep the driving row's columns next to the record columns"
    )
    response_column_prefix: str | None = Field(
        None, alias="responseColumnPrefix", description="Prefix for the record columns, e.g. when names clash with input columns"
    )
    on_error: Literal["fail", "null"] = Field(
        "fail",
        alias="onError",
        description="When a call still fails after the retries: fail the run, or keep the row with empty record columns",
    )


class Concurrency(Section):
    model_config = ConfigDict(title="Concurrency")

    max_concurrent_calls: int = Field(
        10,
        alias="maxConcurrentCalls",
        ge=1,
        description="Spark tasks calling the API at once, cluster-wide: the input is repartitioned to this many",
    )
    rate_limit_per_executor: float | None = Field(
        None,
        alias="rateLimitPerExecutor",
        gt=0,
        description="Calls per second each task may make (token bucket), so at most maxConcurrentCalls x this cluster-wide",
    )
    connection_pool_size: int = Field(
        2, alias="connectionPoolSize", ge=1, description="Keep-alive HTTP connections per task"
    )


class RestOptionsBase(PluginOptions):
    """Fields both REST plugins have. ``authType``/``token``/... are flat so a ``rest_api``
    connection can provide them per environment (the backend encrypts token and password).

    ``useDummyData`` turns the node into a stand-in: it never contacts the API, the
    ``dummyData`` dump is what it "returns", so the endpoint is not even needed."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, field_title_generator=_title)

    use_dummy_data: bool = Field(
        False,
        alias="useDummyData",
        validation_alias=AliasChoices("useDummyData", "use_dummy_data", "dummyDataFlag", "dummy_data_flag"),
        description="Pretend to be the API: never connect to it and hand dummyData to the downstream nodes instead",
    )
    dummy_data: list[Any] = Field(
        default_factory=list,
        validate_default=True,
        alias="dummyData",
        description=f"JSON array of up to {MAX_DUMMY_RECORDS} records, as the API would return them after parsing. With useDummyData off it still tells deep validation the columns when no schema is declared",
        json_schema_extra={"format": "json", "maxItems": MAX_DUMMY_RECORDS, **when("useDummyData", required=(True,))},
    )
    endpoint: Endpoint | None = Field(
        None, validate_default=True, json_schema_extra=when("useDummyData", required=(False,))
    )
    parsing: Parsing = Field(default_factory=Parsing)
    base_url: str | None = Field(
        None, alias="baseUrl", description="Prefix for a relative endpoint.url; typically from a connection"
    )
    auth_type: AuthType = Field("none", alias="authType")
    token: str | None = Field(
        None,
        validate_default=True,
        description="Bearer token or API key; use ${secret:...}",
        json_schema_extra=when("authType", ("bearer", "api_key"), required=("bearer", "api_key")),
    )
    username: str | None = Field(
        None, validate_default=True, json_schema_extra=when("authType", ("basic",), required=("basic",))
    )
    password: str | None = Field(
        None,
        validate_default=True,
        description="Use ${secret:...}",
        json_schema_extra=when("authType", ("basic",), required=("basic",)),
    )
    api_key_header: str = Field(
        "X-API-Key", alias="apiKeyHeader", description="Header carrying the API key",
        json_schema_extra=when("authType", ("api_key",)),
    )

    @field_validator("dummy_data", mode="after")
    @classmethod
    def _dummy_when_used(cls, value: list[Any], info: ValidationInfo) -> list[Any]:
        if info.data.get("use_dummy_data") and not value:
            raise ValueError("required when useDummyData is enabled (a JSON array of records)")
        if len(value) > MAX_DUMMY_RECORDS:
            raise ValueError(f"at most {MAX_DUMMY_RECORDS} dummy records ({len(value)} given)")
        return value

    @field_validator("endpoint", mode="after")
    @classmethod
    def _endpoint_unless_dummy(cls, value: Endpoint | None, info: ValidationInfo) -> Endpoint | None:
        if value is None and not info.data.get("use_dummy_data"):
            raise ValueError("required unless useDummyData is enabled")
        return value

    @field_validator("token", "username", "password", mode="after")
    @classmethod
    def _auth_fields(cls, value: str | None, info: ValidationInfo) -> str | None:
        needed = {"token": ("bearer", "api_key"), "username": ("basic",), "password": ("basic",)}[info.field_name]
        if not value and info.data.get("auth_type") in needed:
            raise ValueError(f"required for authType '{info.data.get('auth_type')}'")
        return value

    @property
    def full_url(self) -> str:
        if self.endpoint is None:  # pretending: nothing is ever called
            return ""
        url = self.endpoint.url
        if self.base_url and "://" not in url:
            return self.base_url.rstrip("/") + "/" + url.lstrip("/")
        return url

    def http(self) -> HttpConfig:
        e = self.endpoint
        if e is None:
            return HttpConfig()
        headers = dict(e.headers)
        auth = None
        if self.auth_type == "bearer":
            headers.setdefault("Authorization", f"Bearer {self.token}")
        elif self.auth_type == "api_key":
            headers.setdefault(self.api_key_header, str(self.token))
        elif self.auth_type == "basic":
            auth = (self.username or "", self.password or "")
        return HttpConfig(
            method=e.method,
            headers=headers,
            timeout=e.timeout_seconds,
            retries=e.max_retries,
            backoff=e.retry_backoff_seconds,
            verify=e.verify_ssl,
            auth=auth,
        )

    def parsing_config(self) -> ParsingConfig:
        p = self.parsing
        return ParsingConfig(p.response_type, p.record_extraction_path, p.explode_array)

    def dummy_records(self) -> list[dict[str, Any]]:
        return normalise_records(self.dummy_data, explode=True)


# --- HTTP ------------------------------------------------------------------------------


@dataclass(frozen=True)
class HttpConfig:
    """Plain data: it travels to Spark tasks inside the partition function."""

    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30
    retries: int = 3
    backoff: float = 1.0
    verify: bool = True
    auth: tuple[str, str] | None = None
    pool_size: int = 2
    rate_limit: float | None = None  # calls per second; None = unpaced


class TokenBucket:
    """Paces calls to at most ``rate`` per second. The bucket holds a single token, so the
    limit holds for any one-second window rather than allowing an initial burst."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.next_allowed = time.monotonic()

    def acquire(self) -> None:
        now = time.monotonic()
        if now < self.next_allowed:
            time.sleep(self.next_allowed - now)
            now = time.monotonic()
        self.next_allowed = now + self.interval


class HttpClient:
    """One keep-alive session with retries; one per Spark task (or one on the driver)."""

    def __init__(self, cfg: HttpConfig):
        self.cfg = cfg
        self.session = requests.Session()
        retry = Retry(
            total=cfg.retries,
            connect=cfg.retries,
            read=cfg.retries,
            status=cfg.retries,
            backoff_factor=cfg.backoff,
            status_forcelist=_RETRY_STATUSES,
            allowed_methods=frozenset({cfg.method}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=cfg.pool_size, max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.bucket = TokenBucket(cfg.rate_limit) if cfg.rate_limit else None
        self.calls = 0

    def close(self) -> None:
        self.session.close()

    def fetch(self, url: str, params: dict[str, Any] | None = None, body: Any = None) -> str:
        """The response text of one request, or RestApiError."""
        if self.bucket:
            self.bucket.acquire()
        self.calls += 1
        kwargs: dict[str, Any] = {
            "params": params or None,
            "headers": self.cfg.headers,
            "timeout": self.cfg.timeout,
            "verify": self.cfg.verify,
            "auth": self.cfg.auth,
        }
        if body is not None:
            if isinstance(body, str):
                kwargs["data"] = body.encode()
                kwargs["headers"] = {"Content-Type": "application/json", **self.cfg.headers}
            else:
                kwargs["json"] = body
        try:
            response = self.session.request(self.cfg.method, url, **kwargs)
        except requests.RequestException as exc:
            raise RestApiError(f"{self.cfg.method} {url}: {exc}") from exc
        if not response.ok:
            snippet = response.text[:200].replace("\n", " ")
            raise RestApiError(f"{self.cfg.method} {url}: HTTP {response.status_code} {response.reason}: {snippet}")
        return response.text


# --- parsing ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsingConfig:
    response_type: str = "JSONArray"
    path: str | None = None
    explode: bool = True


def parse_records(text: str, parsing: ParsingConfig, where: str = "") -> list[dict[str, Any]]:
    """Response body -> records (JSON objects). Non-object records land under a ``value`` key."""
    try:
        if parsing.response_type == "JSONLines":
            root: Any = [json.loads(line) for line in text.splitlines() if line.strip()]
        else:
            root = json.loads(text) if text.strip() else None
    except json.JSONDecodeError as exc:
        raise RestApiError(f"{where or 'response'} is not {parsing.response_type}: {exc}") from None
    if parsing.response_type == "JSONArray" and root is not None and not isinstance(root, list):
        raise RestApiError(f"{where or 'response'} is a {_kind(root)}, not a JSON array; set responseType")
    if parsing.response_type == "JSONObject" and root is not None and not isinstance(root, dict):
        raise RestApiError(f"{where or 'response'} is a {_kind(root)}, not a JSON object; set responseType")
    node = jsonpath(root, parsing.path) if parsing.path else root
    return normalise_records(node, parsing.explode)


def normalise_records(node: Any, explode: bool) -> list[dict[str, Any]]:
    if node is None:
        return []
    items = (node if isinstance(node, list) else [node]) if explode else [node]
    return [item if isinstance(item, dict) else {"value": item} for item in items]


def _kind(value: Any) -> str:
    return {dict: "JSON object", list: "JSON array", str: "string"}.get(type(value), type(value).__name__)


_SEGMENT = re.compile(r"\.([A-Za-z_][A-Za-z0-9_\-]*|\*)|\[(\*|-?\d+|'[^']*'|\"[^\"]*\")\]")


def jsonpath(value: Any, path: str) -> Any:
    """A small JSONPath: ``$.a.b``, ``$['a-b']``, ``$[0]`` and ``$[*]`` / ``$.*`` projections.

    Without a wildcard the result is the single value at the path (None if absent); with
    one it is the list of matches."""
    text = path.strip()
    if text.startswith("$"):
        text = text[1:]
    nodes, projected, pos = [value], False, 0
    while pos < len(text):
        match = _SEGMENT.match(text, pos)
        if not match:
            raise RestApiError(
                f"unsupported JSONPath '{path}' near '{text[pos:pos + 12]}' (supported: $.a.b, $['a'], $[0], $[*])"
            )
        pos = match.end()
        name, index = match.group(1), match.group(2)
        key = index if name is None else name
        if key == "*":
            projected = True
            nodes = [c for n in nodes for c in (list(n.values()) if isinstance(n, dict) else n if isinstance(n, list) else [])]
        elif name is not None or key[0] in "'\"":
            key = key if name is not None else key[1:-1]
            nodes = [n[key] for n in nodes if isinstance(n, dict) and key in n]
        else:
            i = int(key)
            nodes = [n[i] for n in nodes if isinstance(n, list) and -len(n) <= i < len(n)]
    if projected:
        return nodes
    return nodes[0] if nodes else None


# --- templating ------------------------------------------------------------------------


def to_json_value(value: Any) -> Any:
    """Row values as JSON: datetimes in ISO format, decimals as strings, bytes as latin-1."""
    if isinstance(value, dict):
        return {k: to_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_value(v) for v in value]
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("latin-1")
    return value


def param_text(value: Any) -> str:
    """A row value as it goes into a URL path or query string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join(param_text(v) for v in value)
    value = to_json_value(value)
    return "" if value is None else value if isinstance(value, str) else json.dumps(value)


def render_template(template: Any, values: dict[str, Any], where: str = "bodyTemplate") -> Any:
    """Fills ``${column}`` placeholders. A string that is exactly one placeholder becomes the
    column's value with its type; inside longer strings values are spliced in as text."""

    def lookup(name: str) -> Any:
        if name not in values:
            known = ", ".join(values) or "none"
            raise RestApiError(f"{where} refers to column '{name}', which the input does not have (columns: {known})")
        return to_json_value(values[name])

    def text_of(value: Any) -> str:
        return value if isinstance(value, str) else "" if value is None else json.dumps(value)

    if isinstance(template, str):
        whole = COLUMN_PLACEHOLDER.fullmatch(template.strip())
        if whole:
            return lookup(whole.group(1))
        structured = _as_json_structure(template)
        if structured is not None:  # JSON written as text: fill it as a structure so types survive
            return render_template(structured, values, where)
        return COLUMN_PLACEHOLDER.sub(lambda m: text_of(lookup(m.group(1))), template)
    if isinstance(template, dict):
        return {k: render_template(v, values, where) for k, v in template.items()}
    if isinstance(template, list):
        return [render_template(v, values, where) for v in template]
    return template


def render_url(url: str, values: dict[str, Any], path_params: dict[str, str]) -> str:
    """Fills ``{name}`` path variables from ``pathParams`` (name -> column), or from a column
    of the same name when there is no mapping."""

    def fill(match: re.Match) -> str:
        name = match.group(1)
        column = path_params.get(name, name)
        if column not in values:
            known = ", ".join(values) or "none"
            raise RestApiError(f"URL placeholder {{{name}}} needs column '{column}', which the input does not have (columns: {known})")
        return urllib.parse.quote(param_text(values[column]), safe="")

    return PATH_PLACEHOLDER.sub(fill, url)


def template_columns(template: Any) -> set[str]:
    """Columns a body template refers to."""
    if isinstance(template, str):
        return set(COLUMN_PLACEHOLDER.findall(template))
    if isinstance(template, dict):
        return set().union(*(template_columns(v) for v in template.values())) if template else set()
    if isinstance(template, list):
        return set().union(*(template_columns(v) for v in template)) if template else set()
    return set()


def _as_json_structure(text: str) -> dict | list | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "{[":
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, (dict, list)) else None


def body_as_json(rendered: Any) -> Any:
    """A rendered string template that is JSON text is sent as JSON, else as raw text."""
    if isinstance(rendered, str):
        structured = _as_json_structure(rendered)
        return rendered if structured is None else structured
    return rendered
