from __future__ import annotations

import base64
import json
import urllib.request
from typing import Literal
from urllib.parse import quote

from pydantic import Field

from ...core import PluginOptions, ResolvedSchema, SchemaResolver
from ...errors import TributaryError


class RegistrySchemaOptions(PluginOptions):
    url: str | None = Field(None, description="Schema Registry URL; defaults to the source's schema_registry_url")
    subject: str = Field(..., description="e.g. orders-value")
    version: str | int = "latest"
    format: Literal["avro"] = "avro"
    basic_auth: str | None = Field(None, description="user:password (use ${secret:...})")
    timeout_seconds: float = 10


class SchemaRegistrySchema(SchemaResolver):
    """Fetches the schema from a Confluent-compatible Schema Registry."""

    type_name = "registry"
    Options = RegistrySchemaOptions

    def resolve(self, ctx, opts: RegistrySchemaOptions, source_opts) -> ResolvedSchema:
        base = opts.url or getattr(source_opts, "schema_registry_url", None)
        if not base:
            raise TributaryError("schema kind 'registry' needs 'url' or the source's 'schema_registry_url'")
        url = f"{base.rstrip('/')}/subjects/{quote(opts.subject, safe='')}/versions/{opts.version}"
        request = urllib.request.Request(url, headers={"Accept": "application/vnd.schemaregistry.v1+json"})
        if opts.basic_auth:
            token = base64.b64encode(opts.basic_auth.encode()).decode()
            request.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(request, timeout=opts.timeout_seconds) as response:
                payload = json.load(response)
        except OSError as exc:
            raise TributaryError(f"could not fetch schema '{opts.subject}' from {base}: {exc}") from exc
        schema_type = payload.get("schemaType", "AVRO")
        if schema_type != "AVRO":
            raise TributaryError(f"schema type {schema_type} is not supported yet (only AVRO)")
        return ResolvedSchema(avro=payload["schema"], wire_format="confluent")
