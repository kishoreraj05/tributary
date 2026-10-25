from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from .context import RunContext


@dataclass(frozen=True)
class ResolvedSchema:
    """Schema information handed to a source. Sources use whichever form they understand."""

    struct: StructType | None = None
    avro: str | None = None
    # "confluent" = payload prefixed with the 5-byte Schema Registry header.
    wire_format: str = "plain"


class SchemaResolver(Plugin):
    """Turns a source's ``schema:`` block (discriminated by ``kind``) into a ResolvedSchema."""

    category = "schema"

    @abstractmethod
    def resolve(self, ctx: RunContext, opts: Any, source_opts: Any) -> ResolvedSchema: ...
