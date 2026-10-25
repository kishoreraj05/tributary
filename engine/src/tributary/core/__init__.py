"""Plugin interfaces. Nothing in here knows about concrete connectors."""

from .context import RunContext
from .metrics_sink import MetricsSink
from .plugin import CATEGORIES, Plugin, PluginOptions
from .quality import Check, CheckResult, DatasetCheck, RowCheck, RowCheckOptions
from .schema import ResolvedSchema, SchemaResolver
from .secrets import SecretProvider
from .sink import Sink
from .source import BatchReadable, Source, StreamReadable
from .transform import Transform

__all__ = [
    "CATEGORIES",
    "BatchReadable",
    "Check",
    "CheckResult",
    "DatasetCheck",
    "MetricsSink",
    "Plugin",
    "PluginOptions",
    "ResolvedSchema",
    "RowCheck",
    "RowCheckOptions",
    "RunContext",
    "SchemaResolver",
    "SecretProvider",
    "Sink",
    "Source",
    "StreamReadable",
    "Transform",
]
