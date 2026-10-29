"""Tributary: a config-driven, plugin-based PySpark pipeline engine."""

from .api import analyze, compile_pipeline, explain, export_schema, list_components, plan, preview, run
from .errors import PluginError, QualityFailure, SpecError, SpecIssue, TributaryError, short_error
from .plugins._dummy import LOCAL_ENV

__version__ = "0.1.0"

__all__ = [
    "LOCAL_ENV",
    "analyze",
    "explain",
    "compile_pipeline",
    "export_schema",
    "list_components",
    "plan",
    "preview",
    "run",
    "PluginError",
    "QualityFailure",
    "SpecError",
    "SpecIssue",
    "TributaryError",
    "short_error",
]
