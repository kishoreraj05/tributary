"""Tributary: a config-driven, plugin-based PySpark pipeline engine."""

from .api import analyze, compile_pipeline, export_schema, list_components, plan, preview, run
from .errors import PluginError, QualityFailure, SpecError, SpecIssue, TributaryError

__version__ = "0.1.0"

__all__ = [
    "analyze",
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
]
