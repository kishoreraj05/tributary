from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

# Plugin families. Each maps to the entry-point group "tributary.<category>s".
CATEGORIES: tuple[str, ...] = (
    "source",
    "transform",
    "sink",
    "check",
    "metrics_sink",
    "schema",
    "secret_provider",
)


class PluginOptions(BaseModel):
    """Base for every plugin's options model. Unknown keys are rejected so typos surface early."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Plugin(ABC):
    """Common metadata for all plugins.

    A plugin is a stateless class: it declares a ``type_name`` (the value users put in
    ``type:``), an ``Options`` model (validated config + JSON Schema for the UI) and
    implements the interface of its family (Source, Transform, Sink, ...).
    """

    category: ClassVar[str]
    type_name: ClassVar[str]
    Options: ClassVar[type[PluginOptions]] = PluginOptions
    display_name: ClassVar[str | None] = None
    description: ClassVar[str] = ""
    icon: ClassVar[str] = "box"
    # Maven coordinates; "{spark_version}" and "{scala_version}" are filled in at session build.
    spark_packages: ClassVar[tuple[str, ...]] = ()
    # Spark settings the plugin needs in every session it runs in (for example SQL extensions).
    # Static settings only: anything that depends on the options goes in ``required_conf``.
    spark_conf: ClassVar[dict[str, str]] = {}
    # Connection types (besides type_name itself) this plugin can take settings from; "*" = any.
    accepts_connections: ClassVar[tuple[str, ...]] = ()
    # Internal plugins (the local environment's dummy source and sink) are valid in specs but
    # are not offered in the component catalog: nodes become them, nobody picks them.
    hidden: ClassVar[bool] = False

    def required_packages(self, opts: Any) -> list[str]:
        return list(self.spark_packages)

    def required_conf(self, opts: Any) -> dict[str, str]:
        """Spark settings to apply when the session is built (catalogs, extensions, ...).

        The user's ``runtime.spark_conf`` still wins, except ``spark.sql.extensions``,
        where every contribution is kept."""
        return dict(self.spark_conf)

    @classmethod
    def capabilities(cls) -> list[str]:
        return []

    @classmethod
    def accepts_connection(cls, connection_type: str) -> bool:
        allowed = cls.accepts_connections
        return connection_type == cls.type_name or connection_type in allowed or "*" in allowed

    @classmethod
    def metadata(cls) -> dict[str, Any]:
        doc = (cls.__doc__ or "").strip()
        return {
            "type": cls.type_name,
            "category": cls.category,
            "display_name": cls.display_name or cls.type_name.replace("_", " ").title(),
            "description": cls.description or (doc.splitlines()[0] if doc else ""),
            "icon": cls.icon,
            "capabilities": cls.capabilities(),
            "connection_types": [cls.type_name, *cls.accepts_connections],
            "options_schema": cls.Options.model_json_schema(),
        }
