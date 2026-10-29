"""Plugin discovery.

Plugins are found through Python entry points (group ``tributary.<category>s``), so
adding a connector never requires editing engine code: ship a package that declares
an entry point, or call ``register`` for an in-process plugin.
"""

from __future__ import annotations

import importlib
import inspect
import re
from importlib.metadata import entry_points
from typing import Any

from .core.plugin import CATEGORIES, Plugin
from .errors import PluginError

_NAME = re.compile(r"[a-z][a-z0-9_]*")


def group_for(category: str) -> str:
    return f"tributary.{category}s"


class Registry:
    def __init__(self, *, autoload: bool = True):
        self._plugins: dict[str, dict[str, type[Plugin]]] = {c: {} for c in CATEGORIES}
        self._autoload = autoload
        self._loaded = False
        # Bumped on every registration so cached spec models get rebuilt.
        self.version = 0

    def register(self, cls: type[Plugin]) -> type[Plugin]:
        category = getattr(cls, "category", None)
        name = getattr(cls, "type_name", None)
        if category not in self._plugins:
            raise PluginError(f"{cls.__qualname__}: unknown plugin category {category!r}")
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise PluginError(f"{cls.__qualname__}: type_name must match [a-z][a-z0-9_]*, got {name!r}")
        if inspect.isabstract(cls):
            missing = ", ".join(sorted(cls.__abstractmethods__))
            raise PluginError(f"{cls.__qualname__} does not implement: {missing}")
        existing = self._plugins[category].get(name)
        if existing is not None and existing is not cls:
            raise PluginError(
                f"{category} type '{name}' is already registered by {existing.__module__}.{existing.__qualname__}"
            )
        self._plugins[category][name] = cls
        self.version += 1
        return cls

    def ensure_loaded(self) -> None:
        if not self._autoload or self._loaded:
            return
        self._loaded = True
        for category in CATEGORIES:
            for ep in entry_points(group=group_for(category)):
                cls = ep.load()
                if getattr(cls, "type_name", None) != ep.name:
                    raise PluginError(
                        f"entry point '{ep.name}' in {group_for(category)} points to a plugin "
                        f"with type_name {getattr(cls, 'type_name', None)!r}"
                    )
                self.register(cls)
        # Built-ins are registered even if the installed metadata does not list them yet
        # (registering the same class twice is a no-op).
        from .plugins import BUILTIN

        for category, plugins in BUILTIN.items():
            for name, target in plugins.items():
                if name not in self._plugins[category]:
                    module, _, attr = target.partition(":")
                    self.register(getattr(importlib.import_module(module), attr))

    def plugins(self, category: str) -> dict[str, type[Plugin]]:
        self.ensure_loaded()
        return dict(self._plugins[category])

    def get(self, category: str, name: str) -> type[Plugin]:
        self.ensure_loaded()
        try:
            return self._plugins[category][name]
        except KeyError:
            available = ", ".join(sorted(self._plugins.get(category, {}))) or "none"
            raise PluginError(f"unknown {category} type '{name}' (available: {available})") from None

    def create(self, category: str, name: str) -> Any:
        return self.get(category, name)()

    def catalog(self) -> dict[str, list[dict[str, Any]]]:
        self.ensure_loaded()
        return {c: [p.metadata() for _, p in sorted(self._plugins[c].items()) if not p.hidden] for c in CATEGORIES}


_default: Registry | None = None


def default_registry() -> Registry:
    global _default
    if _default is None:
        _default = Registry()
    return _default


def register(cls: type[Plugin]) -> type[Plugin]:
    """Decorator registering an in-process plugin with the default registry."""
    return default_registry().register(cls)
