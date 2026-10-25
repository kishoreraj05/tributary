"""Reading/writing pipeline documents and resolving ``${...}`` placeholders."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ..errors import SpecError, SpecIssue, TributaryError
from ..registry import Registry

_TOKEN = re.compile(r"\$\{([^}]+)\}")


def parse_text(text: str, fmt: str = "yaml") -> dict[str, Any]:
    data = json.loads(text) if fmt == "json" else yaml.safe_load(text)
    if not isinstance(data, dict):
        raise SpecError([SpecIssue("", "pipeline document must be a mapping")])
    return data


def load_document(source: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(source, Mapping):
        return copy.deepcopy(dict(source))
    path = Path(source)
    return parse_text(path.read_text(), "json" if path.suffix.lower() == ".json" else "yaml")


def dump_document(doc: Mapping[str, Any], fmt: str = "yaml") -> str:
    if fmt == "json":
        return json.dumps(doc, indent=2) + "\n"
    return yaml.safe_dump(dict(doc), sort_keys=False, allow_unicode=True)


class SecretResolver:
    """``${secret:path}`` uses the default provider; ``${secret:vault:path}`` picks one."""

    def __init__(self, registry: Registry, default: str = "env"):
        self._registry = registry
        self._default = default
        self._instances: dict[str, Any] = {}

    def _provider(self, name: str) -> Any:
        if name not in self._instances:
            self._instances[name] = self._registry.create("secret_provider", name)
        return self._instances[name]

    def get(self, ref: str) -> str:
        name, sep, rest = ref.partition(":")
        if sep and name in self._registry.plugins("secret_provider"):
            return self._provider(name).get(rest)
        return self._provider(self._default).get(ref)


class Interpolator:
    """Replaces ``${env.NAME}`` with environment variables and ``${secret:...}`` with secrets.

    A value that is exactly one placeholder keeps the variable's type (number, list, ...).
    With ``secrets=None`` secret placeholders are left as-is (validation without secrets).
    """

    def __init__(self, variables: Mapping[str, Any], secrets: SecretResolver | None, issues: list[SpecIssue]):
        self.variables = variables
        self.secrets = secrets
        self.issues = issues

    def resolve(self, value: Any, path: str) -> Any:
        if isinstance(value, dict):
            return {k: self.resolve(v, f"{path}.{k}" if path else str(k)) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(v, f"{path}[{i}]") for i, v in enumerate(value)]
        if isinstance(value, str):
            whole = _TOKEN.fullmatch(value)
            if whole:
                return self._lookup(whole.group(1).strip(), value, path)
            return _TOKEN.sub(lambda m: str(self._lookup(m.group(1).strip(), m.group(0), path)), value)
        return value

    def _lookup(self, expr: str, original: str, path: str) -> Any:
        if expr.startswith("env."):
            name = expr[4:]
            if name in self.variables:
                return self.variables[name]
            self.issues.append(SpecIssue(path, f"undefined variable '{name}' (define it under environments.<env>.vars)"))
            return original
        if expr.startswith("secret:"):
            if self.secrets is None:
                return original
            try:
                return self.secrets.get(expr[len("secret:"):])
            except TributaryError as exc:
                self.issues.append(SpecIssue(path, str(exc)))
                return original
        self.issues.append(SpecIssue(path, f"unknown placeholder '${{{expr}}}' (use ${{env.NAME}} or ${{secret:PATH}})"))
        return original
