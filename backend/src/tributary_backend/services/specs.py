"""Turning stored pipeline drafts/versions into documents the engine can compile."""

from __future__ import annotations

import copy
import re
from collections.abc import Iterable
from typing import Any

from tributary.errors import SpecError, SpecIssue

from ..crypto import decrypt
from ..models import Connection

MASK = "********"
_SECRET_HINTS = ("password", "passwd", "secret", "token", "credential", "api_key", "access_key", "private_key")
_NODE_PATH = re.compile(r"^(sources|transforms|sinks|quality)\[(\d+)\]")


def looks_secret(field: str) -> bool:
    name = field.lower()
    return any(hint in name for hint in _SECRET_HINTS)


def referenced_connections(spec: dict[str, Any]) -> list[str]:
    names: list[str] = []

    def add(node: Any) -> None:
        if isinstance(node, dict) and isinstance(node.get("connection"), str) and node["connection"] not in names:
            names.append(node["connection"])

    for key in ("sources", "transforms", "sinks"):
        for node in spec.get(key) or []:
            add(node)
            options = node.get("options") if isinstance(node, dict) else None
            if isinstance(options, dict):
                # Targets embedded in the node's options (a deequ node's results_sinks).
                for value in options.values():
                    for target in value if isinstance(value, list) else ():
                        add(target)
    for block in spec.get("quality") or []:
        if isinstance(block, dict):
            for target in block.get("results_sinks") or []:
                add(target)
            add(block.get("quarantine_sink"))
    return names


def choose_env(spec: dict[str, Any], env: str | None) -> str:
    if env:
        return env
    envs = spec.get("environments") or {}
    if isinstance(envs, dict) and len(envs) == 1:
        return next(iter(envs))
    return "dev" if isinstance(envs, dict) and "dev" in envs else "default"


def materialize(
    spec: dict[str, Any], env: str | None, connections: Iterable[Connection], *, reveal: bool
) -> tuple[dict[str, Any], str]:
    """Copies the referenced DB connections for ``env`` into the document.

    Connections defined inline in the spec win over stored ones. Secret fields are
    decrypted only with ``reveal`` (previews and the run worker); otherwise they are
    masked, which is enough for validation and safe to store as a run snapshot.
    """
    doc = copy.deepcopy(spec)
    env_name = choose_env(doc, env)
    by_name = {c.name: c for c in connections}
    environments = doc.get("environments")
    if not isinstance(environments, dict):
        environments = doc["environments"] = {}
    block = environments.get(env_name)
    if not isinstance(block, dict):
        block = environments[env_name] = {}
    inline = block.setdefault("connections", {})

    issues = []
    for name in referenced_connections(doc):
        if name in inline or name not in by_name:
            continue  # inline wins; unknown names are reported by the engine
        conn = by_name[name]
        settings = (conn.settings or {}).get(env_name)
        secrets = (conn.secrets or {}).get(env_name) or {}
        if settings is None and not secrets:
            defined = ", ".join(sorted(set(conn.settings or {}) | set(conn.secrets or {}))) or "none"
            issues.append(SpecIssue("connections", f"connection '{name}' has no settings for environment '{env_name}' (has: {defined})"))
            continue
        values = {k: (decrypt(v) if reveal else MASK) for k, v in secrets.items()}
        inline[name] = {"type": conn.type, **(settings or {}), **values}
    if issues:
        raise SpecError(issues)
    return doc, env_name


def issue_dict(issue: SpecIssue, spec: dict[str, Any]) -> dict[str, Any]:
    """Adds the id of the node an issue belongs to, so the UI can badge it on the canvas."""
    node_id = None
    match = _NODE_PATH.match(issue.path)
    if match:
        section, index = match.group(1), int(match.group(2))
        nodes = spec.get(section) or []
        if index < len(nodes) and isinstance(nodes[index], dict):
            node_id = nodes[index].get("id")
    return {"path": issue.path, "message": issue.message, "node_id": node_id}


def spec_error_body(exc: SpecError, spec: dict[str, Any]) -> dict[str, Any]:
    return {"message": "invalid pipeline spec", "issues": [issue_dict(i, spec) for i in exc.issues]}
