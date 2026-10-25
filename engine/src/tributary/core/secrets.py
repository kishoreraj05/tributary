from __future__ import annotations

from abc import abstractmethod

from .plugin import Plugin


class SecretProvider(Plugin):
    """Resolves ``${secret:<path>}`` / ``${secret:<provider>:<path>}`` references."""

    category = "secret_provider"

    @abstractmethod
    def get(self, path: str) -> str: ...
