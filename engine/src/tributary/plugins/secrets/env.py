from __future__ import annotations

import os
import re

from ...core import SecretProvider
from ...errors import SecretNotFound


class EnvSecretProvider(SecretProvider):
    """Secrets from environment variables: ${secret:dq/pw} reads TRIBUTARY_SECRET_DQ_PW."""

    type_name = "env"

    def get(self, path: str) -> str:
        candidates = [path] if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", path) else []
        candidates.append("TRIBUTARY_SECRET_" + re.sub(r"[^A-Za-z0-9]", "_", path).upper())
        for name in candidates:
            if name in os.environ:
                return os.environ[name]
        raise SecretNotFound(f"secret '{path}' not found; set the environment variable {candidates[-1]}")
