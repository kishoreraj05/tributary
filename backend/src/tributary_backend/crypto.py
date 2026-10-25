from __future__ import annotations

import logging
from functools import lru_cache

from cryptography.fernet import Fernet

from .settings import get_settings

log = logging.getLogger(__name__)


@lru_cache
def _fernet() -> Fernet:
    settings = get_settings()
    key = settings.secret_key
    if not key:
        path = settings.data_dir / "secret.key"
        if path.exists():
            key = path.read_text().strip()
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            key = Fernet.generate_key().decode()
            path.write_text(key)
            path.chmod(0o600)
            log.warning("TRIBUTARY_SECRET_KEY not set; generated a dev key at %s", path)
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()
