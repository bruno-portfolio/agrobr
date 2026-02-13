"""Construção de cache keys versionadas."""

from __future__ import annotations

import hashlib
from typing import Any


def build_cache_key(
    dataset: str,
    params: dict[str, Any],
    schema_version: str = "1.0",
) -> str:
    from agrobr import __version__

    sorted_items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    raw = "&".join(f"{k}={v}" for k, v in sorted_items)
    params_hash = hashlib.sha256(raw.encode()).hexdigest()[:12]

    return f"{dataset}|{params_hash}|v{__version__}|sv{schema_version}"
