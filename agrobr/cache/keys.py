"""Construção de cache keys versionadas."""

from __future__ import annotations

import hashlib
from typing import Any

_KEY_PARTS = 4


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


def parse_cache_key(key: str) -> dict[str, str]:
    """Extrai componentes de uma cache key versionada.

    Args:
        key: Cache key no formato ``dataset|hash|vX.Y.Z|svX.Y``

    Returns:
        Dict com ``dataset``, ``params_hash``, ``lib_version``, ``schema_version``.

    Raises:
        ValueError: Se a key nao tem o formato esperado.
    """
    parts = key.split("|")
    if len(parts) != _KEY_PARTS:
        raise ValueError(f"Cache key inválida (esperado {_KEY_PARTS} partes): {key!r}")
    return {
        "dataset": parts[0],
        "params_hash": parts[1],
        "lib_version": parts[2].lstrip("v"),
        "schema_version": parts[3].lstrip("sv"),
    }


def is_legacy_key(key: str) -> bool:
    """Retorna True se a key nao segue o formato versionado (4 partes separadas por ``|``)."""
    return len(key.split("|")) != _KEY_PARTS


def legacy_key_prefix(key: str) -> str:
    """Extrai o prefixo ``dataset|params_hash`` de uma key versionada.

    Usado para localizar entradas legacy no banco que compartilham
    o mesmo dataset + params mas nao tem versao na key.
    """
    parts = key.split("|")
    if len(parts) < 2:
        return key
    return f"{parts[0]}|{parts[1]}"
