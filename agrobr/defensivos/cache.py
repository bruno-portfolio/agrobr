from __future__ import annotations

from pathlib import Path

from agrobr import _log
from agrobr.constants import CacheSettings

logger = _log.get_logger(__name__)


def _cache_dir() -> Path:
    d = CacheSettings().cache_dir / "defensivos"
    d.mkdir(parents=True, exist_ok=True)
    return d


def invalidate() -> None:
    directory = _cache_dir()
    for path in directory.glob("*.csv"):
        path.unlink()
    (directory / "formulados.zip").unlink(missing_ok=True)
    for kind in ("formulados", "tecnicos"):
        snapshot_path(kind).unlink(missing_ok=True)
    logger.info("defensivos_cache_invalidated")


def snapshot_path(kind: str) -> Path:
    return _cache_dir() / f"{kind}.v3.zip"
