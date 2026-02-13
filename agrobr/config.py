"""Configuracao global do agrobr."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

_config: AgrobrConfig | None = None


@dataclass
class AgrobrConfig:
    """Configuracao global do agrobr."""

    mode: Literal["normal", "deterministic"] = "normal"
    snapshot_date: date | None = None
    snapshot_path: Path | None = None

    cache_enabled: bool = True
    cache_path: Path | None = None

    network_enabled: bool = True
    timeout_seconds: int = 30

    browser_fallback: bool = False
    alternative_source: bool = True

    log_level: str = "INFO"

    def is_deterministic(self) -> bool:
        return self.mode == "deterministic"

    def get_snapshot_dir(self) -> Path:
        if self.snapshot_path:
            return self.snapshot_path
        return Path.home() / ".agrobr" / "snapshots"

    def get_current_snapshot_path(self) -> Path | None:
        if not self.snapshot_date:
            return None
        return self.get_snapshot_dir() / self.snapshot_date.isoformat()


def set_mode(
    mode: Literal["normal", "deterministic"],
    snapshot: str | date | None = None,
    snapshot_path: str | Path | None = None,
) -> None:
    """
    Define modo de operacao do agrobr.

    Args:
        mode: "normal" ou "deterministic"
        snapshot: Data do snapshot (YYYY-MM-DD ou date)
        snapshot_path: Caminho customizado para snapshots

    Example:
        agrobr.set_mode("normal")
        agrobr.set_mode("deterministic", snapshot="2025-12-31")
    """
    global _config

    if isinstance(snapshot, str):
        snapshot = date.fromisoformat(snapshot)

    if isinstance(snapshot_path, str):
        snapshot_path = Path(snapshot_path)

    _config = AgrobrConfig(
        mode=mode,
        snapshot_date=snapshot,
        snapshot_path=snapshot_path,
        network_enabled=(mode == "normal"),
    )


def get_config() -> AgrobrConfig:
    global _config
    if _config is None:
        _config = AgrobrConfig()
    return _config


def reset_config() -> None:
    global _config
    _config = None


def configure(
    cache_enabled: bool | None = None,
    cache_path: str | Path | None = None,
    timeout_seconds: int | None = None,
    browser_fallback: bool | None = None,
    alternative_source: bool | None = None,
    log_level: str | None = None,
) -> None:
    config = get_config()

    if cache_enabled is not None:
        config.cache_enabled = cache_enabled
    if cache_path is not None:
        config.cache_path = Path(cache_path) if isinstance(cache_path, str) else cache_path
    if timeout_seconds is not None:
        config.timeout_seconds = timeout_seconds
    if browser_fallback is not None:
        config.browser_fallback = browser_fallback
    if alternative_source is not None:
        config.alternative_source = alternative_source
    if log_level is not None:
        config.log_level = log_level


__all__ = [
    "AgrobrConfig",
    "set_mode",
    "get_config",
    "reset_config",
    "configure",
]
