from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

_config: AgrobrConfig | None = None


@dataclass
class AgrobrConfig:
    mode: Literal["normal", "deterministic"] = "normal"
    snapshot_date: date | None = None
    snapshot_path: Path | None = None

    cache_enabled: bool = True
    cache_path: Path | None = None

    network_enabled: bool = True

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


__all__ = [
    "AgrobrConfig",
    "set_mode",
    "get_config",
    "reset_config",
]
