"""Telemetria opt-in."""

from __future__ import annotations

from .collector import (
    TelemetryCollector,
    track_cache_operation,
    track_fetch,
    track_parse_error,
)

__all__: list[str] = [
    "TelemetryCollector",
    "track_cache_operation",
    "track_fetch",
    "track_parse_error",
]
