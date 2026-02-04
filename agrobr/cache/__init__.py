"""Cache DuckDB com separação cache/histórico."""

from __future__ import annotations

from .duckdb_store import DuckDBStore, get_store
from .history import HistoryManager, get_history_manager
from .policies import (
    TTL,
    CachePolicy,
    calculate_expiry,
    get_policy,
    get_stale_max,
    get_ttl,
    is_expired,
    is_stale_acceptable,
)

__all__ = [
    "DuckDBStore",
    "get_store",
    "CachePolicy",
    "TTL",
    "get_policy",
    "get_ttl",
    "get_stale_max",
    "is_expired",
    "is_stale_acceptable",
    "calculate_expiry",
    "HistoryManager",
    "get_history_manager",
]
