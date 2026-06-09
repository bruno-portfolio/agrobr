"""Health-check state — pure-log persistence and alerting decisions."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from agrobr.alerts.notifier import AlertLevel
from agrobr.cache.duckdb_store import get_store
from agrobr.constants import AlertSettings, Fonte

if TYPE_CHECKING:
    import duckdb

logger = structlog.get_logger()


def _get_conn() -> duckdb.DuckDBPyConnection:
    return get_store()._get_conn()


def record_check(
    source: Fonte,
    status: str,
    category: str | None,
    latency_ms: float,
    message: str | None = None,
) -> None:
    """INSERT a health-check row (pure log, no mutable state)."""
    conn = _get_conn()
    conn.execute(
        "INSERT INTO health_checks (source, status, category, latency_ms, message, checked_at) "
        "VALUES (?, ?, ?, ?, ?, current_timestamp)",
        [source.value, status, category, latency_ms, message],
    )


def get_consecutive_failures(source: Fonte) -> int:
    """Count failures since the last OK for *source* (via query, not mutable state)."""
    conn = _get_conn()
    result = conn.execute(
        """
        SELECT COUNT(*) FROM health_checks
        WHERE source = ?
          AND checked_at > COALESCE(
              (SELECT MAX(checked_at) FROM health_checks
               WHERE source = ? AND status = 'ok'),
              '1970-01-01'
          )
          AND status != 'ok'
        """,
        [source.value, source.value],
    ).fetchone()
    return int(result[0]) if result and result[0] else 0


def get_last_success(source: Fonte) -> datetime | None:
    """Return the timestamp of the most recent OK check for *source*."""
    conn = _get_conn()
    result = conn.execute(
        "SELECT MAX(checked_at) FROM health_checks WHERE source = ? AND status = 'ok'",
        [source.value],
    ).fetchone()
    return result[0] if result and result[0] else None


def should_send_alert(
    source: Fonte,
    current_status: str,
    category: str | None,
    settings: AlertSettings | None = None,
) -> tuple[bool, AlertLevel | None]:
    """Decide whether to fire an alert and at what level.

    Returns (should_alert, level).  All filtering logic lives here;
    the notifier is a dumb pipe.
    """
    settings = settings or AlertSettings()

    # --- category filters (flags from AlertSettings) ---
    if category == "api_key_missing":
        return False, None
    if category == "parse_error" and not settings.alert_on_parse_error:
        return False, None
    if category == "layout_change" and not settings.alert_on_layout_change:
        return False, None
    if category == "source_down" and not settings.alert_on_source_down:
        return False, None
    if category == "anomaly" and not settings.alert_on_anomaly:
        return False, None
    if category == "soft_block" and not settings.alert_on_soft_block:
        return False, None

    failures = get_consecutive_failures(source)

    # --- recovery ---
    if current_status == "ok":
        if failures > 0 and settings.alert_on_recovery:
            return True, AlertLevel.INFO
        return False, None

    # --- consecutive-failure escalation ---
    if failures < settings.consecutive_failures_warning:
        return False, None
    if failures < settings.consecutive_failures_critical:
        return True, AlertLevel.WARNING

    # soft_block never escalates beyond WARNING
    if category == "soft_block":
        return True, AlertLevel.WARNING
    return True, AlertLevel.CRITICAL
