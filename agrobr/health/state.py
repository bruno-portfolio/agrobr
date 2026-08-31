"""Health-check state — pure-log persistence and alerting decisions."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from agrobr.alerts.notifier import AlertLevel
from agrobr.cache.duckdb_store import get_store
from agrobr.constants import AlertSettings, Fonte

if TYPE_CHECKING:
    pass

logger = structlog.get_logger()


def record_check(
    source: Fonte,
    status: str,
    category: str | None,
    latency_ms: float,
    message: str | None = None,
) -> None:
    """INSERT a health-check row (pure log, no mutable state)."""
    store = get_store()
    with store._lock:
        conn = store._get_conn()
        if conn is None:
            return
        conn.execute(
            "INSERT INTO health_checks (source, status, category, latency_ms, message, checked_at) "
            "VALUES (?, ?, ?, ?, ?, current_timestamp)",
            [source.value, status, category, latency_ms, message],
        )


def get_consecutive_failures(source: Fonte) -> int:
    """Count failures since the last OK for *source* (via query, not mutable state)."""
    store = get_store()
    with store._lock:
        conn = store._get_conn()
        if conn is None:
            return 0
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


def _category_allows_alert(category: str | None, settings: AlertSettings) -> bool:
    if category == "api_key_missing":
        return False
    if category == "parse_error":
        return settings.alert_on_parse_error
    if category == "layout_change":
        return settings.alert_on_layout_change
    if category == "source_down":
        return settings.alert_on_source_down
    if category == "anomaly":
        return settings.alert_on_anomaly
    if category == "soft_block":
        return settings.alert_on_soft_block
    return True


def get_alertable_failures(source: Fonte, settings: AlertSettings | None = None) -> int:
    """Count failures since the last OK whose category would allow an alert.

    Recovery must not fire for an outage that stayed silent — failures whose
    category is suppressed (a missing API key, or a flag turned off) never
    reached the user, so there is nothing to recover from.
    """
    settings = settings or AlertSettings()
    store = get_store()
    with store._lock:
        conn = store._get_conn()
        if conn is None:
            return 0
        rows = conn.execute(
            """
            SELECT category FROM health_checks
            WHERE source = ?
              AND checked_at > COALESCE(
                  (SELECT MAX(checked_at) FROM health_checks
                   WHERE source = ? AND status = 'ok'),
                  '1970-01-01'
              )
              AND status != 'ok'
            """,
            [source.value, source.value],
        ).fetchall()
    return sum(1 for (category,) in rows if _category_allows_alert(category, settings))


def get_last_success(source: Fonte) -> datetime | None:
    """Return the timestamp of the most recent OK check for *source*."""
    store = get_store()
    with store._lock:
        conn = store._get_conn()
        if conn is None:
            return None
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
    prior_failures: int | None = None,
) -> tuple[bool, AlertLevel | None]:
    """Decide whether to fire an alert and at what level.

    Args:
        prior_failures: alertable failures *before* the current check was
            recorded (see `get_alertable_failures`). Required for recovery
            detection, since recording an OK resets the query-based counter
            to zero.

    Returns (should_alert, level).  All filtering logic lives here;
    the notifier is a dumb pipe.

    Both escalation and recovery count only alertable failures, so an
    incident that stayed silent never produces a recovery. Alerts fire when
    the counter crosses a threshold, not on every run above it.
    """
    settings = settings or AlertSettings()

    if not _category_allows_alert(category, settings):
        return False, None

    # --- recovery: only for outages that were eligible to alert ---
    if current_status == "ok":
        previous = (
            prior_failures
            if prior_failures is not None
            else get_alertable_failures(source, settings)
        )
        if previous >= settings.consecutive_failures_warning and settings.alert_on_recovery:
            return True, AlertLevel.INFO
        return False, None

    failures = get_alertable_failures(source, settings)
    capped_at_warning = category == "soft_block" or current_status != "failed"

    # --- threshold crossings only ---
    if failures == settings.consecutive_failures_critical:
        if capped_at_warning:
            return True, AlertLevel.WARNING
        return True, AlertLevel.CRITICAL
    if failures == settings.consecutive_failures_warning:
        return True, AlertLevel.WARNING
    return False, None
