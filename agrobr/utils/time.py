from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

BRASILIA = timezone(timedelta(hours=-3), "BRT")


def utcnow() -> datetime:
    return utcnow_aware().replace(tzinfo=None)


def utcnow_aware() -> datetime:
    return datetime.now(UTC)


def hoje() -> date:
    """Data civil de Brasília, UTC-3 fixo: sem horário de verão desde o Decreto 9.772/2019."""
    return utcnow_aware().astimezone(BRASILIA).date()
