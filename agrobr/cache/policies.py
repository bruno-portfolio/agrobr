from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from enum import Enum
from typing import NamedTuple

from ..constants import Fonte
from ..exceptions import InvalidParameterError
from ..utils.time import utcnow


class CachePolicy(NamedTuple):
    ttl_seconds: int
    stale_max_seconds: int
    description: str
    smart_expiry: bool = False


class TTL(Enum):
    HOURS_24 = 24 * 60 * 60
    DAYS_7 = 7 * 24 * 60 * 60
    DAYS_30 = 30 * 24 * 60 * 60
    DAYS_90 = 90 * 24 * 60 * 60


CEPEA_UPDATE_HOUR_BRT = 18
CEPEA_UPDATE_HOUR_UTC = 21
CEPEA_UPDATE_MINUTE = 0

POLICIES: dict[str, CachePolicy] = {
    "cepea_diario": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.HOURS_24.value * 2,
        description="CEPEA indicador diário (expira às 18h)",
        smart_expiry=True,
    ),
}

SOURCE_POLICY_MAP: dict[Fonte, str] = {
    Fonte.CEPEA: "cepea_diario",
}


def _sem_cache(source: str) -> InvalidParameterError:
    com_cache = [fonte.value for fonte in SOURCE_POLICY_MAP]
    return InvalidParameterError(
        f"A fonte {source!r} não tem cache no agrobr. Fontes com cache: {com_cache}"
    )


def get_policy(source: Fonte | str, endpoint: str | None = None) -> CachePolicy:
    if isinstance(source, str):
        if source in POLICIES:
            return POLICIES[source]
        try:
            source = Fonte(source)
        except ValueError:
            raise _sem_cache(source) from None

    if endpoint:
        key = f"{source.value}_{endpoint}"
        if key in POLICIES:
            return POLICIES[key]

    if source not in SOURCE_POLICY_MAP:
        raise _sem_cache(source.value)
    return POLICIES[SOURCE_POLICY_MAP[source]]


def _get_smart_expiry_time(desde: datetime | None = None) -> datetime:
    instante = desde or utcnow()
    if instante.tzinfo is not None:
        instante = instante.astimezone(UTC).replace(tzinfo=None)
    virada = datetime.combine(instante.date(), time(CEPEA_UPDATE_HOUR_UTC, CEPEA_UPDATE_MINUTE))
    if instante >= virada:
        virada += timedelta(days=1)
    while virada.weekday() >= 5:
        virada += timedelta(days=1)
    return virada


def calculate_expiry(
    source: Fonte | str, endpoint: str | None = None, desde: datetime | None = None
) -> datetime:
    policy = get_policy(source, endpoint)

    if policy.smart_expiry:
        return _get_smart_expiry_time(desde)

    return (desde or utcnow()) + timedelta(seconds=policy.ttl_seconds)


def format_ttl(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} segundos"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minuto{'s' if minutes > 1 else ''}"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hora{'s' if hours > 1 else ''}"

    days = seconds // 86400
    return f"{days} dia{'s' if days > 1 else ''}"


def get_next_update_info(source: Fonte | str) -> dict[str, str]:
    policy = get_policy(source)

    if policy.smart_expiry:
        next_expiry = _get_smart_expiry_time()
        return {
            "type": "smart",
            "expires_at": next_expiry.strftime("%Y-%m-%d %H:%M"),
            "description": f"Expira às {CEPEA_UPDATE_HOUR_BRT}h BRT (atualização CEPEA)",
        }

    return {
        "type": "ttl",
        "ttl": format_ttl(policy.ttl_seconds),
        "description": policy.description,
    }
