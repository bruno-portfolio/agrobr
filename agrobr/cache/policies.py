from __future__ import annotations

from datetime import datetime, time, timedelta
from enum import Enum
from typing import NamedTuple

from ..constants import Fonte


class CachePolicy(NamedTuple):
    """Política de cache para uma fonte."""

    ttl_seconds: int
    stale_max_seconds: int
    description: str
    smart_expiry: bool = False


class TTL(Enum):
    """TTLs pré-definidos."""

    MINUTES_15 = 15 * 60
    MINUTES_30 = 30 * 60
    HOUR_1 = 60 * 60
    HOURS_4 = 4 * 60 * 60
    HOURS_12 = 12 * 60 * 60
    HOURS_24 = 24 * 60 * 60
    DAYS_7 = 7 * 24 * 60 * 60
    DAYS_30 = 30 * 24 * 60 * 60
    DAYS_90 = 90 * 24 * 60 * 60


CEPEA_UPDATE_HOUR = 18
CEPEA_UPDATE_MINUTE = 0


POLICIES: dict[str, CachePolicy] = {
    "cepea_diario": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.HOURS_24.value * 2,
        description="CEPEA indicador diário (expira às 18h)",
        smart_expiry=True,
    ),
    "cepea_semanal": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_7.value,
        description="CEPEA indicador semanal (atualiza sexta)",
        smart_expiry=False,
    ),
    "conab_safras": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="CONAB safras (atualiza mensalmente)",
        smart_expiry=False,
    ),
    "conab_balanco": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="CONAB balanço (atualiza mensalmente)",
        smart_expiry=False,
    ),
    "ibge_pam": CachePolicy(
        ttl_seconds=TTL.DAYS_7.value,
        stale_max_seconds=TTL.DAYS_90.value,
        description="IBGE PAM (atualiza anualmente)",
        smart_expiry=False,
    ),
    "ibge_lspa": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="IBGE LSPA (atualiza mensalmente)",
        smart_expiry=False,
    ),
    "noticias_agricolas": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.HOURS_24.value * 2,
        description="Notícias Agrícolas (expira às 18h, mirror CEPEA)",
        smart_expiry=True,
    ),
    "conab_custo": CachePolicy(
        ttl_seconds=TTL.DAYS_30.value,
        stale_max_seconds=TTL.DAYS_90.value,
        description="CONAB custos de produção (atualiza anualmente por safra)",
        smart_expiry=False,
    ),
    "inmet": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_7.value,
        description="INMET dados meteorológicos (atualiza diariamente)",
        smart_expiry=False,
    ),
    "bcb": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="BCB/SICOR crédito rural (atualiza mensalmente)",
        smart_expiry=False,
    ),
    "comexstat": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_7.value,
        description="ComexStat exportação (atualiza semanalmente/mensalmente)",
        smart_expiry=False,
    ),
    "anda": CachePolicy(
        ttl_seconds=TTL.DAYS_7.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="ANDA entregas fertilizantes (atualiza mensalmente, PDF)",
        smart_expiry=False,
    ),
}

SOURCE_POLICY_MAP: dict[Fonte, str] = {
    Fonte.ANDA: "anda",
    Fonte.BCB: "bcb",
    Fonte.CEPEA: "cepea_diario",
    Fonte.COMEXSTAT: "comexstat",
    Fonte.CONAB: "conab_safras",
    Fonte.IBGE: "ibge_lspa",
    Fonte.INMET: "inmet",
    Fonte.NASA_POWER: "bcb",
    Fonte.NOTICIAS_AGRICOLAS: "noticias_agricolas",
}


def get_policy(source: Fonte | str, endpoint: str | None = None) -> CachePolicy:
    """
    Retorna política de cache para uma fonte/endpoint.

    Args:
        source: Fonte de dados
        endpoint: Endpoint específico (opcional)

    Returns:
        CachePolicy aplicável
    """
    if isinstance(source, str):
        if source in POLICIES:
            return POLICIES[source]
        try:
            source = Fonte(source)
        except ValueError:
            return POLICIES["cepea_diario"]

    if endpoint:
        key = f"{source.value}_{endpoint}"
        if key in POLICIES:
            return POLICIES[key]

    default_key = SOURCE_POLICY_MAP.get(source, "cepea_diario")
    return POLICIES[default_key]


def _get_smart_expiry_time() -> datetime:
    """
    Calcula próximo horário de expiração para CEPEA (18h).

    CEPEA atualiza dados por volta das 17-18h.
    Cache expira às 18h para pegar dados novos.

    Returns:
        Datetime da próxima expiração
    """
    now = datetime.now()
    today_expiry = datetime.combine(now.date(), time(CEPEA_UPDATE_HOUR, CEPEA_UPDATE_MINUTE))

    if now < today_expiry:
        return today_expiry
    else:
        return today_expiry + timedelta(days=1)


def _get_last_expiry_time() -> datetime:
    """
    Retorna o último horário de expiração (18h anterior).

    Returns:
        Datetime da última expiração
    """
    return _get_smart_expiry_time() - timedelta(days=1)


def get_ttl(source: Fonte | str, endpoint: str | None = None) -> int:
    """
    Retorna TTL em segundos para uma fonte.

    Args:
        source: Fonte de dados
        endpoint: Endpoint específico

    Returns:
        TTL em segundos
    """
    return get_policy(source, endpoint).ttl_seconds


def get_stale_max(source: Fonte | str, endpoint: str | None = None) -> int:
    """
    Retorna tempo máximo stale em segundos.

    Args:
        source: Fonte de dados
        endpoint: Endpoint específico

    Returns:
        Stale máximo em segundos
    """
    return get_policy(source, endpoint).stale_max_seconds


def is_expired(created_at: datetime, source: Fonte | str, endpoint: str | None = None) -> bool:
    """
    Verifica se entrada de cache está expirada.

    Para fontes com smart_expiry (CEPEA), expira às 18h.
    Para outras fontes, usa TTL fixo.

    Args:
        created_at: Data de criação
        source: Fonte de dados
        endpoint: Endpoint específico

    Returns:
        True se expirado
    """
    policy = get_policy(source, endpoint)

    if policy.smart_expiry:
        last_expiry = _get_last_expiry_time()
        return created_at < last_expiry

    expires_at = created_at + timedelta(seconds=policy.ttl_seconds)
    return datetime.now() > expires_at


def is_stale_acceptable(created_at: datetime, source: Fonte | str) -> bool:
    """
    Verifica se dados stale ainda são aceitáveis.

    Args:
        created_at: Data de criação
        source: Fonte de dados

    Returns:
        True se stale ainda é aceitável
    """
    stale_max = get_stale_max(source)
    max_acceptable = created_at + timedelta(seconds=stale_max)
    return datetime.now() <= max_acceptable


def calculate_expiry(source: Fonte | str, endpoint: str | None = None) -> datetime:
    """
    Calcula data de expiração para nova entrada.

    Para fontes com smart_expiry (CEPEA), retorna próximas 18h.
    Para outras fontes, usa TTL fixo.

    Args:
        source: Fonte de dados
        endpoint: Endpoint específico

    Returns:
        Data de expiração
    """
    policy = get_policy(source, endpoint)

    if policy.smart_expiry:
        return _get_smart_expiry_time()

    return datetime.now() + timedelta(seconds=policy.ttl_seconds)


class InvalidationReason(Enum):
    """Razões para invalidação de cache."""

    EXPIRED = "expired"
    MANUAL = "manual"
    SOURCE_UPDATE = "source_update"
    PARSE_ERROR = "parse_error"
    VALIDATION_ERROR = "validation_error"
    FINGERPRINT_CHANGE = "fingerprint_change"


def should_refresh(
    created_at: datetime,
    source: Fonte | str,
    force: bool = False,
    endpoint: str | None = None,
) -> tuple[bool, str]:
    """
    Determina se cache deve ser atualizado.

    Args:
        created_at: Data de criação do cache
        source: Fonte de dados
        force: Forçar atualização
        endpoint: Endpoint específico

    Returns:
        Tupla (deve_atualizar, razão)
    """
    if force:
        return True, "force_refresh"

    if is_expired(created_at, source, endpoint):
        return True, "expired"

    return False, "fresh"


def format_ttl(seconds: int) -> str:
    """
    Formata TTL para exibição.

    Args:
        seconds: TTL em segundos

    Returns:
        String formatada (ex: "4 horas", "7 dias")
    """
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
    """
    Retorna informações sobre próxima atualização.

    Args:
        source: Fonte de dados

    Returns:
        Dict com info de expiração
    """
    policy = get_policy(source)

    if policy.smart_expiry:
        next_expiry = _get_smart_expiry_time()
        return {
            "type": "smart",
            "expires_at": next_expiry.strftime("%Y-%m-%d %H:%M"),
            "description": f"Expira às {CEPEA_UPDATE_HOUR}h (atualização CEPEA)",
        }

    return {
        "type": "ttl",
        "ttl": format_ttl(policy.ttl_seconds),
        "description": policy.description,
    }
