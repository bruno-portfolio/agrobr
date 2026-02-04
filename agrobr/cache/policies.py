"""
Políticas de cache e TTL por fonte.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import NamedTuple

from ..constants import Fonte


class CachePolicy(NamedTuple):
    """Política de cache para uma fonte."""

    ttl_seconds: int
    stale_max_seconds: int
    description: str


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


POLICIES: dict[str, CachePolicy] = {
    "cepea_diario": CachePolicy(
        ttl_seconds=TTL.HOURS_4.value,
        stale_max_seconds=TTL.HOURS_24.value * 2,
        description="CEPEA indicador diário (atualiza ~18h)",
    ),
    "cepea_semanal": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_7.value,
        description="CEPEA indicador semanal (atualiza sexta)",
    ),
    "conab_safras": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="CONAB safras (atualiza mensalmente)",
    ),
    "conab_balanco": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="CONAB balanço (atualiza mensalmente)",
    ),
    "ibge_pam": CachePolicy(
        ttl_seconds=TTL.DAYS_7.value,
        stale_max_seconds=TTL.DAYS_90.value,
        description="IBGE PAM (atualiza anualmente)",
    ),
    "ibge_lspa": CachePolicy(
        ttl_seconds=TTL.HOURS_24.value,
        stale_max_seconds=TTL.DAYS_30.value,
        description="IBGE LSPA (atualiza mensalmente)",
    ),
    "noticias_agricolas": CachePolicy(
        ttl_seconds=TTL.HOURS_4.value,
        stale_max_seconds=TTL.HOURS_24.value * 2,
        description="Notícias Agrícolas (mirror CEPEA)",
    ),
}

SOURCE_POLICY_MAP: dict[Fonte, str] = {
    Fonte.CEPEA: "cepea_diario",
    Fonte.CONAB: "conab_safras",
    Fonte.IBGE: "ibge_lspa",
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


def is_expired(created_at: datetime, source: Fonte | str) -> bool:
    """
    Verifica se entrada de cache está expirada.

    Args:
        created_at: Data de criação
        source: Fonte de dados

    Returns:
        True se expirado
    """
    ttl = get_ttl(source)
    expires_at = created_at + timedelta(seconds=ttl)
    return datetime.utcnow() > expires_at


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
    return datetime.utcnow() <= max_acceptable


def calculate_expiry(source: Fonte | str, endpoint: str | None = None) -> datetime:
    """
    Calcula data de expiração para nova entrada.

    Args:
        source: Fonte de dados
        endpoint: Endpoint específico

    Returns:
        Data de expiração
    """
    ttl = get_ttl(source, endpoint)
    return datetime.utcnow() + timedelta(seconds=ttl)


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
) -> tuple[bool, str]:
    """
    Determina se cache deve ser atualizado.

    Args:
        created_at: Data de criação do cache
        source: Fonte de dados
        force: Forçar atualização

    Returns:
        Tupla (deve_atualizar, razão)
    """
    if force:
        return True, "force_refresh"

    if is_expired(created_at, source):
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
