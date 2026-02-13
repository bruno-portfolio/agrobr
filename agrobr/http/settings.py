"""Helpers centralizados para configuracao HTTP."""

from __future__ import annotations

from typing import Any

import httpx

from agrobr.constants import Fonte, HTTPSettings


def get_timeout(settings: HTTPSettings | None = None) -> httpx.Timeout:
    """Constroi ``httpx.Timeout`` a partir de ``HTTPSettings``.

    Args:
        settings: Instancia de HTTPSettings. Se None, usa defaults.

    Returns:
        httpx.Timeout configurado.
    """
    s = settings or HTTPSettings()
    return httpx.Timeout(
        connect=s.timeout_connect,
        read=s.timeout_read,
        write=s.timeout_write,
        pool=s.timeout_pool,
    )


def get_rate_limit(fonte: Fonte, settings: HTTPSettings | None = None) -> float:
    """Retorna rate limit (segundos entre requests) para uma fonte.

    Procura atributo ``rate_limit_{fonte.value}`` em HTTPSettings.
    Se nao existir, retorna ``rate_limit_default``.

    Args:
        fonte: Fonte de dados.
        settings: Instancia de HTTPSettings. Se None, usa defaults.

    Returns:
        Intervalo minimo em segundos entre requests.
    """
    s = settings or HTTPSettings()
    attr = f"rate_limit_{fonte.value}"
    return getattr(s, attr, s.rate_limit_default)


def get_client_kwargs(
    fonte: Fonte,
    settings: HTTPSettings | None = None,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Retorna dict pronto para ``httpx.AsyncClient(**kwargs)``.

    Inclui timeout configurado e headers base. Nao inclui rate limiter
    (que e responsabilidade do caller via ``RateLimiter``).

    Args:
        fonte: Fonte de dados (para logging e rate limit lookup).
        settings: Instancia de HTTPSettings. Se None, usa defaults.
        extra_headers: Headers adicionais a mesclar.

    Returns:
        Dict com ``timeout``, ``headers``, ``follow_redirects``.
    """
    timeout = get_timeout(settings)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "X-Agrobr-Source": fonte.value,
    }
    if extra_headers:
        headers.update(extra_headers)

    return {
        "timeout": timeout,
        "headers": headers,
        "follow_redirects": True,
    }
