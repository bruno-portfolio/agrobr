"""Cliente HTTP async para CEPEA."""

from __future__ import annotations

import httpx
import structlog

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import retry_async, should_retry_status
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.encoding import decode_content

logger = structlog.get_logger()

# Flag para controlar uso de browser
_use_browser: bool = True
# Flag para controlar uso de fonte alternativa (Notícias Agrícolas)
_use_alternative_source: bool = True


def set_use_browser(enabled: bool) -> None:
    """Habilita ou desabilita uso de browser para CEPEA."""
    global _use_browser
    _use_browser = enabled
    logger.info("cepea_browser_mode", enabled=enabled)


def set_use_alternative_source(enabled: bool) -> None:
    """Habilita ou desabilita uso de fonte alternativa (Notícias Agrícolas)."""
    global _use_alternative_source
    _use_alternative_source = enabled
    logger.info("cepea_alternative_source_mode", enabled=enabled)


def _get_timeout() -> httpx.Timeout:
    """Retorna configuração de timeout."""
    settings = constants.HTTPSettings()
    return httpx.Timeout(
        connect=settings.timeout_connect,
        read=settings.timeout_read,
        write=settings.timeout_write,
        pool=settings.timeout_pool,
    )


def _get_produto_url(produto: str) -> str:
    """Retorna URL do indicador para o produto."""
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    base = constants.URLS[constants.Fonte.CEPEA]["indicadores"]
    return f"{base}/{produto_key}.aspx"


async def _fetch_with_httpx(url: str, headers: dict) -> str:
    """Tenta buscar com httpx (mais rápido, mas pode falhar com Cloudflare)."""

    async def _fetch() -> httpx.Response:
        async with RateLimiter.acquire(constants.Fonte.CEPEA):
            async with httpx.AsyncClient(
                timeout=_get_timeout(),
                follow_redirects=True,
            ) as client:
                response = await client.get(url, headers=headers)

                if should_retry_status(response.status_code):
                    raise httpx.HTTPStatusError(
                        f"Retriable status: {response.status_code}",
                        request=response.request,
                        response=response,
                    )

                response.raise_for_status()
                return response

    response = await retry_async(_fetch)

    declared_encoding = response.charset_encoding
    html, actual_encoding = decode_content(
        response.content,
        declared_encoding=declared_encoding,
        source="cepea",
    )

    logger.info(
        "http_response",
        source="cepea",
        status_code=response.status_code,
        content_length=len(response.content),
        encoding=actual_encoding,
        method="httpx",
    )

    return html


async def _fetch_with_browser(produto: str) -> str:
    """Busca usando Playwright (contorna Cloudflare)."""
    from agrobr.http.browser import fetch_cepea_indicador

    logger.info("browser_fallback", source="cepea", produto=produto)
    return await fetch_cepea_indicador(produto)


async def _fetch_with_alternative_source(produto: str) -> str:
    """Busca usando Notícias Agrícolas (fonte alternativa sem Cloudflare)."""
    from agrobr.noticias_agricolas.client import fetch_indicador_page as na_fetch

    logger.info(
        "alternative_source_fallback",
        source="cepea",
        alternative="noticias_agricolas",
        produto=produto,
    )
    return await na_fetch(produto)


async def fetch_indicador_page(
    produto: str,
    force_browser: bool = False,
    force_alternative: bool = False,
) -> str:
    """
    Busca página de indicador do CEPEA.

    Cadeia de fallback:
    1. httpx direto (mais rápido)
    2. Playwright browser (contorna Cloudflare básico)
    3. Notícias Agrícolas (fonte alternativa sem Cloudflare)

    Args:
        produto: Nome do produto (soja, milho, cafe, boi, etc)
        force_browser: Se True, pula httpx e usa browser diretamente
        force_alternative: Se True, usa fonte alternativa diretamente

    Returns:
        HTML da página como string

    Raises:
        SourceUnavailableError: Se não conseguir acessar após todos os fallbacks
    """
    # Se forçar fonte alternativa, vai direto para Notícias Agrícolas
    if force_alternative:
        return await _fetch_with_alternative_source(produto)

    url = _get_produto_url(produto)
    headers = UserAgentRotator.get_headers(source="cepea")

    logger.info(
        "http_request",
        source="cepea",
        url=url,
        method="GET",
    )

    last_error: str = ""

    # Passo 1: Tenta httpx (a menos que force_browser)
    if not force_browser:
        try:
            return await _fetch_with_httpx(url, headers)
        except (httpx.HTTPError, httpx.HTTPStatusError, SourceUnavailableError) as e:
            last_error = str(e)
            logger.warning(
                "httpx_failed",
                source="cepea",
                url=url,
                error=last_error,
            )

    # Passo 2: Tenta browser (se habilitado)
    if _use_browser:
        try:
            return await _fetch_with_browser(produto)
        except (SourceUnavailableError, Exception) as e:
            last_error = str(e)
            logger.warning(
                "browser_failed",
                source="cepea",
                url=url,
                error=last_error,
            )

    # Passo 3: Tenta fonte alternativa (se habilitado)
    if _use_alternative_source:
        try:
            return await _fetch_with_alternative_source(produto)
        except (SourceUnavailableError, ValueError, Exception) as e:
            last_error = str(e)
            logger.warning(
                "alternative_source_failed",
                source="cepea",
                alternative="noticias_agricolas",
                error=last_error,
            )

    # Todos os métodos falharam
    logger.error(
        "all_methods_failed",
        source="cepea",
        url=url,
        last_error=last_error,
    )
    raise SourceUnavailableError(
        source="cepea",
        url=url,
        last_error=f"All fetch methods failed. Last error: {last_error}",
    )


async def fetch_series_historica(produto: str, anos: int = 5) -> str:
    """
    Busca série histórica do CEPEA.

    Args:
        produto: Nome do produto
        anos: Quantidade de anos de histórico

    Returns:
        HTML da página de série histórica
    """
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    base = constants.URLS[constants.Fonte.CEPEA]["base"]
    url = f"{base}/br/consultas-ao-banco-de-dados-do-site.aspx"

    headers = UserAgentRotator.get_headers(source="cepea")

    logger.info(
        "http_request",
        source="cepea",
        url=url,
        method="GET",
        produto=produto,
        anos=anos,
    )

    async def _fetch() -> httpx.Response:
        async with RateLimiter.acquire(constants.Fonte.CEPEA):
            async with httpx.AsyncClient(
                timeout=_get_timeout(),
                follow_redirects=True,
            ) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response

    try:
        response = await retry_async(_fetch)
    except httpx.HTTPError as e:
        logger.error(
            "http_request_failed",
            source="cepea",
            url=url,
            error=str(e),
        )
        raise SourceUnavailableError(
            source="cepea",
            url=url,
            last_error=str(e),
        ) from e

    declared_encoding = response.charset_encoding
    html, _ = decode_content(
        response.content,
        declared_encoding=declared_encoding,
        source="cepea",
    )

    return html
