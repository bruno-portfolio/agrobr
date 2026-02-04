"""Cliente HTTP async para Notícias Agrícolas (fonte alternativa CEPEA).

Nota: Este site carrega dados via JavaScript/AJAX, então usa Playwright por padrão.
"""

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

_use_browser: bool = True


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
    produto_key = constants.NOTICIAS_AGRICOLAS_PRODUTOS.get(produto.lower())
    if produto_key is None:
        raise ValueError(
            f"Produto '{produto}' não disponível no Notícias Agrícolas. "
            f"Produtos disponíveis: {list(constants.NOTICIAS_AGRICOLAS_PRODUTOS.keys())}"
        )
    base = constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]
    return f"{base}/{produto_key}"


def set_use_browser(enabled: bool) -> None:
    """Habilita ou desabilita uso de browser para Notícias Agrícolas."""
    global _use_browser
    _use_browser = enabled
    logger.info("noticias_agricolas_browser_mode", enabled=enabled)


async def _fetch_with_browser(url: str, produto: str) -> str:
    """Busca usando Playwright (necessário pois página carrega dados via AJAX)."""
    from agrobr.http.browser import get_page

    logger.info(
        "browser_fetch",
        source="noticias_agricolas",
        url=url,
        produto=produto,
    )

    try:
        async with get_page() as page:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=30000,
            )

            if response is None:
                raise SourceUnavailableError(
                    source="noticias_agricolas",
                    url=url,
                    last_error="No response received",
                )

            try:
                await page.wait_for_selector(
                    "table.cot-fisicas",
                    timeout=15000,
                )
            except Exception:
                await page.wait_for_selector(
                    "table",
                    timeout=10000,
                )

            await page.wait_for_timeout(2000)

            html: str = await page.content()

            logger.info(
                "browser_fetch_success",
                source="noticias_agricolas",
                url=url,
                content_length=len(html),
                status=response.status,
            )

            return html

    except Exception as e:
        logger.error(
            "browser_fetch_failed",
            source="noticias_agricolas",
            url=url,
            error=str(e),
        )
        raise SourceUnavailableError(
            source="noticias_agricolas",
            url=url,
            last_error=str(e),
        ) from e


async def _fetch_with_httpx(url: str) -> str:
    """Busca usando httpx (pode não ter todos os dados se página usa AJAX)."""
    headers = UserAgentRotator.get_headers(source="noticias_agricolas")

    async def _fetch() -> httpx.Response:
        async with (
            RateLimiter.acquire(constants.Fonte.NOTICIAS_AGRICOLAS),
            httpx.AsyncClient(
                timeout=_get_timeout(),
                follow_redirects=True,
            ) as client,
        ):
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
        source="noticias_agricolas",
    )

    logger.info(
        "http_response",
        source="noticias_agricolas",
        status_code=response.status_code,
        content_length=len(response.content),
        encoding=actual_encoding,
        method="httpx",
    )

    return html


async def fetch_indicador_page(produto: str, force_httpx: bool = False) -> str:
    """
    Busca página de indicador CEPEA via Notícias Agrícolas.

    Esta é uma fonte alternativa que republica dados CEPEA/ESALQ
    sem proteção Cloudflare. Por padrão usa Playwright pois a página
    carrega dados via JavaScript/AJAX.

    Args:
        produto: Nome do produto (soja, milho, boi, cafe, algodao, trigo)
        force_httpx: Se True, usa httpx ao invés de browser (pode faltar dados)

    Returns:
        HTML da página como string

    Raises:
        SourceUnavailableError: Se não conseguir acessar após retries
        ValueError: Se o produto não estiver disponível
    """
    url = _get_produto_url(produto)

    logger.info(
        "http_request",
        source="noticias_agricolas",
        url=url,
        method="GET",
        produto=produto,
    )

    if not force_httpx and _use_browser:
        try:
            return await _fetch_with_browser(url, produto)
        except SourceUnavailableError:
            logger.warning(
                "browser_failed_trying_httpx",
                source="noticias_agricolas",
                url=url,
            )

    try:
        return await _fetch_with_httpx(url)
    except httpx.HTTPError as e:
        logger.error(
            "http_request_failed",
            source="noticias_agricolas",
            url=url,
            error=str(e),
        )
        raise SourceUnavailableError(
            source="noticias_agricolas",
            url=url,
            last_error=str(e),
        ) from e
