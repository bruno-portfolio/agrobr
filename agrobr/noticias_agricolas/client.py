"""Cliente HTTP async para Notícias Agrícolas (fonte alternativa CEPEA).

O Notícias Agrícolas serve dados CEPEA/ESALQ via HTML server-side rendered.
Não requer JavaScript nem Playwright — httpx puro com parse BeautifulSoup.

AVISO: Fallback temporário. Pendente deprecação em favor de acesso direto
ao CEPEA. Dados sujeitos a CC BY-NC 4.0 (CEPEA) + direitos reservados (NA).
"""

from __future__ import annotations

import warnings

import httpx
import structlog

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import retry_async, should_retry_status
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.encoding import decode_content

logger = structlog.get_logger()

_WARNED = False

_SOFT_BLOCK_SIZE_THRESHOLD = 20_000


def _validate_html_has_data(html: str, url: str) -> None:
    """Valida que o HTML contém dados reais (não é página de consent/challenge).

    Páginas normais do NA são ~75KB com tabelas. Páginas de challenge/consent
    são ~10KB sem tabela. Se o HTML é pequeno E não tem <table, é soft block.
    """
    if len(html) < _SOFT_BLOCK_SIZE_THRESHOLD and "<table" not in html.lower():
        raise SourceUnavailableError(
            source="noticias_agricolas",
            url=url,
            last_error=(
                "Soft block detected: response too small "
                f"({len(html)} bytes) and contains no table element"
            ),
        )


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


async def fetch_indicador_page(produto: str) -> str:
    """
    Busca página de indicador CEPEA via Notícias Agrícolas.

    Esta é uma fonte alternativa que republica dados CEPEA/ESALQ
    sem proteção Cloudflare. Os dados vêm server-side rendered
    no HTML, sem necessidade de JavaScript.

    Args:
        produto: Nome do produto (soja, milho, boi, cafe, algodao, trigo, etc.)

    Returns:
        HTML da página como string

    Raises:
        SourceUnavailableError: Se não conseguir acessar após retries
        ValueError: Se o produto não estiver disponível
    """
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "Notícias Agrícolas: fallback temporário do CEPEA, pendente "
            "deprecação. Dados originários do CEPEA (CC BY-NC 4.0). "
            "Redistribuição sujeita a restrições.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

    url = _get_produto_url(produto)
    headers = UserAgentRotator.get_headers(source="noticias_agricolas")

    logger.info(
        "http_request",
        source="noticias_agricolas",
        url=url,
        method="GET",
        produto=produto,
    )

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

    try:
        response = await retry_async(_fetch)
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
    )

    _validate_html_has_data(html, url)

    return html
