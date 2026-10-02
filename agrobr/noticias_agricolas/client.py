from __future__ import annotations

import httpx

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import RetriableStatusError, retry_async, should_retry_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize import regions
from agrobr.normalize.encoding import decode_content
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)

_SOFT_BLOCK_SIZE_THRESHOLD = 20_000


def _validate_html_has_data(html: str, url: str) -> None:
    if len(html) < _SOFT_BLOCK_SIZE_THRESHOLD and "<table" not in html.lower():
        raise SourceUnavailableError(
            source="noticias_agricolas",
            url=url,
            last_error=(
                "Soft block detected: response too small "
                f"({len(html)} bytes) and contains no table element"
            ),
        )


TIMEOUT = get_timeout()


def _get_produto_url(produto: str) -> str:
    produto_key = constants.NOTICIAS_AGRICOLAS_PRODUTOS.get(
        "_".join(regions.remover_acentos(produto).lower().split())
        if isinstance(produto, str)
        else ""
    )
    if produto_key is None:
        raise InvalidParameterError(
            f"Produto '{produto}' não disponível no Notícias Agrícolas. "
            f"Produtos disponíveis: {list(constants.NOTICIAS_AGRICOLAS_PRODUTOS.keys())}"
        )
    base = constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]
    return f"{base}/{produto_key}"


async def fetch_indicador_page(produto: str) -> str:
    warn_once(
        "noticias_agricolas",
        "Notícias Agrícolas (classificação restrito): fallback temporário do CEPEA, pendente "
        "deprecação. O site não publica termos de uso: todos os direitos reservados "
        "(Lei 9.610/98), sem permissão de republicação. Os dados vêm do CEPEA (CC BY-NC 4.0). "
        "Veja https://www.agrobr.dev/docs/licenses/.",
    )

    url = _get_produto_url(produto)
    headers = UserAgentRotator.get_headers(source="noticias_agricolas")

    logger.debug("http_request", source="noticias_agricolas", url=url, method="GET")
    logger.info("http_request", source="noticias_agricolas", produto=produto)

    async def _fetch() -> httpx.Response:
        async with (
            RateLimiter.acquire(constants.Fonte.NOTICIAS_AGRICOLAS),
            httpx.AsyncClient(
                timeout=TIMEOUT,
                follow_redirects=True,
            ) as client,
        ):
            response = await client.get(url, headers=headers)

            if should_retry_status(response.status_code):
                raise RetriableStatusError(
                    f"Retriable status: {response.status_code}",
                    request=response.request,
                    response=response,
                )

            responses.raise_for_status(response, source="noticias_agricolas")
            return response

    try:
        response = await retry_async(_fetch)
    except httpx.HTTPError as e:
        logger.debug("http_request_failed_detail", source="noticias_agricolas", url=url)
        logger.error(
            "http_request_failed",
            source="noticias_agricolas",
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
