from __future__ import annotations

import time
from datetime import datetime
from typing import NamedTuple

import httpx
import structlog

from agrobr import constants
from agrobr.constants import _CEPEA_ENDPOINTS
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import RetriableStatusError, retry_async, should_retry_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.encoding import decode_content
from agrobr.utils.io import validate_download
from agrobr.utils.time import utcnow

logger = structlog.get_logger()


class FetchResult(NamedTuple):
    html: str
    source: str


class SerieBaixada(NamedTuple):
    conteudo: bytes
    url: str
    fetched_at: datetime


_use_browser: bool = False
_use_alternative_source: bool = True

_circuit_state: dict[str, float] = {}
_CIRCUIT_RESET_SECONDS: float = 600.0


TIMEOUT = get_timeout()


def _endpoint_index(endpoint: str) -> int:
    try:
        return _CEPEA_ENDPOINTS.index(endpoint)
    except ValueError:
        return -1


def _is_circuit_open(endpoint: str) -> bool:
    opened_at = _circuit_state.get(endpoint)
    if opened_at is None:
        return False
    elapsed = time.monotonic() - opened_at
    if elapsed >= _CIRCUIT_RESET_SECONDS:
        del _circuit_state[endpoint]
        logger.info(
            "cepea_circuit_reset",
            endpoint_index=_endpoint_index(endpoint),
            elapsed_s=int(elapsed),
        )
        return False
    return True


def _open_circuit(endpoint: str) -> None:
    _circuit_state[endpoint] = time.monotonic()
    logger.debug(
        "cepea_circuit_opened",
        endpoint_index=_endpoint_index(endpoint),
        reset_after_s=int(_CIRCUIT_RESET_SECONDS),
    )


def _get_produto_url(produto: str, base: str) -> str:
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    return f"{base}/br/indicador/{produto_key}.aspx"


async def _get(url: str, headers: dict[str, str]) -> httpx.Response:
    async def _fetch() -> httpx.Response:
        async with (
            RateLimiter.acquire(constants.Fonte.CEPEA),
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

            responses.raise_for_status(response, source="cepea")
            return response

    return await retry_async(_fetch)


async def _fetch_with_httpx(url: str, headers: dict[str, str]) -> FetchResult:
    response = await _get(url, headers)

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

    return FetchResult(html=html, source="cepea")


async def _try_endpoints(produto: str, headers: dict[str, str]) -> FetchResult | None:
    for endpoint in _CEPEA_ENDPOINTS:
        idx = _endpoint_index(endpoint)

        if _is_circuit_open(endpoint):
            logger.debug(
                "cepea_endpoint_circuit_open",
                endpoint_index=idx,
                produto=produto,
            )
            continue

        url = _get_produto_url(produto, endpoint)
        try:
            result = await _fetch_with_httpx(url, headers)
            logger.info("cepea_fetch_ok", source="cepea", produto=produto)
            return result
        except (httpx.HTTPError, httpx.HTTPStatusError, SourceUnavailableError) as e:
            error_str = str(e)
            is_cloudflare = "403" in error_str or "cloudflare" in error_str.lower()
            if is_cloudflare:
                _open_circuit(endpoint)
            logger.debug(
                "cepea_endpoint_failed",
                endpoint_index=idx,
                error=error_str,
                circuit_opened=is_cloudflare,
            )

    return None


async def _fetch_with_browser(produto: str) -> FetchResult:
    from agrobr.http.browser import fetch_cepea_indicador

    logger.info("browser_fallback", source="cepea", produto=produto)
    html = await fetch_cepea_indicador(produto)
    return FetchResult(html=html, source="browser")


async def _fetch_with_alternative_source(produto: str) -> FetchResult:
    from agrobr.noticias_agricolas.client import fetch_indicador_page as na_fetch

    logger.info(
        "alternative_source_fallback",
        source="cepea",
        alternative="noticias_agricolas",
        produto=produto,
    )
    html = await na_fetch(produto)
    return FetchResult(html=html, source="noticias_agricolas")


def can_use_alternative_source(produto: str) -> bool:
    return (
        _use_alternative_source
        and produto.lower() != "leite"
        and produto.lower() in constants.NOTICIAS_AGRICOLAS_PRODUTOS
    )


async def fetch_serie(pagina: str, identificador: str) -> SerieBaixada:
    """Planilha XLS da série histórica, com os cabeçalhos e o circuito da página do indicador."""
    headers = UserAgentRotator.get_headers(source="cepea")
    ultimo_erro = "circuito aberto em todos os endereços"
    for endpoint in _CEPEA_ENDPOINTS:
        if _is_circuit_open(endpoint):
            continue
        url = f"{endpoint}/br/indicador/series/{pagina}.aspx?id={identificador}"
        try:
            response = await _get(url, headers)
            conteudo = response.content
            validate_download(conteudo, kinds=("xls",), source="cepea", url=url, min_size=512)
        except (httpx.HTTPError, SourceUnavailableError) as e:
            ultimo_erro = str(e)
            if "403" in ultimo_erro:
                _open_circuit(endpoint)
            logger.debug(
                "cepea_serie_falhou", endpoint_index=_endpoint_index(endpoint), error=ultimo_erro
            )
            continue
        if len(conteudo) > constants.CEPEA_SERIE_MAX_BYTES:
            ultimo_erro = f"série com {len(conteudo)} bytes, acima do teto de {constants.CEPEA_SERIE_MAX_BYTES}"
            continue
        logger.info("cepea_serie_ok", pagina=pagina, id=identificador, bytes=len(conteudo))
        return SerieBaixada(conteudo, url, utcnow())
    raise SourceUnavailableError(
        source="cepea",
        url=f"{_CEPEA_ENDPOINTS[0]}/br/indicador/series/{pagina}.aspx?id={identificador}",
        last_error=f"série histórica indisponível: {ultimo_erro}",
        attempted_sources=["cepea"],
    )


async def fetch_indicador_page(
    produto: str,
    force_browser: bool = False,
    force_alternative: bool = False,
) -> FetchResult:
    if force_alternative:
        if produto.lower() == "leite":
            raise SourceUnavailableError(
                source="cepea",
                last_error="Leite NA usa data de publicação incompatível com o mês de referência CEPEA",
            )
        return await _fetch_with_alternative_source(produto)

    headers = UserAgentRotator.get_headers(source="cepea")

    logger.info("http_request", source="cepea", produto=produto, method="GET")

    last_error: str = ""
    attempted_sources = ["cepea"]

    if not force_browser:
        result = await _try_endpoints(produto, headers)
        if result is not None:
            return result
        last_error = "all endpoints failed"

    if _use_browser:
        try:
            return await _fetch_with_browser(produto)
        except Exception as e:
            last_error = str(e)
            logger.warning(
                "browser_failed",
                source="cepea",
                error=last_error,
            )

    if can_use_alternative_source(produto):
        attempted_sources.append("noticias_agricolas")
        try:
            return await _fetch_with_alternative_source(produto)
        except Exception as e:
            last_error = str(e)
            logger.warning(
                "alternative_source_failed",
                source="cepea",
                alternative="noticias_agricolas",
                error=last_error,
            )

    logger.error(
        "all_methods_failed",
        source="cepea",
        produto=produto,
        last_error=last_error,
    )
    raise SourceUnavailableError(
        source="cepea",
        url=_get_produto_url(produto, _CEPEA_ENDPOINTS[0]),
        last_error=f"All fetch methods failed. Last error: {last_error}",
        attempted_sources=attempted_sources,
    )
