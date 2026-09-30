from __future__ import annotations

import httpx

from agrobr import _log
from agrobr.constants import MIN_WFS_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.io import extract_csv_from_zip

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.QUEIMADAS]["dados_abertos"]

ANUAL_URL = f"{BASE_URL}/anual/Brasil_todos_sats"

TIMEOUT = get_timeout(read=120.0)


async def _try_fetch(
    client: httpx.AsyncClient, url: str, vazios: list[str]
) -> httpx.Response | None:
    logger.debug("queimadas_request", url=url)
    response = await retry_on_status(
        lambda: client.get(url),
        source="queimadas",
    )
    if response.status_code == 404:
        return None
    responses.raise_for_status(response, source="queimadas")

    content = response.content
    if len(content) < MIN_WFS_SIZE:
        logger.debug("queimadas_response_too_small_detail", url=url)
        logger.warning(
            "queimadas_response_too_small",
            source="queimadas",
            size=len(content),
        )
        vazios.append(
            f"{url} respondeu HTTP {response.status_code} com corpo vazio ({len(content)} bytes)"
        )
        return None
    return response


async def fetch_focos_diario(data: str) -> tuple[bytes, str, str | None]:
    """Devolve o CSV, a URL e o `Last-Modified` da resposta."""
    url = f"{BASE_URL}/diario/Brasil/focos_diario_br_{data}.csv"
    vazios: list[str] = []
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as c:
        resposta = await _try_fetch(c, url, vazios)
    if resposta is None:
        raise SourceUnavailableError(
            source="queimadas",
            url=url,
            last_error=(
                f"{vazios[0]}: o servidor respondeu, mas sem o arquivo"
                if vazios
                else "HTTP 404: o arquivo diário só existe para os últimos dias; "
                "remova o parâmetro dia para usar o arquivo mensal"
            ),
        )
    content = resposta.content
    logger.info("queimadas_csv_found", source="queimadas", size=len(content))
    return content, url, resposta.headers.get("last-modified")


async def fetch_focos_mensal(ano: int, mes: int) -> tuple[bytes, str, bytes, str | None]:
    """Devolve o CSV, a URL, o corpo recebido e o `Last-Modified` da resposta."""
    periodo = f"{ano:04d}{mes:02d}"
    csv_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.csv"
    zip_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.zip"
    anual_url = f"{ANUAL_URL}/focos_br_todos-sats_{ano:04d}.zip"
    vazios: list[str] = []

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as c:
        resposta = await _try_fetch(c, csv_url, vazios)
        if resposta is not None:
            logger.info("queimadas_csv_found", source="queimadas", size=len(resposta.content))
            return (
                resposta.content,
                csv_url,
                resposta.content,
                resposta.headers.get("last-modified"),
            )

        logger.debug("queimadas_csv_404_trying_zip", periodo=periodo)
        resposta = await _try_fetch(c, zip_url, vazios)
        if resposta is not None:
            csv_bytes = extract_csv_from_zip(resposta.content, source="queimadas", url=zip_url)
            logger.info("queimadas_zip_found", source="queimadas", size=len(csv_bytes))
            return csv_bytes, zip_url, resposta.content, resposta.headers.get("last-modified")

        logger.debug("queimadas_zip_404_trying_anual", ano=ano)
        resposta = await _try_fetch(c, anual_url, vazios)
        if resposta is not None:
            csv_bytes = extract_csv_from_zip(resposta.content, source="queimadas", url=anual_url)
            logger.info(
                "queimadas_anual_found",
                source="queimadas",
                size_raw=len(csv_bytes),
                filtering_month=mes,
            )
            return csv_bytes, anual_url, resposta.content, resposta.headers.get("last-modified")

    raise SourceUnavailableError(
        source="queimadas",
        url=csv_url,
        last_error=(
            f"Focos mensal {periodo}: {'; '.join(vazios)}; o servidor respondeu, mas sem o arquivo"
            if vazios
            else f"Focos mensal {periodo} nao encontrado. "
            f"Tentativas: .csv, .zip mensal, .zip anual ({ano})"
        ),
    )
