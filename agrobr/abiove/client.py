from __future__ import annotations

import httpx

from agrobr import _log
from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.time import utcnow

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.ABIOVE]["exportacao"]

TIMEOUT = get_timeout(read=60.0)

_NAO_ENCONTRADO = "HTTP 404"


async def _fetch_url(url: str) -> bytes:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("abiove_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="abiove",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="abiove", url=url, last_error=_NAO_ENCONTRADO)

        responses.raise_for_status(response, source="abiove")

        content = response.content
        if len(content) < MIN_XLSX_SIZE:
            raise SourceUnavailableError(
                source="abiove",
                url=url,
                last_error=(
                    f"Downloaded file too small ({len(content)} bytes), expected a valid XLSX"
                ),
            )
        return content


def edicoes_candidatas(ano: int) -> list[str]:
    hoje = utcnow()
    return [
        f"{ano_edicao:04d}-{mes_edicao:02d}"
        for ano_edicao in (ano + 1, ano)
        for mes_edicao in range(12, 0, -1)
        if (ano_edicao, mes_edicao) <= (hoje.year, hoje.month)
    ]


def edicao_url(edicao: str) -> str:
    return f"{BASE_URL}/exp_{edicao.replace('-', '')}.xlsx"


async def fetch_exportacao_excel(ano: int, edicao: str | None = None) -> tuple[bytes, str, str]:
    if edicao is not None:
        url = edicao_url(edicao)
        return await _fetch_url(url), url, edicao

    for candidata in edicoes_candidatas(ano):
        url = edicao_url(candidata)
        try:
            data = await _fetch_url(url)
        except SourceUnavailableError as e:
            if e.last_error != _NAO_ENCONTRADO:
                raise
            logger.debug("abiove_edicao_nao_publicada", edicao=candidata)
            continue
        logger.info("abiove_excel_found", source="abiove", edicao=candidata, size=len(data))
        return data, url, candidata

    raise SourceUnavailableError(
        source="abiove",
        url=f"{BASE_URL}/exp_*.xlsx",
        last_error=f"HTTP 404: nenhuma edição publicada com dados de {ano}",
    )
