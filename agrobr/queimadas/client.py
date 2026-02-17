from __future__ import annotations

import io
import zipfile

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv"

ANUAL_URL = f"{BASE_URL}/anual/Brasil_todos_sats"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)"}


async def _try_fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    logger.debug("queimadas_request", url=url)
    response = await retry_on_status(
        lambda: client.get(url),
        source="queimadas",
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content


def _extract_csv_from_zip(data: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise SourceUnavailableError(
                source="queimadas",
                url="(zip)",
                last_error="ZIP nao contem arquivo CSV",
            )
        return zf.read(csv_names[0])


async def fetch_focos_diario(data: str) -> tuple[bytes, str]:
    """Baixa CSV de focos diarios.

    Args:
        data: Data no formato YYYYMMDD.

    Returns:
        Tupla (bytes_csv, url_usada).

    Raises:
        SourceUnavailableError: Se CSV nao encontrado.
    """
    url = f"{BASE_URL}/diario/Brasil/focos_diario_br_{data}.csv"
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as c:
        content = await _try_fetch(c, url)
    if content is None:
        raise SourceUnavailableError(source="queimadas", url=url, last_error="HTTP 404")
    logger.info("queimadas_csv_found", url=url, size=len(content))
    return content, url


async def fetch_focos_mensal(ano: int, mes: int) -> tuple[bytes, str]:
    """Baixa CSV de focos mensais com fallback em cascata.

    Estrategia:
        1. .csv mensal (disponivel para 2024+)
        2. .zip mensal (disponivel para 2023)
        3. .zip anual completo + filtro por mes (2003-2022)

    Args:
        ano: Ano (ex: 2024).
        mes: Mes (1-12).

    Returns:
        Tupla (bytes_csv, url_usada).

    Raises:
        SourceUnavailableError: Se nenhuma fonte disponivel.
    """
    periodo = f"{ano:04d}{mes:02d}"
    csv_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.csv"
    zip_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.zip"
    anual_url = f"{ANUAL_URL}/focos_br_todos-sats_{ano:04d}.zip"

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as c:
        content = await _try_fetch(c, csv_url)
        if content is not None:
            logger.info("queimadas_csv_found", url=csv_url, size=len(content))
            return content, csv_url

        logger.debug("queimadas_csv_404_trying_zip", periodo=periodo)
        content = await _try_fetch(c, zip_url)
        if content is not None:
            csv_bytes = _extract_csv_from_zip(content)
            logger.info("queimadas_zip_found", url=zip_url, size=len(csv_bytes))
            return csv_bytes, zip_url

        logger.debug("queimadas_zip_404_trying_anual", ano=ano)
        content = await _try_fetch(c, anual_url)
        if content is not None:
            csv_bytes = _extract_csv_from_zip(content)
            logger.info(
                "queimadas_anual_found",
                url=anual_url,
                size_raw=len(csv_bytes),
                filtering_month=mes,
            )
            return csv_bytes, anual_url

    raise SourceUnavailableError(
        source="queimadas",
        url=csv_url,
        last_error=(
            f"Focos mensal {periodo} nao encontrado. "
            f"Tentativas: .csv, .zip mensal, .zip anual ({ano})"
        ),
    )
