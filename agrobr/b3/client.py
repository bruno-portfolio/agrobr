from __future__ import annotations

import asyncio
import threading
from datetime import date, datetime

import httpx

from agrobr import _log
from agrobr.constants import MIN_CSV_SIZE, MIN_ZIP_SIZE, URLS, Fonte
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = _log.get_logger(__name__)

BASE_URL_ZIP = URLS[Fonte.B3]["ajustes_zip"]
BASE_URL_ARQUIVOS = URLS[Fonte.B3]["arquivos"]

TIMEOUT = get_timeout()
TIMEOUT_DOWNLOAD = get_timeout(read=120.0)
_OI_LOCK = threading.Lock()


def validate_oi_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError(value)
        return parsed
    except (TypeError, ValueError) as exc:
        raise InvalidParameterError("data deve ser uma data válida no formato AAAA-MM-DD") from exc


async def fetch_ajustes_zip(data: str) -> tuple[bytes, str]:
    dt = datetime.strptime(data, "%d/%m/%Y")
    filename = f"PR{dt.strftime('%y%m%d')}.zip"
    url = f"{BASE_URL_ZIP}?filelist={filename}"

    async with httpx.AsyncClient(
        timeout=TIMEOUT_DOWNLOAD,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        logger.debug("b3_zip_request", url=url)
        response = await retry_on_status(
            lambda: http.get(url),
            source="b3",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="b3", url=url, last_error="HTTP 404")

        responses.raise_for_status(response, source="b3")
        content = response.content

        if len(content) <= 100:
            raise SourceUnavailableError(
                source="b3",
                url=url,
                last_error=f"ZIP vazio ({len(content)} bytes) — pregão de {data} ainda não publicado",
            )
        io_utils.validate_download(
            content,
            kinds=("zip",),
            source="b3",
            url=url,
            min_size=MIN_ZIP_SIZE,
        )

        logger.info("b3_zip_fetch_ok", source="b3", size=len(content))
        return content, url


async def fetch_posicoes_abertas(data: str) -> tuple[bytes, str]:
    validate_oi_date(data)
    while not _OI_LOCK.acquire(blocking=False):
        await asyncio.sleep(0.05)
    try:
        return await _fetch_posicoes_abertas(data)
    finally:
        _OI_LOCK.release()


def ticket_url(data: str) -> str:
    return (
        f"{BASE_URL_ARQUIVOS}/requestname"
        f"?fileName=DerivativesOpenPosition&date={data}&recaptchaToken="
    )


async def _fetch_posicoes_abertas(data: str) -> tuple[bytes, str]:
    token_url = ticket_url(data)
    async with httpx.AsyncClient(
        timeout=TIMEOUT_DOWNLOAD, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        logger.debug("b3_oi_token_request", url=token_url)
        token_resp = await retry_on_status(
            lambda: http.get(token_url),
            source="b3_arquivos",
        )

        if token_resp.status_code in (400, 404):
            logger.info("b3_oi_nao_publicado", data=data, status=token_resp.status_code)
            return b"", token_url
        responses.raise_for_status(token_resp, source="b3")

        token_data = responses.parse_json_response(token_resp, source="b3", url=token_url)
        token = token_data.get("token")
        if not token:
            raise SourceUnavailableError(
                source="b3", url=token_url, last_error="Token vazio na resposta"
            )

        download_url = f"{BASE_URL_ARQUIVOS}?token={token}"
        logger.debug("b3_oi_download", url=BASE_URL_ARQUIVOS)
        csv_resp = await retry_on_status(
            lambda: http.get(download_url),
            source="b3_arquivos",
        )

        if csv_resp.status_code == 404:
            logger.info("b3_oi_nao_publicado", data=data, status=404)
            return b"", token_url
        if csv_resp.status_code == 400:
            raise SourceUnavailableError(
                source="b3", url=BASE_URL_ARQUIVOS, last_error=f"HTTP 400: {csv_resp.text[:500]}"
            )
        responses.raise_for_status(csv_resp, source="b3")

        csv_bytes = csv_resp.content
        if len(csv_bytes) < MIN_CSV_SIZE:
            raise SourceUnavailableError(
                source="b3",
                url=BASE_URL_ARQUIVOS,
                last_error=(
                    f"Posicoes abertas CSV too small ({len(csv_bytes)} bytes), "
                    f"expected derivative position data"
                ),
            )

        logger.info("b3_oi_fetch_ok", source="b3", size=len(csv_bytes))
        return csv_bytes, str(csv_resp.url).replace(token, "[REDACTED]")
