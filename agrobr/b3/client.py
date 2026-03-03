from __future__ import annotations

from datetime import datetime

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE, MIN_ZIP_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL_ZIP = URLS[Fonte.B3]["ajustes_zip"]
BASE_URL_ARQUIVOS = URLS[Fonte.B3]["arquivos"]

TIMEOUT = get_timeout()
TIMEOUT_DOWNLOAD = get_timeout(read=120.0)


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

        response.raise_for_status()
        content = response.content

        if len(content) < MIN_ZIP_SIZE:
            raise SourceUnavailableError(
                source="b3",
                url=url,
                last_error=f"ZIP too small ({len(content)} bytes)",
            )

        logger.info("b3_zip_fetch_ok", url=url, size=len(content))
        return content, url


async def fetch_posicoes_abertas(data: str) -> tuple[bytes, str]:
    token_url = (
        f"{BASE_URL_ARQUIVOS}/requestname"
        f"?fileName=DerivativesOpenPosition&date={data}&recaptchaToken="
    )
    async with httpx.AsyncClient(
        timeout=TIMEOUT_DOWNLOAD, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        logger.debug("b3_oi_token_request", url=token_url)
        token_resp = await retry_on_status(
            lambda: http.get(token_url),
            source="b3",
        )

        if token_resp.status_code in (400, 404):
            raise SourceUnavailableError(
                source="b3", url=token_url, last_error=f"HTTP {token_resp.status_code}"
            )
        token_resp.raise_for_status()

        token_data = token_resp.json()
        token = token_data.get("token")
        if not token:
            raise SourceUnavailableError(
                source="b3", url=token_url, last_error="Token vazio na resposta"
            )

        download_url = f"{BASE_URL_ARQUIVOS}?token={token}"
        logger.debug("b3_oi_download", url=download_url)
        csv_resp = await retry_on_status(
            lambda: http.get(download_url),
            source="b3",
        )

        if csv_resp.status_code in (400, 404):
            raise SourceUnavailableError(
                source="b3", url=download_url, last_error=f"HTTP {csv_resp.status_code}"
            )
        csv_resp.raise_for_status()

        csv_bytes = csv_resp.content
        if len(csv_bytes) < MIN_CSV_SIZE:
            raise SourceUnavailableError(
                source="b3",
                url=download_url,
                last_error=(
                    f"Posicoes abertas CSV too small ({len(csv_bytes)} bytes), "
                    f"expected derivative position data"
                ),
            )

        logger.info("b3_oi_fetch_ok", url=download_url, size=len(csv_bytes))
        return csv_bytes, token_url
