from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE, MIN_HTML_SIZE, URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.B3]["ajustes"]
BASE_URL_ARQUIVOS = URLS[Fonte.B3]["arquivos"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=_settings.timeout_read,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

TIMEOUT_DOWNLOAD = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def fetch_ajustes(data: str) -> tuple[str, str]:
    url = f"{BASE_URL}?txtData={data}"
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
        verify=False,
    ) as http:
        logger.debug("b3_request", url=url)
        response = await retry_on_status(
            lambda: http.get(BASE_URL, params={"txtData": data}),
            source="b3",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="b3", url=url, last_error="HTTP 404")

        response.raise_for_status()
        html = response.content.decode("iso-8859-1")

        if len(html) < MIN_HTML_SIZE or "<table" not in html.lower():
            raise SourceUnavailableError(
                source="b3",
                url=url,
                last_error=(
                    f"Ajustes HTML too small or missing table "
                    f"({len(html)} chars, no '<table' found)"
                ),
            )

        logger.info("b3_fetch_ok", url=url, size=len(html))
        return html, url


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
