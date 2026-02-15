from __future__ import annotations

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=_settings.timeout_read,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)"}


async def fetch_ajustes(data: str) -> tuple[str, str]:
    url = f"{BASE_URL}?txtData={data}"
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=HEADERS, follow_redirects=True, verify=False
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
        logger.info("b3_fetch_ok", url=url, size=len(html))
        return html, url
