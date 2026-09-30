from __future__ import annotations

import httpx

from agrobr import _log
from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.DERAL]["downloads"]

TIMEOUT = get_timeout()


async def _fetch_bytes(url: str) -> bytes:
    headers = UserAgentRotator.get_bot_headers()
    headers["Accept"] = (
        "application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*"
    )

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
        logger.debug("deral_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="deral",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(
                source="deral",
                url=url,
                last_error="HTTP 404: arquivo não encontrado",
            )

        responses.raise_for_status(response, source="deral")

        content = response.content
        io_utils.validate_download(
            content,
            kinds=("xls",),
            source="deral",
            url=url,
            min_size=MIN_XLSX_SIZE,
        )
        return content


async def fetch_pc_xls() -> bytes:
    url = f"{BASE_URL}/PC.xls"
    logger.debug("deral_fetch_pc", url=url)
    return await _fetch_bytes(url)
