from __future__ import annotations

import httpx
import structlog

from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from .models import DOWNLOAD_URL

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=60.0)
MIN_PDF_SIZE = 5_000


async def fetch_empregadores() -> tuple[bytes, str]:
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        response = await retry_on_status(
            lambda: http.get(DOWNLOAD_URL),
            source="lista_suja",
        )
        response.raise_for_status()
        content = response.content

        io_utils.validate_download(
            content,
            kinds=("pdf",),
            source="lista_suja",
            url=DOWNLOAD_URL,
            min_size=MIN_PDF_SIZE,
        )

        logger.info("lista_suja_download_ok", size=len(content))
        return content, DOWNLOAD_URL
