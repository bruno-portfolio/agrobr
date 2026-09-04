from __future__ import annotations

import httpx
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from .models import MIN_PDF_SIZE, SAFRAS_URLS

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=60.0)


async def fetch_ensaio_soja(safra: str) -> tuple[bytes, str]:
    url = SAFRAS_URLS.get(safra)
    if url is None:
        raise InvalidParameterError(
            f"Safra '{safra}' não disponível. Safras: {list(SAFRAS_URLS.keys())}"
        )

    logger.debug("rio_verde_fetch", safra=safra, url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="rio_verde"),
        follow_redirects=True,
    ) as http:
        response = await retry_on_status(
            lambda: http.get(url),
            source="rio_verde",
        )
        response.raise_for_status()
        content = response.content

        io_utils.validate_download(
            content,
            kinds=("pdf",),
            source="rio_verde",
            url=url,
            min_size=MIN_PDF_SIZE,
        )

    logger.info("rio_verde_fetch_ok", safra=safra, size_bytes=len(content))
    return content, url
