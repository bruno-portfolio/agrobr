from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from .models import CSV_URL, MIN_CSV_BYTES

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=300.0)


async def fetch_embargos_csv() -> tuple[bytes, str]:
    """Baixa o CSV completo de termos de embargo do IBAMA (~208 MB sem compressão,
    com geometrias WKT); a fonte não publica versão compactada."""
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        logger.info("ibama_embargos_request", url=CSV_URL)
        response = await retry_on_status(
            lambda: http.get(CSV_URL),
            source="ibama",
        )
        responses.raise_for_status(response, source="ibama")
        content = response.content

    io_utils.validate_download(
        content,
        kinds=("csv",),
        source="ibama",
        url=CSV_URL,
        min_size=MIN_CSV_SIZE,
    )

    if len(content) < MIN_CSV_BYTES:
        raise SourceUnavailableError(
            source="ibama",
            url=CSV_URL,
            last_error=(
                f"CSV de embargos com {len(content)} bytes "
                f"(esperado >= {MIN_CSV_BYTES}) — possível truncamento na fonte"
            ),
        )

    logger.info("ibama_embargos_csv_ok", csv_bytes=len(content))
    return content, CSV_URL
