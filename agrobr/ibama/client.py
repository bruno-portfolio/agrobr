from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_ZIP_SIZE
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils
from agrobr.utils.io import extract_csv_from_zip

from .models import MIN_CSV_BYTES, ZIP_URL

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=300.0)


async def fetch_embargos_zip() -> tuple[bytes, str]:
    """Baixa o dump completo de termos de embargo do SIFISC (~47 MB zipado,
    ~170 MB de CSV com geometrias WKT) e retorna os bytes do CSV extraído."""
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        logger.info("ibama_embargos_request", url=ZIP_URL)
        response = await retry_on_status(
            lambda: http.get(ZIP_URL),
            source="ibama",
        )
        response.raise_for_status()
        content = response.content

    io_utils.validate_download(
        content,
        kinds=("zip",),
        source="ibama",
        url=ZIP_URL,
        min_size=MIN_ZIP_SIZE,
    )

    csv_bytes = extract_csv_from_zip(content, source="ibama", url=ZIP_URL)
    if len(csv_bytes) < MIN_CSV_BYTES:
        raise SourceUnavailableError(
            source="ibama",
            url=ZIP_URL,
            last_error=(
                f"CSV de embargos com {len(csv_bytes)} bytes "
                f"(esperado >= {MIN_CSV_BYTES}) — possível truncamento na fonte"
            ),
        )

    logger.info("ibama_embargos_zip_ok", zip_bytes=len(content), csv_bytes=len(csv_bytes))
    return csv_bytes, ZIP_URL
