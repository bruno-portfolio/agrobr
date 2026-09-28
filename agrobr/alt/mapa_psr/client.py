from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import IO

import httpx
import structlog

from agrobr import constants
from agrobr.constants import MIN_CSV_SIZE
from agrobr.exceptions import ResourceLimitError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=180.0)


async def _download_to_file(url: str, target: IO[bytes]) -> None:
    transferred = 0
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="mapa_psr"),
        follow_redirects=True,
    ) as client:

        async def attempt() -> httpx.Response:
            nonlocal transferred
            target.seek(0)
            target.truncate()
            async with client.stream("GET", url) as response:
                if response.is_success:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        transferred += len(chunk)
                        if (
                            target.tell() + len(chunk) > constants.MAPA_PSR_MAX_DOWNLOAD_BYTES
                            or transferred > constants.MAPA_PSR_MAX_TRANSFER_BYTES
                        ):
                            raise ResourceLimitError(
                                "mapa_psr", "Download excede o orçamento de bytes", url=url
                            )
                        target.write(chunk)
                return response

        response = await retry_on_status(attempt, source="mapa_psr")
        responses.raise_for_status(response, source="mapa_psr")
    size = target.tell()
    target.seek(0)
    io_utils.validate_download(
        target.read(max(512, MIN_CSV_SIZE)),
        kinds=("csv",),
        source="mapa_psr",
        url=url,
        min_size=MIN_CSV_SIZE,
    )
    target.seek(0)
    logger.info("mapa_psr_download_ok", source="mapa_psr", size_bytes=size)


async def fetch_catalogo() -> bytes:
    from agrobr.alt.mapa_psr.models import CATALOGO_URL

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="mapa_psr"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(lambda: client.get(CATALOGO_URL), source="mapa_psr")
        responses.raise_for_status(response, source="mapa_psr")
        return response.content


@asynccontextmanager
async def open_url(url: str) -> AsyncIterator[IO[bytes]]:
    with tempfile.TemporaryFile(mode="w+b", prefix="agrobr-psr-") as target:
        await _download_to_file(url, target)
        yield target


@asynccontextmanager
async def open_periodo(periodo: str) -> AsyncIterator[IO[bytes]]:
    from agrobr.alt.mapa_psr.models import get_csv_url

    async with open_url(get_csv_url(periodo)) as target:
        yield target
