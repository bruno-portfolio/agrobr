from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from agrobr import _log, constants
from agrobr.constants import MIN_CSV_SIZE, MIN_XLSX_SIZE
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from . import _catalog

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=180.0)


@dataclass(frozen=True)
class PrecosResource:
    content: bytes
    requested_url: str
    url: str
    fetched_at: datetime
    etag: str | None = None
    last_modified: str | None = None

    def receipt(self) -> dict[str, Any]:
        return {
            "requested_url": self.requested_url,
            "url": self.url,
            "sha256": hashlib.sha256(self.content).hexdigest(),
            "bytes": len(self.content),
            "fetched_at": self.fetched_at.isoformat(),
            "etag": self.etag,
            "last_modified": self.last_modified,
        }


async def fetch_precos_resource(url: str) -> PrecosResource:
    logger.debug("anp_diesel_download", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="anp_diesel"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anp_diesel",
        )

        responses.raise_for_status(response, source="anp_diesel")

        content = response.content
        io_utils.validate_download(
            content,
            kinds=("xlsx",),
            source="anp_diesel",
            url=url,
            min_size=MIN_XLSX_SIZE,
        )

        logger.info(
            "anp_diesel_download_ok",
            source="anp_diesel",
            size_bytes=len(content),
        )
        return PrecosResource(
            content=content,
            requested_url=url,
            url=str(response.url),
            fetched_at=datetime.now(UTC),
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
        )


async def download_csv(url: str) -> bytes:
    logger.debug("anp_diesel_download_csv", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="anp_diesel"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anp_diesel",
        )

        responses.raise_for_status(response, source="anp_diesel")

        content = response.content
        io_utils.validate_download(
            content,
            kinds=("csv",),
            source="anp_diesel",
            url=url,
            min_size=MIN_CSV_SIZE,
        )

        logger.info(
            "anp_diesel_download_csv_ok",
            source="anp_diesel",
            size_bytes=len(content),
        )
        return content


async def fetch_vendas_m3() -> bytes:
    from agrobr.alt.anp_diesel.models import VENDAS_DIESEL_CSV_URL

    return await download_csv(VENDAS_DIESEL_CSV_URL)


async def fetch_precos_catalog() -> dict[str, str]:
    url = constants.URLS[constants.Fonte.ANP_DIESEL]["precos_catalogo"]
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="anp_diesel"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(lambda: client.get(url), source="anp_diesel")
        responses.raise_for_status(response, source="anp_diesel")
        return _catalog.parse_municipal_catalog(response.text)
