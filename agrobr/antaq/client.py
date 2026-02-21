from __future__ import annotations

import io
import zipfile

import httpx
import structlog

from agrobr.constants import MIN_ZIP_SIZE, URLS, Fonte, HTTPSettings
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BULK_TXT_BASE = URLS[Fonte.ANTAQ]["bulk_txt"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def _download_zip(url: str) -> bytes:
    logger.info("antaq_download_zip", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_headers(source="antaq"), follow_redirects=True
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antaq",
        )
        response.raise_for_status()

        content = response.content
        if len(content) < MIN_ZIP_SIZE:
            from agrobr.exceptions import SourceUnavailableError

            raise SourceUnavailableError(
                source="antaq",
                url=url,
                last_error=(
                    f"Downloaded ZIP too small ({len(content)} bytes), expected a valid ZIP archive"
                ),
            )

        logger.info(
            "antaq_download_ok",
            url=url,
            size_bytes=len(content),
        )
        return content


def _extract_txt_from_zip(zip_bytes: bytes, filename: str) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf, zf.open(filename) as f:
        return f.read().decode("utf-8-sig")


def list_zip_contents(zip_bytes: bytes) -> list[str]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return zf.namelist()


async def fetch_ano_zip(ano: int) -> bytes:
    url = f"{BULK_TXT_BASE}/{ano}.zip"
    return await _download_zip(url)


async def fetch_mercadoria_zip() -> bytes:
    url = f"{BULK_TXT_BASE}/Mercadoria.zip"
    return await _download_zip(url)


def extract_atracacao(zip_bytes: bytes, ano: int) -> str:
    return _extract_txt_from_zip(zip_bytes, f"{ano}Atracacao.txt")


def extract_carga(zip_bytes: bytes, ano: int) -> str:
    return _extract_txt_from_zip(zip_bytes, f"{ano}Carga.txt")


def extract_mercadoria(zip_bytes: bytes) -> str:
    return _extract_txt_from_zip(zip_bytes, "Mercadoria.txt")
