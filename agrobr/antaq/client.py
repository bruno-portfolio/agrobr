from __future__ import annotations

import asyncio
import io
import zipfile

import requests
import structlog

from agrobr.constants import MIN_ZIP_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_async, should_retry_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BULK_TXT_BASE = URLS[Fonte.ANTAQ]["bulk_txt"]

ANTAQ_TIMEOUT = 180.0


class _RetriableHTTPError(requests.exceptions.HTTPError):
    pass


def _get_sync(url: str) -> bytes:
    """Baixa via requests: o WAF da ANTAQ rejeita o fingerprint do httpx (HTTP 403)."""
    response = requests.get(
        url,
        timeout=ANTAQ_TIMEOUT,
        headers=UserAgentRotator.get_headers(source="antaq"),
        allow_redirects=True,
    )
    if should_retry_status(response.status_code):
        raise _RetriableHTTPError(f"Retriable status: {response.status_code}")
    response.raise_for_status()
    return response.content


async def _download_zip(url: str) -> bytes:
    logger.debug("antaq_download_zip", url=url)

    try:
        content = await retry_async(
            lambda: asyncio.to_thread(_get_sync, url),
            retriable_exceptions=(
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                _RetriableHTTPError,
                TimeoutError,
            ),
        )
    except requests.exceptions.RequestException as e:
        raise SourceUnavailableError(
            source="antaq",
            url=url,
            last_error=f"{type(e).__name__}: {e}",
        ) from e

    if len(content) < MIN_ZIP_SIZE:
        raise SourceUnavailableError(
            source="antaq",
            url=url,
            last_error=(
                f"Downloaded ZIP too small ({len(content)} bytes), expected a valid ZIP archive"
            ),
        )

    if not content.startswith(b"PK\x03\x04"):
        raise SourceUnavailableError(
            source="antaq",
            url=url,
            last_error=(
                f"Downloaded content is not a ZIP ({len(content)} bytes, missing PK signature) "
                "— likely a WAF/Cloudflare challenge page"
            ),
        )

    logger.info(
        "antaq_download_ok",
        source="antaq",
        size_bytes=len(content),
    )
    return content


def _extract_txt_from_zip(zip_bytes: bytes, filename: str) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf, zf.open(filename) as f:
        return f.read().decode("utf-8-sig")


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
