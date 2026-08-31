from __future__ import annotations

import asyncio
import io
import zipfile
from typing import NamedTuple

import requests
import structlog

from agrobr.constants import MIN_ZIP_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_async, should_retry_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BULK_TXT_BASE = URLS[Fonte.ANTAQ]["bulk_txt"]

ANTAQ_TIMEOUT = 180.0
OUTAGE_NOTICE_SLUG = "painel-estatistico-aquaviario-indisponivel"


class _RetriableHTTPError(requests.exceptions.HTTPError):
    pass


class _Download(NamedTuple):
    content: bytes
    content_type: str
    final_url: str


def _get_sync(url: str) -> _Download:
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
    return _Download(
        content=response.content,
        content_type=response.headers.get("Content-Type", "?"),
        final_url=response.url,
    )


def _rejection_reason(download: _Download, problem: str) -> str:
    detail = (
        f"{problem}: {len(download.content)} bytes of "
        f"{download.content_type} from {download.final_url}"
    )
    if OUTAGE_NOTICE_SLUG in download.final_url:
        return (
            f"ANTAQ redirected to the official outage notice ({detail}). "
            "The Estatistico Aquaviario has been offline since 2026-06-23."
        )
    return detail


async def _download_zip(url: str) -> bytes:
    logger.debug("antaq_download_zip", url=url)

    try:
        download = await retry_async(
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

    content = download.content

    if not content.startswith(b"PK\x03\x04"):
        raise SourceUnavailableError(
            source="antaq",
            url=url,
            last_error=_rejection_reason(download, "not a ZIP (missing PK signature)"),
        )

    if len(content) < MIN_ZIP_SIZE:
        raise SourceUnavailableError(
            source="antaq",
            url=url,
            last_error=_rejection_reason(download, "ZIP too small"),
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
