from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, TypeVar
from urllib.parse import quote
from weakref import WeakValueDictionary

import httpx
import structlog

from agrobr import constants
from agrobr.constants import MIN_ZIP_SIZE, CacheSettings
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import RetriableStatusError, retry_async, should_retry_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import atomic
from agrobr.utils.warnings import warn_once

from .models import BASE_URL, FILENAME_PATTERNS

logger = structlog.get_logger()

_CHUNK_SIZE = 64 * 1024

TIMEOUT = get_timeout(read=120.0)

_FETCH_LOCKS: WeakValueDictionary[tuple[asyncio.AbstractEventLoop, str], asyncio.Lock] = (
    WeakValueDictionary()
)
_LOCKS_GUARD = threading.Lock()


class Aquisicao(NamedTuple):
    zip_path: Path
    from_cache: bool
    fetched_at: datetime
    source_details: dict[str, str]
    sha256: str | None = None
    size_bytes: int = 0


def _cache_disabled() -> bool:
    return os.environ.get("AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED") == "1"


async def _get_lock(key: str) -> asyncio.Lock:
    loop_key = (asyncio.get_running_loop(), key)
    with _LOCKS_GUARD:
        lock = _FETCH_LOCKS.get(loop_key)
        if lock is None:
            lock = asyncio.Lock()
            _FETCH_LOCKS[loop_key] = lock
        return lock


def _build_filename(tema: str, uf: str | None) -> str:
    pattern = FILENAME_PATTERNS[tema]
    return pattern.format(uf=uf) if uf else pattern


def _build_url(tema: str, uf: str | None) -> str:
    return BASE_URL + quote(_build_filename(tema, uf))


def _cache_key(tema: str, uf: str | None) -> str:
    return f"{tema}:{uf}" if uf else tema


def _cache_dir(tema: str) -> Path:
    settings = CacheSettings()
    return settings.cache_dir / "acervo_fundiario" / tema


def _zip_path(tema: str, uf: str | None) -> Path:
    name = uf if uf else "brasil"
    return _cache_dir(tema) / f"{name}.zip"


def _meta_path(tema: str, uf: str | None) -> Path:
    name = uf if uf else "brasil"
    return _cache_dir(tema) / f"{name}.json"


def _atomic_write_text(target: Path, text: str) -> None:
    with atomic.atomic_output(target) as tmp:
        tmp.write_text(text, encoding="utf-8")


def _load_meta(meta_path: Path) -> dict[str, Any] | None:
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("acervo_fundiario_cache_meta_invalid", path=str(meta_path), error=str(exc))
        return None


def _save_meta(meta_path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(meta_path, json.dumps(payload, ensure_ascii=False, indent=2))


def _validate_zip_bytes_prefix(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            head = f.read(4)
    except OSError:
        return False
    return head == b"PK\x03\x04"


def _validate_cached_zip(zip_path: Path) -> bool:
    if not zip_path.exists():
        return False
    if not _validate_zip_bytes_prefix(zip_path):
        logger.warning("acervo_fundiario_cache_zip_invalid_magic", path=str(zip_path))
        return False
    return True


def _cache_matches_remote(
    zip_path: Path, cached_meta: dict[str, Any], head_info: dict[str, str | int]
) -> bool:
    size = zip_path.stat().st_size
    if size != cached_meta.get("size_bytes"):
        return False
    remote_size = head_info["content_length"]
    if isinstance(remote_size, int) and remote_size > 0 and remote_size != size:
        return False
    matched = False
    for key in ("etag", "last_modified"):
        cached = cached_meta.get(key, "")
        remote = head_info[key]
        if cached != remote:
            return False
        matched = matched or bool(remote)
    return matched


_T = TypeVar("_T")


def _conferir_status(response: httpx.Response, url: str) -> None:
    if response.status_code == 404:
        raise SourceUnavailableError(
            source="acervo_fundiario",
            url=url,
            last_error="HTTP 404 — recurso não disponível no servidor INCRA",
        )
    if should_retry_status(response.status_code):
        raise RetriableStatusError(
            f"HTTP {response.status_code}", request=response.request, response=response
        )
    responses.raise_for_status(response, source="acervo_fundiario")


async def _com_retry(pedido: Callable[[], Awaitable[_T]], url: str) -> _T:
    """Repete o pedido nas falhas de rede e nos status transitórios, e tipa a falha final."""
    try:
        return await retry_async(pedido)
    except httpx.HTTPError as exc:
        raise SourceUnavailableError(
            source="acervo_fundiario", url=url, last_error=f"{type(exc).__name__}: {exc}"
        ) from exc


async def _head(client: httpx.AsyncClient, url: str) -> dict[str, str | int]:
    async def pedir() -> httpx.Response:
        response = await client.head(url, timeout=TIMEOUT)
        _conferir_status(response, url)
        return response

    response = await _com_retry(pedir, url)
    headers = response.headers
    last_modified = headers.get("Last-Modified", "")
    etag = headers.get("ETag", "")
    content_length = int(headers.get("Content-Length", "0"))
    return {"last_modified": last_modified, "etag": etag, "content_length": content_length}


async def _stream_download(client: httpx.AsyncClient, url: str, dst_path: Path) -> tuple[int, str]:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    sha = hashlib.sha256()
    bytes_written = 0

    async with atomic.atomic_output_async(dst_path) as tmp:
        async with client.stream("GET", url, timeout=TIMEOUT) as response:
            _conferir_status(response, url)
            with open(tmp, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=_CHUNK_SIZE):
                    if bytes_written + len(chunk) > constants.ACERVO_MAX_DOWNLOAD_BYTES:
                        raise ResourceLimitError(
                            "acervo_fundiario", "Download excede o orçamento de bytes", url=url
                        )
                    f.write(chunk)
                    sha.update(chunk)
                    bytes_written += len(chunk)
        if bytes_written < MIN_ZIP_SIZE:
            raise SourceUnavailableError(
                source="acervo_fundiario",
                url=url,
                last_error=f"Resposta muito pequena ({bytes_written} bytes), esperado ZIP >={MIN_ZIP_SIZE}",
            )
        if not _validate_zip_bytes_prefix(tmp):
            raise SourceUnavailableError(
                source="acervo_fundiario",
                url=url,
                last_error="Resposta não é um ZIP válido (magic bytes incorretas)",
            )

    return bytes_written, sha.hexdigest()


def _warn_download_size_once() -> None:
    warn_once(
        "acervo_fundiario_download_size",
        (
            "acervo_fundiario: download de shapefile estatico do INCRA. "
            "Tamanhos: SIGEF 2-766 MB por UF, SNCI 0.01-23 MB por UF, Assentamentos 50 MB. "
            "Cache em ~/.agrobr/cache/acervo_fundiario/ (opt-out: use_cache=False ou "
            "AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1)."
        ),
    )


async def download_and_cache(
    tema: str, uf: str | None = None, *, use_cache: bool = True
) -> Aquisicao:
    if tema not in FILENAME_PATTERNS:
        raise ValueError(f"tema invalido: {tema!r}. Validos: {sorted(FILENAME_PATTERNS)}")

    url = _build_url(tema, uf)
    zip_path = _zip_path(tema, uf)
    meta_path = _meta_path(tema, uf)
    cache_active = use_cache and not _cache_disabled()

    from agrobr.http.rate_limiter import RateLimiter

    lock = await _get_lock(_cache_key(tema, uf))
    async with (
        lock,
        httpx.AsyncClient(
            headers=UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
        ) as client,
    ):
        head_info: dict[str, str | int] | None = None

        if cache_active:
            cached_meta = _load_meta(meta_path)
            if cached_meta and "fetched_at" in cached_meta and _validate_cached_zip(zip_path):
                async with RateLimiter.acquire("acervo_fundiario"):
                    head_info = await _head(client, url)
                if _cache_matches_remote(zip_path, cached_meta, head_info):
                    logger.debug(
                        "acervo_fundiario_cache_hit",
                        tema=tema,
                        uf=uf,
                        last_modified=head_info["last_modified"],
                    )
                    return Aquisicao(
                        zip_path,
                        True,
                        datetime.fromisoformat(cached_meta["fetched_at"]),
                        {
                            "revalidado_em": datetime.now(UTC).isoformat(),
                            "etag": str(head_info["etag"]),
                            "last_modified": str(head_info["last_modified"]),
                        },
                        cached_meta.get("sha256"),
                        int(cached_meta.get("size_bytes", 0)),
                    )
                logger.info(
                    "acervo_fundiario_cache_stale",
                    tema=tema,
                    uf=uf,
                    cached=cached_meta.get("last_modified"),
                    remote=head_info["last_modified"],
                )

        _warn_download_size_once()
        logger.info("acervo_fundiario_download_start", tema=tema, uf=uf, url=url)
        async with RateLimiter.acquire("acervo_fundiario"):
            size_bytes, sha256 = await _com_retry(
                lambda: _stream_download(client, url, zip_path), url
            )

            if head_info is None:
                head_info = await _head(client, url)

        fetched_at = datetime.now(UTC)
        _save_meta(
            meta_path,
            {
                "tema": tema,
                "uf": uf,
                "source_url": url,
                "last_modified": head_info["last_modified"],
                "etag": head_info["etag"],
                "size_bytes": size_bytes,
                "sha256": sha256,
                "fetched_at": fetched_at.isoformat(),
            },
        )
        logger.info(
            "acervo_fundiario_download_ok",
            tema=tema,
            uf=uf,
            size_bytes=size_bytes,
            sha256=sha256[:16],
        )
        return Aquisicao(
            zip_path,
            False,
            fetched_at,
            {"etag": str(head_info["etag"]), "last_modified": str(head_info["last_modified"])},
            sha256,
            size_bytes,
        )
