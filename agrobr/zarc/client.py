from __future__ import annotations

import io
import re

import httpx

from agrobr import _log, constants
from agrobr.constants import MIN_CSV_SIZE
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from . import acquisition, catalog, models

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=180.0)
_FAIXA_INTEIRA = re.compile(r"bytes 0-(\d+)/(\d+)")
_CSV_NA_ORIGEM = io_utils.download_url_hook(
    base_url=constants.URLS[constants.Fonte.ZARC]["base"], source="zarc"
)


def _tamanho_publicado(headers: httpx.Headers) -> int | None:
    """O tamanho do arquivo inteiro, como o servidor o informa.

    O portal do MAPA manda o CSV em gzip e por partes, sem Content-Length, mas com
    ``Content-Range: bytes 0-N/T`` no 200, em que T é o tamanho descomprimido. Sem compressão, vale o
    Content-Length.
    """
    faixa = _FAIXA_INTEIRA.fullmatch(headers.get("content-range", "").strip())
    if faixa and int(faixa[1]) + 1 == int(faixa[2]):
        return int(faixa[2])
    tamanho = headers.get("content-length", "")
    if tamanho.isdigit() and headers.get("content-encoding", "identity").lower() == "identity":
        return int(tamanho)
    return None


async def _get(url: str, *, csv: bool) -> tuple[httpx.Response, acquisition.HTTPAcquisition]:
    transferred = 0
    limit = constants.ZARC_MAX_DOWNLOAD_BYTES if csv else constants.ZARC_MAX_CATALOG_BYTES
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="zarc"),
        follow_redirects=True,
        event_hooks={"request": [_CSV_NA_ORIGEM] if csv else []},
    ) as session:

        async def attempt() -> httpx.Response:
            nonlocal transferred
            async with session.stream("GET", url) as streamed:
                if not streamed.is_success:
                    return streamed
                content = io.BytesIO()
                total = _tamanho_publicado(streamed.headers)
                if total and total <= limit:
                    content.seek(total - 1)
                    content.write(b"\0")
                    content.seek(0)
                async for chunk in streamed.aiter_bytes():
                    transferred += len(chunk)
                    if (
                        content.tell() + len(chunk) > limit
                        or transferred > constants.ZARC_MAX_TRANSFER_BYTES
                    ):
                        raise ResourceLimitError(
                            "zarc", "Download excede o orçamento de bytes", url=url
                        )
                    content.write(chunk)
                content.truncate()
                response = httpx.Response(
                    streamed.status_code, content=content.getvalue(), request=streamed.request
                )
                response.headers = streamed.headers
                return response

        response = await retry_on_status(attempt, source="zarc")
        responses.raise_for_status(response, source="zarc")
        total = _tamanho_publicado(response.headers) if csv else None
        if total is not None and total != len(response.content):
            raise SourceUnavailableError(
                source="zarc",
                url=url,
                last_error=f"download incompleto: {len(response.content)} de {total} bytes",
            )
        if csv:
            io_utils.validate_download(
                response.content, kinds=("csv",), source="zarc", url=url, min_size=MIN_CSV_SIZE
            )
        captured = acquisition.from_response(response, url, published_size_bytes=total)
    logger.info("zarc_download_ok", source="zarc", size_bytes=captured.resource.size_bytes)
    return response, captured


async def discover_catalog() -> catalog.Catalog:
    url = models.build_ckan_package_url(models.DATASET_SLUG)
    response, captured = await _get(url, csv=False)
    payload = responses.parse_json_response(response, source="zarc", url=url)
    return catalog.parse_catalog(payload, captured.resource)


async def discover_resources() -> list[dict[str, str]]:
    return (await discover_catalog()).resources()


async def download_acquisition(url: str) -> acquisition.HTTPAcquisition:
    _, captured = await _get(url, csv=True)
    return captured


async def download_csv(url: str) -> bytes:
    return (await download_acquisition(url)).content
