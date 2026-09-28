from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import BinaryIO, cast
from urllib.parse import urljoin, urlsplit

import httpx
import structlog

from agrobr import constants
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http import retry
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io

from . import _tls, transport_models

logger = structlog.get_logger()
TIMEOUT = get_timeout(read=120.0)


class _StopRetry(Exception):
    def __init__(self, primary: BaseException) -> None:
        super().__init__(str(primary))
        self.primary = primary


def _failure(url: str, reason: str) -> SourceUnavailableError:
    return SourceUnavailableError(source="comexstat", url=url, last_error=reason)


def _limit(url: str, reason: str) -> ResourceLimitError:
    return ResourceLimitError(source="comexstat", url=url, reason=reason)


def _temporary_file() -> BinaryIO:
    with ExitStack() as stack:
        file = cast(BinaryIO, stack.enter_context(tempfile.TemporaryFile(mode="w+b")))
        stack.pop_all()
        return file


def _validate_url(url: str) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != constants.COMEXSTAT_DOWNLOAD_HOST
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError("Comex Stat: URL fora do host HTTPS autorizado")
    return url


def _content_length(response: httpx.Response, limit: int) -> int | None:
    values = response.headers.get_list("content-length")
    if not values:
        return None
    if len(values) != 1 or re.fullmatch(r"[0-9]+", values[0]) is None:
        raise _failure(str(response.url), "Content-Length inválido ou ambíguo")
    digits = values[0].lstrip("0") or "0"
    if (len(digits), digits) > (len(str(limit)), str(limit)):
        raise _limit(str(response.url), "Content-Length excede orçamento antes do corpo")
    return int(digits)


async def _tamanho_por_head(http: httpx.AsyncClient, url: str, limit: int) -> int | None:
    """Sem Content-Length no GET, o tamanho que o HEAD do mesmo arquivo publica."""
    try:
        resposta = await http.head(url)
    except httpx.HTTPError:
        return None
    if resposta.status_code != 200:
        return None
    return _content_length(resposta, limit)


def _headers(response: httpx.Response, limit: int) -> int | None:
    encoding = response.headers.get_list("content-encoding")
    if len(encoding) > 1 or (encoding and encoding[0].strip().lower() not in ("", "identity")):
        raise _failure(str(response.url), "Content-Encoding deve ser identity antes do spool")
    if response.status_code == 206 or "content-range" in response.headers:
        raise _failure(
            str(response.url), "Recurso parcial inesperado; necessário HTTP 200 completo"
        )
    return _content_length(response, limit)


async def _attempt(
    http: httpx.AsyncClient,
    bundle: transport_models.DownloadedResource,
    url: str,
    attempt: int,
    limit: int,
) -> httpx.Response:
    _validate_url(url)
    if len(bundle.receipts) >= constants.COMEXSTAT_MAX_PHYSICAL_REQUESTS:
        raise _limit(url, "Limite de envios físicos excedido antes do GET")
    file = bundle.file
    file.seek(0)
    file.truncate()
    receipt = transport_models.DownloadReceipt(
        index=len(bundle.receipts), attempt=attempt, url=url, started_at=datetime.now(UTC)
    )
    bundle.receipts.append(receipt)
    digest, saved_digest = hashlib.sha256(), hashlib.sha256()
    response: httpx.Response | None = None
    primary: BaseException | None = None
    try:
        response = await http.send(
            http.build_request("GET", url), stream=True, follow_redirects=False
        )
        receipt.status = response.status_code
        allowed = {
            "content-type",
            "content-length",
            "content-range",
            "content-encoding",
            "date",
            "etag",
            "last-modified",
            "retry-after",
            "location",
        }
        receipt.headers = [
            (key, value) for key, value in response.headers.multi_items() if key in allowed
        ]
        if str(response.url) != url:
            raise _failure(url, "URL efetiva diverge da tentativa enviada")
        remaining = min(limit, constants.COMEXSTAT_MAX_TRANSFER_BYTES - bundle.transfer_bytes)
        expected = _headers(response, remaining)
        async for chunk in response.aiter_raw():
            digest.update(chunk)
            receipt.received_bytes += len(chunk)
            bundle.transfer_bytes += len(chunk)
            if (
                receipt.received_bytes > limit
                or bundle.transfer_bytes > constants.COMEXSTAT_MAX_TRANSFER_BYTES
            ):
                raise _limit(
                    url, "Limite de transferência excedido após chunk recebido; não gravado"
                )
            view = memoryview(chunk)
            for offset in range(0, len(view), constants.COMEXSTAT_CHUNK_BYTES):
                part = view[offset : offset + constants.COMEXSTAT_CHUNK_BYTES]
                written = file.write(part)
                saved_digest.update(part[:written])
                receipt.size_bytes += written
                if written != len(part):
                    raise _failure(url, "Gravação incompleta do spool")
        receipt.complete_body = True
        if response.status_code == 200:
            bundle.size_check = "content_length" if expected is not None else None
            if expected is None:
                expected = await _tamanho_por_head(http, url, remaining)
                bundle.size_check = None if expected is None else "head"
        if expected is not None and expected != receipt.received_bytes:
            raise _failure(url, "Content-Length diverge dos bytes completos recebidos")
        if response.status_code == 200:
            file.seek(0)
            io.validate_download(
                file.read(512).removeprefix(b"\xef\xbb\xbf"),
                kinds=("csv",),
                source="comexstat",
                url=url,
                min_size=1,
            )
            file.seek(0)
        elif response.status_code not in (
            301,
            302,
            303,
            307,
            308,
        ) and not retry.should_retry_status(response.status_code):
            response.raise_for_status()
            raise _failure(url, f"HTTP {response.status_code}; necessário 200")
        return response
    except (
        httpx.HTTPError,
        SourceUnavailableError,
        ResourceLimitError,
        OSError,
        asyncio.CancelledError,
    ) as exc:
        primary = exc
        receipt.error_type, receipt.error_message = type(exc).__name__, str(exc)
        if isinstance(exc, httpx.CloseError) or (
            isinstance(exc, OSError) and response is not None and response.is_closed
        ):
            receipt.close_error_type, receipt.close_error_message = type(exc).__name__, str(exc)
            raise _StopRetry(exc) from exc
        raise
    finally:
        close_error: httpx.HTTPError | OSError | asyncio.CancelledError | None = None
        if response is not None:
            try:
                await response.aclose()
                receipt.closed = receipt.close_error_type is None
            except (httpx.HTTPError, OSError, asyncio.CancelledError) as exc:
                close_error = exc
                receipt.close_error_type, receipt.close_error_message = type(exc).__name__, str(exc)
        receipt.sha256, receipt.saved_sha256 = digest.hexdigest(), saved_digest.hexdigest()
        receipt.finished_at = datetime.now(UTC)
        if close_error is not None:
            raise _StopRetry(primary or close_error) from primary


async def _download(
    http: httpx.AsyncClient, bundle: transport_models.DownloadedResource, limit: int
) -> None:
    url = bundle.resource.url
    seen = {url}
    attempt = 0

    async def receive() -> httpx.Response:
        nonlocal attempt
        attempt += 1
        return await _attempt(http, bundle, url, attempt, limit)

    while True:
        response = await retry.retry_on_status(receive, source="comexstat")
        if response.status_code == 200:
            receipt = bundle.receipts[-1]
            bundle.resource_index = receipt.index
            bundle.size_bytes, bundle.sha256 = receipt.size_bytes, receipt.sha256
            bundle.fetched_at = receipt.finished_at
            bundle.complete = True
            return
        locations = response.headers.get_list("location")
        if len(locations) != 1:
            raise _failure(url, "Redirect exige um único Location")
        target = _validate_url(urljoin(url, locations[0]))
        if target in seen:
            raise _failure(url, "Loop de redirecionamento")
        seen.add(target)
        url = target


async def _close(
    bundle: transport_models.DownloadedResource, http: httpx.AsyncClient | None
) -> BaseException | None:
    first: BaseException | None = None
    for scope in ("client", "spool"):
        try:
            if scope == "client" and http is not None:
                await http.aclose()
                bundle.client_closed = True
            elif scope == "spool" and bundle._file is not None:
                bundle.file.close()
                bundle.spool_closed = True
        except (httpx.HTTPError, OSError, asyncio.CancelledError) as exc:
            if first is None:
                first = exc
            bundle.close_errors.append(
                transport_models.CloseFailure(
                    scope=scope,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    at=datetime.now(UTC),
                )
            )
            if scope == "client":
                bundle.client_closed = False
            else:
                bundle.spool_closed = False
    return first


@asynccontextmanager
async def _open_resource(
    resource: transport_models.ResourceSpec, limit: int
) -> AsyncIterator[transport_models.DownloadedResource]:
    _validate_url(resource.url)
    bundle = transport_models.DownloadedResource(resource=resource)
    bundle.budgets = {
        "resource_bytes": limit,
        "transfer_bytes": constants.COMEXSTAT_MAX_TRANSFER_BYTES,
        "chunk_bytes": constants.COMEXSTAT_CHUNK_BYTES,
        "physical_requests": constants.COMEXSTAT_MAX_PHYSICAL_REQUESTS,
        "download_timeout_seconds": constants.COMEXSTAT_DOWNLOAD_TIMEOUT_SECONDS,
    }
    http: httpx.AsyncClient | None = None
    primary: BaseException | None = None
    try:
        context = _tls.build_context()
        bundle.tls = {
            "ca_source": _tls.ca_source(),
            "intermediate_sha256": constants.COMEXSTAT_INTERMEDIATE_SHA256,
            "check_hostname": context.check_hostname,
            "verify_mode": int(context.verify_mode),
            "verify_flags": int(context.verify_flags),
        }
        bundle._file = _temporary_file()
        bundle.spool_closed = False
        headers = httpx.Headers(UserAgentRotator.get_headers(source="comexstat"))
        headers["Accept-Encoding"] = "identity"
        http = httpx.AsyncClient(
            timeout=TIMEOUT, headers=headers, verify=context, follow_redirects=False
        )
        bundle.client_closed = False
        async with asyncio.timeout(constants.COMEXSTAT_DOWNLOAD_TIMEOUT_SECONDS):
            await _download(http, bundle, limit)
        logger.info(
            "comexstat_download_complete",
            url=resource.url,
            size_bytes=bundle.size_bytes,
            attempts=len(bundle.receipts),
        )
        yield bundle
    except BaseException as exc:
        primary = exc.primary if isinstance(exc, _StopRetry) else exc
        if isinstance(primary, (httpx.HTTPError, OSError, TimeoutError)):
            detalhe = (
                f"HTTP {primary.response.status_code}"
                if isinstance(primary, httpx.HTTPStatusError)
                else type(primary).__name__
            )
            wrapped = _failure(resource.url, f"{detalhe}: {primary}")
            wrapped.__cause__ = primary
            primary = wrapped
    finally:
        close_error = await _close(bundle, http)
        if primary is None and close_error is not None:
            primary = _failure(
                resource.url, f"Erro de fechamento: {type(close_error).__name__}: {close_error}"
            )
            primary.__cause__ = close_error
        bundle.finished_at = datetime.now(UTC)
        if primary is not None:
            primary.__dict__["comexstat_acquisition"] = bundle.details()
            raise primary


@asynccontextmanager
async def open_csv(
    *, fluxo: transport_models.Flow, ano: int
) -> AsyncIterator[transport_models.DownloadedResource]:
    if type(fluxo) is not str or fluxo not in ("exportacao", "importacao"):
        raise ValueError("fluxo deve ser exportacao ou importacao")
    if type(ano) is not int or not 1997 <= ano <= 9999:
        raise ValueError("ano deve ser inteiro entre 1997 e 9999")
    prefix = "EXP" if fluxo == "exportacao" else "IMP"
    url = f"{constants.URLS[constants.Fonte.COMEXSTAT]['bulk_csv']}/{prefix}_{ano}.csv"
    resource = transport_models.ResourceSpec(kind="annual", url=url, fluxo=fluxo, ano=ano)
    async with _open_resource(resource, constants.COMEXSTAT_MAX_RESOURCE_BYTES) as result:
        yield result


@asynccontextmanager
async def open_dictionary(
    tabela: transport_models.Dictionary,
) -> AsyncIterator[transport_models.DownloadedResource]:
    if type(tabela) is not str or tabela not in constants.COMEXSTAT_DICTIONARY_URLS:
        raise ValueError("tabela deve ser unidades, paises, vias ou urfs")
    resource = transport_models.ResourceSpec(
        kind="dictionary", url=constants.COMEXSTAT_DICTIONARY_URLS[tabela], tabela=tabela
    )
    async with _open_resource(resource, constants.COMEXSTAT_MAX_DICTIONARY_BYTES) as result:
        yield result
