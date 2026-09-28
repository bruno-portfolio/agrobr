from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Literal

import httpx
from pydantic import ValidationError

from agrobr import constants
from agrobr.exceptions import ParseError, ResourceLimitError
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import _transport, acquisition, catalog
from .models import DATASET_PRACAS_SLUG, DATASET_TRAFEGO_SLUG, build_ckan_package_url

TIMEOUT = get_timeout(read=180.0)
_SESSION: ContextVar[tuple[asyncio.AbstractEventLoop, httpx.AsyncClient] | None] = ContextVar(
    "antt_pedagio_session", default=None
)


@asynccontextmanager
async def session(*, reuse: bool = True) -> AsyncIterator[httpx.AsyncClient]:
    active = _SESSION.get()
    loop = asyncio.get_running_loop()
    if reuse and active is not None and active[0] is loop and not active[1].is_closed:
        yield active[1]
        return
    headers = httpx.Headers(UserAgentRotator.get_headers(source="antt_pedagio"))
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=headers,
        follow_redirects=False,
    ) as http:
        state = _SESSION.set((loop, http))
        try:
            yield http
        finally:
            _SESSION.reset(state)


def _validate_years(anos: list[int], frequencia: catalog.Frequency) -> None:
    if (
        type(anos) is not list
        or not anos
        or any(type(year) is not int or not 2010 <= year <= 9999 for year in anos)
        or len(anos) != len(set(anos))
    ):
        raise ValueError("anos deve conter inteiros únicos entre 2010 e 9999")
    if type(frequencia) is not str or frequencia not in ("mensal", "diaria"):
        raise ValueError("frequencia deve ser 'mensal' ou 'diaria'")


async def _discover(
    http: httpx.AsyncClient, transport: _transport.Transport, slug: str
) -> catalog.CatalogPackage:
    url = build_ckan_package_url(slug)
    file, index = await transport.fetch(
        http, url, role="catalog", limit=constants.ANTT_MAX_CATALOG_BYTES
    )
    receipt = transport.bundle.attempts[index]
    transport.bundle.catalog_sha256 = receipt.sha256
    transport.bundle.catalog_size_bytes = receipt.size_bytes
    transport.bundle.catalog_resource_index = index
    primary: BaseException | None = None
    try:
        package = catalog.CatalogEnvelope.model_validate_json(file.read()).result
        if package.name != slug:
            raise ValueError("Identidade do pacote CKAN divergente")
        transport.bundle.catalog = package
        return package
    except (ValidationError, ValueError) as exc:
        primary = ParseError(
            source="antt_pedagio", parser_version=3, reason=f"Catálogo CKAN inválido: {exc}"
        )
        raise primary from exc
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            file.close()
        except OSError as close_error:
            transport.bundle.spool_close_errors.append(
                {
                    "file_index": None,
                    "resource_id": None,
                    "ano": None,
                    "role": "catalog",
                    "resource_index": index,
                    "error_type": type(close_error).__name__,
                    "error_message": str(close_error),
                    "at": datetime.now(UTC).isoformat(),
                }
            )
            if primary is None:
                raise


async def _download(
    http: httpx.AsyncClient,
    transport: _transport.Transport,
    resource: catalog.CatalogResource,
    year: int | None,
    frequency: catalog.Frequency | None,
) -> None:
    role: Literal["pracas", "trafego"] = "pracas" if year is None else "trafego"
    limit = constants.ANTT_MAX_PRACAS_BYTES if year is None else constants.ANTT_MAX_CSV_BYTES
    file, index = await transport.fetch(
        http,
        resource.url,
        role=role,
        limit=limit,
        expected=resource.size,
        resource_id=resource.id,
        year=year,
    )
    receipt = transport.bundle.attempts[index]
    assert receipt.sha256 is not None
    transport.bundle.files.append(
        acquisition.DownloadedCSV(
            ano=year,
            frequencia=frequency,
            resource=resource,
            file=file,
            size_bytes=receipt.size_bytes,
            sha256=receipt.sha256,
            resource_index=index,
        )
    )
    transport.bundle.spool_bytes += receipt.size_bytes


@asynccontextmanager
async def _open(
    anos: list[int], frequency: catalog.Frequency | None
) -> AsyncIterator[acquisition.TrafegoAcquisition]:
    bundle = acquisition.TrafegoAcquisition(requested_years=list(anos), frequencia=frequency)
    transport = _transport.Transport(bundle)
    primary: BaseException | None = None
    try:
        async with session() as http:
            package = await _discover(
                http,
                transport,
                DATASET_TRAFEGO_SLUG if frequency is not None else DATASET_PRACAS_SLUG,
            )
            selected: list[tuple[int | None, catalog.CatalogResource]]
            if frequency is None:
                selected = [(None, catalog.select_plazas(package.resources))]
            else:
                selected = [
                    (year, catalog.select_traffic(package.resources, year, frequency))
                    for year in anos
                ]
            bundle.selected_resources = [
                {"ano": year, "resource": item.model_dump(mode="json")} for year, item in selected
            ]
            for _, item in selected:
                catalog.validate_download_url(item.url)
            limit = (
                constants.ANTT_MAX_CSV_BYTES
                if frequency is not None
                else constants.ANTT_MAX_PRACAS_BYTES
            )
            announced = [item.size for _, item in selected if item.size is not None]
            if (
                any(size > limit for size in announced)
                or sum(announced) > constants.ANTT_MAX_SPOOL_BYTES
            ):
                raise ResourceLimitError(
                    source="antt_pedagio",
                    reason="Tamanho CKAN excede orçamento antes do GET de CSV",
                )
            for year, item in selected:
                await _download(http, transport, item, year, frequency)
            bundle.complete = True
            bundle.finished_at = datetime.now(UTC)
            yield bundle
    except BaseException as exc:
        primary = exc
        bundle.finished_at = datetime.now(UTC)
        raise
    finally:
        try:
            bundle.close()
        except OSError as exc:
            if primary is None:
                primary = exc
                raise
        finally:
            if primary is not None:
                primary.__dict__["antt_acquisition"] = bundle.details()


@asynccontextmanager
async def open_trafego_anos(
    anos: list[int], *, frequencia: catalog.Frequency = "mensal"
) -> AsyncIterator[acquisition.TrafegoAcquisition]:
    _validate_years(anos, frequencia)
    async with _open(anos, frequencia) as bundle:
        yield bundle


@asynccontextmanager
async def open_pracas() -> AsyncIterator[acquisition.TrafegoAcquisition]:
    async with _open([], None) as bundle:
        yield bundle


async def fetch_pracas(*, source_urls: list[str] | None = None) -> bytes:
    async with open_pracas() as bundle:
        item = bundle.files[0]
        if source_urls is not None:
            source_urls.append(item.resource.url)
        return item.file.read()
