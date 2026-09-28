from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import partial
from typing import Any, cast

import httpx
import pandas as pd
import pydantic
import structlog

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import ptax_acquisition, ptax_models, ptax_parser, ptax_query

logger = structlog.get_logger()
PTAX_BASE = constants.URLS[constants.Fonte.BCB]["ptax"]
TIMEOUT = get_timeout(read=30.0)
PTAX_MAX_RETRIES = 4
StreamQuery = ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery
StreamPage = ptax_models.PtaxQuotesPage | ptax_models.PtaxCurrenciesPage
StreamRecord = ptax_models.PtaxObservation | ptax_models.PtaxCurrency


@dataclass
class _Collection:
    records: list[StreamRecord] = field(default_factory=list)
    resources: list[ptax_acquisition.PtaxResource] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    identities: set[tuple[Any, ...]] = field(default_factory=set)
    expected_count: int | None = None
    received_count: int = 0
    last_timestamp: pd.Timestamp | None = None


def _fail(reason: str) -> ParseError:
    return ParseError(source="bcb_ptax", parser_version=2, reason=reason)


def _input_date(value: date | None) -> str | None:
    if value is None:
        return None
    if type(value) is not date:
        raise InvalidParameterError("Seleção PTAX contém data não civil")
    return f"{value.day:02d}/{value.month:02d}/{value.year:04d}"


def _validate_query(query: ptax_acquisition.PtaxQuery) -> None:
    try:
        ptax_acquisition.PtaxQuery.model_validate(query.model_dump())
    except pydantic.ValidationError:
        raise InvalidParameterError("Seleção PTAX contém campos inválidos") from None
    rebuilt = ptax_query.build_query(
        data=_input_date(query.data),
        data_inicial=_input_date(query.data_inicial),
        data_final=_input_date(query.data_final),
        moeda=query.requested_moeda,
        boletim=query.boletim,
        top=query.top,
        reference_date=query.reference_date,
    )
    if rebuilt != query:
        raise InvalidParameterError("Seleção PTAX incompatível com parâmetros validados")


async def _get_page(
    http: httpx.AsyncClient,
    query: StreamQuery,
    url: str,
    offset: int,
) -> httpx.Response:
    target = ptax_query.validate_page_url(query, url, skip=offset)
    visited: set[str] = set()
    for _ in range(constants.BCB_PTAX_MAX_REDIRECTS + 1):
        if target in visited:
            raise _fail("Ciclo de redirects PTAX")
        visited.add(target)
        try:
            response = await retry_on_status(
                partial(http.get, target, follow_redirects=False),
                source="bcb",
                max_attempts=PTAX_MAX_RETRIES,
            )
        except httpx.HTTPError as exc:
            raise SourceUnavailableError(
                source="bcb_ptax",
                url=target,
                last_error=f"{type(exc).__name__}: {exc}",
            ) from exc
        except SourceUnavailableError as exc:
            raise SourceUnavailableError(
                source="bcb_ptax",
                url=target,
                last_error=exc.last_error,
            ) from exc
        if not response.has_redirect_location:
            if response.status_code != 200:
                raise SourceUnavailableError(
                    source="bcb_ptax",
                    url=str(response.url),
                    last_error=f"HTTP {response.status_code}",
                )
            return response
        target = ptax_query.validate_next_link(
            query,
            str(response.url),
            response.headers["location"],
            skip=offset,
        )
    raise SourceUnavailableError(
        source="bcb_ptax",
        url=url,
        last_error="Limite de redirects excedido",
    )


def _validate_records(query: StreamQuery, page: StreamPage, state: _Collection) -> None:
    if page.source_rows != len(page.records) or page.source_rows > query.top:
        raise _fail("Página PTAX incompatível com top ou quantidade de registros")
    for row in page.records:
        identity: tuple[Any, ...]
        if isinstance(row, ptax_models.PtaxObservation):
            if not isinstance(query, ptax_acquisition.PtaxQuery):
                raise _fail("Cotação PTAX fora de uma consulta de cotação")
            if not query.inicio <= row.data <= query.fim:
                raise _fail("Cotação PTAX fora do intervalo solicitado")
            timestamp = row.timestamp
            if state.last_timestamp is not None and timestamp < state.last_timestamp:
                raise _fail("Ordem crescente dos horários PTAX foi violada")
            state.last_timestamp = timestamp
            identity = ptax_models.identity(row)
        else:
            identity = (row.moeda,)
        if identity in state.identities:
            raise _fail("Chave PTAX repetida dentro ou entre páginas")
        state.identities.add(identity)


def _validate_count(page: StreamPage, state: _Collection) -> None:
    if page.reported_count is not None:
        if state.expected_count is not None and state.expected_count != page.reported_count:
            raise _fail("Contagem PTAX mudou entre páginas")
        state.expected_count = page.reported_count
    received = state.received_count + page.source_rows
    if state.expected_count is not None:
        if received > state.expected_count:
            raise _fail("Registros PTAX excedem a contagem declarada")
        if received == state.expected_count and page.next_link is not None:
            raise _fail("Contagem PTAX esgotada mas nextLink indica continuação")
        if not page.source_rows and received < state.expected_count:
            raise _fail("Página PTAX vazia antes da contagem declarada")


def _select_records(query: StreamQuery, page: StreamPage) -> list[StreamRecord]:
    selected: list[StreamRecord] = []
    for row in page.records:
        if isinstance(query, ptax_acquisition.PtaxQuery) and isinstance(
            row, ptax_models.PtaxObservation
        ):
            category = constants.BCB_PTAX_BULLETIN_LABELS.get(row.tipo_boletim or "")
            if query.boletim != "todos":
                if category is None:
                    raise _fail(
                        "Boletim PTAX desconhecido ou nulo impede aplicar filtro específico"
                    )
                if category != query.boletim:
                    continue
        selected.append(row)
    return selected


def _resource(
    query: StreamQuery,
    response: httpx.Response,
    requested_url: str,
    offset: int,
    page: StreamPage,
    page_index: int,
    retained: int,
) -> ptax_acquisition.PtaxResource:
    return ptax_acquisition.PtaxResource(
        role="quotes" if isinstance(query, ptax_acquisition.PtaxQuery) else "catalog",
        page_index=page_index,
        requested_url=requested_url,
        url=str(response.url),
        parameters=ptax_query.page_parameters(query, offset),
        offset=offset,
        top=query.top,
        sha256=hashlib.sha256(response.content).hexdigest(),
        size_bytes=len(response.content),
        fetched_at=datetime.now(UTC),
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
        received_count=page.source_rows,
        retained_count=retained,
        reported_count=page.reported_count,
        next_link=page.next_link,
        layout_fingerprint=page.layout_fingerprint,
        parser_version=page.parser_version,
    )


def _coverage(query: StreamQuery, state: _Collection) -> ptax_acquisition.PtaxCoverage:
    counted = state.expected_count is not None
    return ptax_acquisition.PtaxCoverage(
        completeness="complete" if counted else "unknown",
        basis="source_count_reconciled" if counted else "terminal_page_without_source_total",
        expected_count=state.expected_count,
        received_count=state.received_count,
        returned_count=len(state.records),
        filtered_by_boletim_count=state.received_count - len(state.records),
        pages_fetched=len(state.resources),
        page_size_requested=query.top,
        stop_reason="source_count" if counted else "terminal_empty_page",
        terminal_empty_page=state.resources[-1].received_count == 0,
    )


async def _collect(query: StreamQuery) -> _Collection:
    state = _Collection()
    target = ptax_query.build_page_url(query, skip=0)
    visited: set[str] = set()
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=False,
    ) as http:
        while True:
            if target in visited:
                raise _fail("Ciclo de páginas PTAX")
            visited.add(target)
            offset = state.received_count
            response = await _get_page(http, query, target, offset)
            page = (
                ptax_parser.parse_quotes_page(response.content, query.moeda)
                if isinstance(query, ptax_acquisition.PtaxQuery)
                else ptax_parser.parse_currencies_page(response.content)
            )
            _validate_records(query, page, state)
            _validate_count(page, state)
            selected = _select_records(query, page)
            next_link = None
            if page.next_link is not None:
                if not page.source_rows:
                    raise _fail("Página PTAX vazia com continuação sem avanço")
                next_link = ptax_query.validate_next_link(
                    query,
                    str(response.url),
                    page.next_link,
                    skip=offset + page.source_rows,
                )
            resource = _resource(
                query, response, target, offset, page, len(state.resources), len(selected)
            )
            state.resources.append(resource)
            state.records.extend(selected)
            state.received_count += page.source_rows
            state.warnings.extend(
                f"Página PTAX {resource.role} {resource.page_index} (offset {offset}): {warning}"
                for warning in page.warnings
            )
            if state.expected_count is not None and state.received_count == state.expected_count:
                return state
            if not page.source_rows:
                return state
            target = next_link or ptax_query.build_page_url(query, skip=state.received_count)


async def fetch_currencies_acquisition(
    query: ptax_acquisition.PtaxCatalogQuery,
) -> ptax_acquisition.PtaxCatalogAcquisition:
    rebuilt = ptax_query.build_catalog_query(query.top)
    if rebuilt != query:
        raise InvalidParameterError(
            "Seleção do catálogo PTAX incompatível com parâmetros validados"
        )
    state = await _collect(query)
    if not state.records:
        state.warnings.append("Catálogo PTAX corrente retornou vazio; nenhuma moeda foi validada.")
    return ptax_acquisition.PtaxCatalogAcquisition(
        query=query,
        records=cast(list[ptax_models.PtaxCurrency], state.records),
        resources=state.resources,
        coverage=_coverage(query, state),
        warnings=state.warnings,
    )


async def fetch_ptax_acquisition(
    query: ptax_acquisition.PtaxQuery,
) -> ptax_acquisition.PtaxAcquisition:
    _validate_query(query)
    catalog = await fetch_currencies_acquisition(
        ptax_query.build_catalog_query(constants.BCB_PTAX_PAGE_SIZE)
    )
    if not catalog.records:
        raise SourceUnavailableError(
            source="bcb_ptax",
            url=catalog.resources[-1].url,
            last_error="Catálogo PTAX vazio impede validar a moeda solicitada",
        )
    if query.moeda not in {row.moeda for row in catalog.records}:
        raise InvalidParameterError(
            f"Moeda {query.moeda} ausente no catálogo PTAX atual; isso não determina sua validade histórica"
        )
    state = await _collect(query)
    logger.info("bcb_ptax_ok", moeda=query.moeda, records=len(state.records))
    return ptax_acquisition.PtaxAcquisition(
        query=query,
        records=cast(list[ptax_models.PtaxObservation], state.records),
        catalog=catalog,
        resources=state.resources,
        coverage=_coverage(query, state),
        warnings=catalog.warnings + state.warnings,
    )
