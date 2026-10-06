from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import partial
from typing import Any, Literal

import httpx

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import focus_acquisition, focus_models, focus_parser, focus_query

logger = _log.get_logger(__name__)
TIMEOUT = get_timeout(read=30.0)


@dataclass
class _Collection:
    records: list[focus_models.FocusObservation] = field(default_factory=list)
    resources: list[focus_acquisition.FocusResource] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    identities: set[tuple[Any, ...]] = field(default_factory=set)
    expected_count: int | None = None
    last_date: date | None = None


def _fail(reason: str) -> ParseError:
    return ParseError(source="bcb_focus", parser_version=2, reason=reason)


async def _get_page(
    http: httpx.AsyncClient,
    query: focus_acquisition.FocusQuery,
    url: str,
    offset: int,
) -> httpx.Response:
    target = focus_query.validate_page_url(query, url, skip=offset)
    visited: set[str] = set()
    for _ in range(constants.BCB_FOCUS_MAX_REDIRECTS + 1):
        if target in visited:
            raise _fail("Ciclo de redirects Focus")
        visited.add(target)
        try:
            response = await retry_on_status(
                partial(http.get, target, follow_redirects=False),
                source="bcb",
            )
        except httpx.HTTPError as exc:
            raise SourceUnavailableError(
                source="bcb_focus",
                url=target,
                last_error=f"{type(exc).__name__}: {exc}",
            ) from exc
        except SourceUnavailableError as exc:
            raise SourceUnavailableError(
                source="bcb_focus",
                url=target,
                last_error=exc.last_error,
            ) from exc
        if not response.has_redirect_location:
            if response.status_code != 200:
                raise SourceUnavailableError(
                    source="bcb_focus",
                    url=str(response.url),
                    last_error=f"HTTP {response.status_code}",
                )
            return response
        target = focus_query.validate_next_link(
            query,
            str(response.url),
            response.headers["location"],
            skip=offset,
        )
    raise SourceUnavailableError(
        source="bcb_focus", url=url, last_error="Limite de redirects excedido"
    )


def _validate_records(
    query: focus_acquisition.FocusQuery,
    page: focus_models.FocusParsedPage,
    state: _Collection,
) -> None:
    if page.source_rows > query.top:
        raise _fail("Página Focus excede top solicitado")
    for row in page.records:
        if row.indicador != query.indicador or row.periodicidade != query.periodicidade:
            raise _fail("Registro Focus incompatível com indicador ou periodicidade solicitados")
        if query.data_inicial is not None and row.data < query.data_inicial:
            raise _fail("Registro Focus anterior à data inicial solicitada")
        if state.last_date is not None and row.data > state.last_date:
            raise _fail("Ordem decrescente das datas Focus foi violada")
        identity = focus_models.identity(row)
        if identity in state.identities:
            raise _fail("Chave de observação Focus repetida dentro ou entre páginas")
        state.identities.add(identity)
        state.last_date = row.data


def _validate_count(page: focus_models.FocusParsedPage, state: _Collection) -> None:
    if page.reported_count is not None:
        if state.expected_count is not None and page.reported_count != state.expected_count:
            raise _fail("Contagem Focus mudou entre páginas")
        state.expected_count = page.reported_count
    if state.expected_count is not None:
        received = len(state.records) + page.source_rows
        if received > state.expected_count:
            raise _fail("Registros Focus excedem a contagem declarada")
        if received == state.expected_count and page.next_link is not None:
            raise _fail("Contagem Focus esgotada mas nextLink indica continuação")
        if page.source_rows == 0 and received < state.expected_count:
            raise _fail("Página Focus vazia antes da contagem declarada")


def _resource(
    query: focus_acquisition.FocusQuery,
    response: httpx.Response,
    requested_url: str,
    offset: int,
    page: focus_models.FocusParsedPage,
    page_index: int,
) -> focus_acquisition.FocusResource:
    retained = page.source_rows
    if query.max_registros is not None:
        retained = min(retained, max(0, query.max_registros - offset))
    return focus_acquisition.FocusResource(
        page_index=page_index,
        requested_url=requested_url,
        url=str(response.url),
        parameters=focus_query.page_parameters(query, offset),
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


def _coverage(
    query: focus_acquisition.FocusQuery,
    state: _Collection,
    returned: int,
    stop: Literal["source_count", "local_limit", "terminal_empty_page"],
    next_link: str | None,
) -> focus_acquisition.FocusCoverage:
    received = len(state.records)
    discarded = received - returned
    completeness: Literal["complete", "partial", "unknown"] = "unknown"
    basis = "terminal_page_without_source_total"
    if state.expected_count is not None:
        if returned == state.expected_count:
            completeness, basis = "complete", "source_count_reconciled"
        else:
            completeness, basis = "partial", "local_limit_below_source_count"
    elif stop == "local_limit":
        if discarded or next_link:
            completeness, basis = "partial", "local_limit_with_remaining_evidence"
        else:
            basis = "local_limit_without_source_total"
    return focus_acquisition.FocusCoverage(
        completeness=completeness,
        basis=basis,
        expected_count=state.expected_count,
        received_count=received,
        returned_count=returned,
        discarded_by_local_limit=discarded,
        pages_fetched=len(state.resources),
        page_size_requested=query.top,
        local_limit=query.max_registros,
        local_limit_reached=query.max_registros is not None and returned >= query.max_registros,
        stop_reason=stop,
        terminal_empty_page=state.resources[-1].received_count == 0,
        next_link_remaining=next_link,
    )


def _finish(
    query: focus_acquisition.FocusQuery,
    state: _Collection,
    stop: Literal["source_count", "local_limit", "terminal_empty_page"],
    next_link: str | None,
) -> focus_acquisition.FocusAcquisition:
    records = state.records if query.max_registros is None else state.records[: query.max_registros]
    coverage = _coverage(query, state, len(records), stop, next_link)
    if stop == "local_limit":
        cobertura = {
            "complete": "completa",
            "partial": "parcial",
            "unknown": "não comprovada",
        }[coverage.completeness]
        total = (
            "total não informado pela fonte"
            if coverage.expected_count is None
            else f"total declarado {coverage.expected_count}"
        )
        state.warnings.append(
            f"Limite local max_registros={query.max_registros} encerrou a coleta Focus; "
            f"cobertura {cobertura}, {total}."
        )
    return focus_acquisition.FocusAcquisition(
        query=query,
        records=records,
        resources=state.resources,
        coverage=coverage,
        warnings=list(dict.fromkeys(state.warnings)),
    )


async def fetch_focus_acquisition(
    query: focus_acquisition.FocusQuery,
) -> focus_acquisition.FocusAcquisition:
    rebuilt = focus_query.build_query(
        query.indicador,
        periodicidade=query.periodicidade,
        top=query.top,
        data_inicial=query.data_inicial.isoformat() if query.data_inicial is not None else None,
        max_registros=query.max_registros,
    )
    if rebuilt != query:
        raise InvalidParameterError("Seleção Focus incompatível com parâmetros validados")
    state = _Collection()
    target = focus_query.build_page_url(query, skip=0)
    visited: set[str] = set()
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=False,
    ) as http:
        while True:
            if target in visited:
                raise _fail("Ciclo de páginas Focus")
            visited.add(target)
            offset = len(state.records)
            response = await _get_page(http, query, target, offset)
            page = focus_parser.parse_page(response.content, query.periodicidade)
            _validate_records(query, page, state)
            _validate_count(page, state)
            next_link = None
            if page.next_link is not None:
                if not page.source_rows:
                    raise _fail("Página Focus vazia com continuação sem avanço")
                next_link = focus_query.validate_next_link(
                    query,
                    str(response.url),
                    page.next_link,
                    skip=offset + page.source_rows,
                )
            state.resources.append(
                _resource(query, response, target, offset, page, len(state.resources))
            )
            state.records.extend(page.records)
            state.warnings.extend(
                f"Página Focus {len(state.resources) - 1} (offset {offset}): {warning}"
                for warning in page.warnings
            )
            if (
                state.expected_count is not None
                and len(state.records) == state.expected_count
                and (query.max_registros is None or len(state.records) <= query.max_registros)
            ):
                return _finish(query, state, "source_count", next_link)
            if query.max_registros is not None and len(state.records) >= query.max_registros:
                return _finish(query, state, "local_limit", next_link)
            if not page.source_rows:
                return _finish(query, state, "terminal_empty_page", None)
            target = next_link or focus_query.build_page_url(query, skip=len(state.records))
