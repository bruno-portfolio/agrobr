from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

import httpx
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.time import hoje

from . import sgs_acquisition, sgs_models, sgs_parser, sgs_query

logger = _log.get_logger(__name__)

SGS_BASE = constants.URLS[constants.Fonte.BCB]["sgs"]
TIMEOUT = get_timeout(read=30.0)


def _default_start_date() -> str:
    return sgs_query.format_date(sgs_query.default_start(hoje()))


def _request_parameters(
    codigo: int,
    block: sgs_acquisition.SGSBlock,
) -> tuple[str, dict[str, str]]:
    url = f"{SGS_BASE}.{codigo}/dados"
    if block.mode == "latest":
        url += f"/ultimos/{block.ultimos}"
    params = {"formato": "json"}
    if block.inicio is not None:
        params["dataInicial"] = sgs_query.format_date(block.inicio)
    if block.fim is not None:
        params["dataFinal"] = sgs_query.format_date(block.fim)
    return url, params


def query_url(query: sgs_acquisition.SGSQuery) -> str:
    block = sgs_acquisition.SGSBlock(
        id="consulta", mode=query.mode, inicio=query.inicio, fim=query.fim, ultimos=query.ultimos
    )
    url, params = _request_parameters(query.codigo, block)
    return str(httpx.URL(url, params=params))


def _parse_response(response: httpx.Response) -> tuple[sgs_models.SGSParsedBlock, list[str]]:
    if response.status_code == 400:
        try:
            limit = sgs_acquisition.SGSLatestLimitEnvelope.model_validate_json(response.content)
        except pydantic.ValidationError:
            pass
        else:
            raise InvalidParameterError(limit.erro.detail.partition(": ")[2])
    if response.status_code == 404:
        try:
            sgs_acquisition.SGSNotFoundEnvelope.model_validate_json(response.content)
        except pydantic.ValidationError:
            pass
        else:
            warning = (
                "SGS declarou ausência de valores (HTTP 404); esse envelope não comprova "
                "existência ou validade do código da série."
            )
            return sgs_models.SGSParsedBlock(
                records=[],
                source_rows=0,
                layout_fingerprint=None,
                parser_version=sgs_models.PARSER_VERSION,
                warnings=[],
            ), [warning]
    if response.status_code in {400, 406}:
        try:
            error = sgs_acquisition.SGSSelectionError.model_validate_json(response.content)
        except pydantic.ValidationError:
            pass
        else:
            detail = f"{error.error}. {error.message}" if error.message else error.error
            raise InvalidParameterError(detail[:2000])
    if response.status_code != 200:
        raise SourceUnavailableError(
            source="bcb_sgs",
            url=str(response.url),
            last_error=f"HTTP {response.status_code}",
        )
    return sgs_parser.parse_observations(response.content), []


def _reference_diagnostics(
    records: list[sgs_models.SGSObservation],
    block: sgs_acquisition.SGSBlock,
) -> sgs_acquisition.SGSReferenceDiagnostics:
    before = [row.data for row in records if block.inicio is not None and row.data < block.inicio]
    after = [row.data for row in records if block.fim is not None and row.data > block.fim]
    return sgs_acquisition.SGSReferenceDiagnostics(
        before_count=len(before),
        before_min=min(before, default=None),
        before_max=max(before, default=None),
        after_count=len(after),
        after_min=min(after, default=None),
        after_max=max(after, default=None),
    )


async def _fetch_block(
    http: httpx.AsyncClient,
    codigo: int,
    block: sgs_acquisition.SGSBlock,
) -> tuple[sgs_models.SGSParsedBlock, sgs_acquisition.SGSResource, list[str]]:
    url, params = _request_parameters(codigo, block)
    try:
        response = await retry_on_status(
            lambda: http.get(url, params=params),
            source="bcb",
            max_attempts=2,
        )
    except httpx.HTTPError as exc:
        raise SourceUnavailableError(
            source="bcb_sgs",
            url=url,
            last_error=f"{type(exc).__name__}: {exc}",
        ) from exc
    except SourceUnavailableError as exc:
        raise SourceUnavailableError(source="bcb_sgs", url=url, last_error=exc.last_error) from exc
    parsed, warnings = _parse_response(response)
    diagnostics = _reference_diagnostics(parsed.records, block)
    if diagnostics.before_count or diagnostics.after_count:
        warnings.append(
            f"Bloco SGS {block.id}: {diagnostics.before_count} referências anteriores e "
            f"{diagnostics.after_count} posteriores aos limites diários foram preservadas; "
            "a resposta não informa frequência ou regras de seleção da série."
        )
    warnings.extend(parsed.warnings)
    resource = sgs_acquisition.SGSResource(
        requested_url=str(httpx.URL(url, params=params)),
        url=str(response.url),
        parameters=params,
        block=block,
        sha256=hashlib.sha256(response.content).hexdigest(),
        size_bytes=len(response.content),
        fetched_at=datetime.now(UTC),
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
        received_count=parsed.source_rows,
        layout_fingerprint=parsed.layout_fingerprint,
        parser_version=parsed.parser_version,
        reference_diagnostics=diagnostics,
    )
    return parsed, resource, warnings


def _merge_block(
    parsed: sgs_models.SGSParsedBlock,
    resource: sgs_acquisition.SGSResource,
    resource_index: int,
    records: dict[date, sgs_models.SGSObservation],
    origins: dict[date, list[sgs_acquisition.SGSOrigin]],
) -> None:
    for index, record in enumerate(parsed.records):
        previous = records.get(record.data)
        if previous is not None and previous.valor != record.valor:
            raise ParseError(
                source="bcb_sgs",
                parser_version=sgs_models.PARSER_VERSION,
                reason=f"Valores conflitantes entre blocos SGS: {record.data.isoformat()}",
            )
        records[record.data] = record
        origins.setdefault(record.data, []).append(
            sgs_acquisition.SGSOrigin(
                block_id=resource.block.id,
                resource_index=resource_index,
                row_index=index,
            )
        )


def _finish_acquisition(
    query: sgs_acquisition.SGSQuery,
    blocks: list[sgs_acquisition.SGSBlock],
    resources: list[sgs_acquisition.SGSResource],
    records: dict[date, sgs_models.SGSObservation],
    origins: dict[date, list[sgs_acquisition.SGSOrigin]],
    warnings: list[str],
) -> sgs_acquisition.SGSAcquisition:
    ordered = [records[key] for key in sorted(records)]
    reconciliation = [
        sgs_acquisition.SGSReconciliation(data=row.data, valor=row.valor, origins=origins[row.data])
        for row in ordered
        if len(origins[row.data]) > 1
    ]
    unique_count = len(ordered)
    start = ordered[0].data if ordered else None
    end = ordered[-1].data if ordered else None
    received_count = sum(resource.received_count for resource in resources)
    if reconciliation:
        warnings.append(
            f"{received_count - unique_count} repetições idênticas entre blocos SGS foram "
            "reconciliadas; todas as origens estão registradas em reconciliation."
        )
    tail_applied = query.ultimos is not None and query.mode != "latest"
    if query.ultimos is not None:
        ordered = ordered[-query.ultimos :]
    coverage = sgs_acquisition.SGSCoverage(
        planned_blocks=len(blocks),
        completed_blocks=len(resources),
        received_count=received_count,
        unique_count=unique_count,
        reconciled_count=received_count - unique_count,
        returned_count=len(ordered),
        empty_blocks=[resource.block.id for resource in resources if resource.received_count == 0],
        observed_start=start,
        observed_end=end,
        tail_applied=tail_applied,
    )
    return sgs_acquisition.SGSAcquisition(
        query=query,
        records=ordered,
        resources=resources,
        coverage=coverage,
        reconciliation=reconciliation,
        warnings=list(dict.fromkeys(warnings)),
    )


async def fetch_sgs_acquisition(
    query: sgs_acquisition.SGSQuery,
) -> sgs_acquisition.SGSAcquisition:
    try:
        query = sgs_acquisition.SGSQuery.model_validate(query.model_dump())
    except pydantic.ValidationError:
        raise InvalidParameterError("Seleção SGS inválida") from None
    blocks = sgs_query.plan_query(query)
    resources: list[sgs_acquisition.SGSResource] = []
    records: dict[date, sgs_models.SGSObservation] = {}
    origins: dict[date, list[sgs_acquisition.SGSOrigin]] = {}
    warnings: list[str] = []
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        for block in blocks:
            parsed, resource, block_warnings = await _fetch_block(http, query.codigo, block)
            _merge_block(parsed, resource, len(resources), records, origins)
            resources.append(resource)
            warnings.extend(block_warnings)
    result = _finish_acquisition(query, blocks, resources, records, origins, warnings)
    logger.info("bcb_sgs_ok", codigo=query.codigo, records=len(result.records), blocks=len(blocks))
    return result


async def fetch_sgs(
    codigo: int,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
) -> tuple[list[dict[str, str | None]], str]:
    query = sgs_query.build_query(
        codigo,
        data_inicial=data_inicial,
        data_final=data_final,
        ultimos=ultimos,
        reference_date=hoje(),
    )
    acquisition = await fetch_sgs_acquisition(query)
    records = [
        {
            "data": sgs_query.format_date(row.data),
            "valor": str(row.valor) if row.valor is not None else None,
        }
        for row in acquisition.records
    ]
    return records, acquisition.resources[-1].url
