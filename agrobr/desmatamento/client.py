from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Literal

import httpx
import pandas as pd

from agrobr import _log, constants
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import ParseError, ResourceLimitError
from agrobr.http import wfs_transport
from agrobr.http.settings import get_timeout
from agrobr.utils import geo
from agrobr.utils.memory import deep_size as _deep_size

from . import acquisition, models, parser
from . import query as query_module

logger = _log.get_logger(__name__)


GEOSERVER_BASE = URLS[Fonte.DESMATAMENTO]["geoserver"]

TIMEOUT = get_timeout(read=120.0)


_UF_TO_ESTADO: dict[str, str] = {
    v: k
    for k, v in {
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAPÁ": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARÁ": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPÍRITO SANTO": "ES",
        "GOIÁS": "GO",
        "MARANHÃO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARÁ": "PA",
        "PARAÍBA": "PB",
        "PARANÁ": "PR",
        "PERNAMBUCO": "PE",
        "PIAUÍ": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDÔNIA": "RO",
        "RORAIMA": "RR",
        "SANTA CATARINA": "SC",
        "SÃO PAULO": "SP",
        "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }.items()
}


def _uf_to_estado(uf: str) -> str | None:
    return _UF_TO_ESTADO.get(uf.upper())


def _selection_cql(query: query_module.DesmatamentoQuery) -> str | None:
    filters = []
    if query.year is not None:
        filters.append(f"year={query.year}")
    if query.uf is not None:
        if query.product == "PRODES":
            state = _uf_to_estado(query.uf)
            values = [query.uf] + ([state] if state else [])
            escaped = ",".join("'" + value.replace("'", "''") + "'" for value in values)
            filters.append(f"strToUpperCase(strTrim(state)) IN ({escaped})")
        else:
            filters.append(f"strToUpperCase(strTrim(uf))='{query.uf}'")
    if query.start_date is not None:
        filters.append(f"view_date>='{query.start_date.isoformat()}'")
    if query.end_date is not None:
        filters.append(f"view_date<='{query.end_date.isoformat()}'")
    if query.class_name is not None:
        escaped_class = query.class_name.replace("'", "''")
        filters.append(f"classname='{escaped_class}'")
    return " AND ".join(filters) if filters else None


def _acquisition_url(
    query: query_module.DesmatamentoQuery, *, offset: int, count: int, hits: bool = False
) -> str:
    workspaces = models.PRODES_WORKSPACES if query.product == "PRODES" else models.DETER_WORKSPACES
    layers = models.PRODES_LAYERS if query.product == "PRODES" else models.DETER_LAYERS
    workspace, layer = workspaces[query.biome], layers[query.biome]
    properties = models.layout_properties(query.product, query.biome)
    candidates = (
        ["fid", "uuid"]
        if query.product == "PRODES"
        else ["gid", "view_date", "classname", "municipality"]
    )
    ordering = list(
        dict.fromkeys([field for field in candidates if field in properties] + properties)
    )
    projection = [*properties]
    if query.include_geometry and not hits:
        projection.append(models.layout_geometry_column(query.product, query.biome))
    url = geo.build_wfs_url(
        f"{GEOSERVER_BASE}/{workspace}/ows",
        workspace,
        layer,
        "2.0.0",
        projection,
        max_features=count,
        output_format="application/json",
        cql_filter=_selection_cql(query),
        start_index=None if hits else offset,
        result_type="hits" if hits else None,
    )
    request_url = httpx.URL(url).copy_add_param(
        "sortBy", ",".join(f"{field} A" for field in ordering)
    )
    if query.include_geometry and not hits:
        request_url = request_url.copy_add_param("srsName", "EPSG:4326")
    return str(request_url)


class _TransportContext(
    wfs_transport.Transport[
        acquisition.DesmatamentoResource, Literal["hits_before", "page", "hits_after"]
    ]
):
    def __init__(self) -> None:
        super().__init__(
            source="desmatamento",
            timeout=TIMEOUT,
            initial_role="hits_before",
            resource_factory=acquisition.DesmatamentoResource.model_validate,
            body_limit=lambda: constants.DESMATAMENTO_MAX_BODY_BYTES,
            total_limit=lambda: constants.DESMATAMENTO_MAX_TOTAL_BYTES,
        )

    def _request_fields(self) -> dict[str, Any]:
        return {"size_bytes": None, "sha256": None, "headers": {}}


def _validate_membership(records: list[Any], query: query_module.DesmatamentoQuery) -> None:
    for index, feature in enumerate(records):
        props = feature.properties.model_dump(by_alias=True)
        reason = None
        if query.uf is not None:
            raw = props.get("state" if query.product == "PRODES" else "uf")
            if not isinstance(raw, str) or models.estado_para_uf(raw.strip().upper()) != query.uf:
                reason = "UF"
        if query.year is not None and props.get("year") != query.year:
            reason = "ano"
        if query.class_name is not None and props.get("classname") != query.class_name:
            reason = "classe"
        if query.start_date is not None or query.end_date is not None:
            value = props.get("view_date")
            if (
                value is None
                or (query.start_date is not None and value < query.start_date)
                or (query.end_date is not None and value > query.end_date)
            ):
                reason = "intervalo de datas"
        if reason is not None:
            raise ParseError(
                source="desmatamento",
                parser_version=2,
                reason=f"Ocorrência {index} contradiz filtro {reason}",
            )


def _page_error(reason: str) -> ParseError:
    return ParseError(source="desmatamento", parser_version=2, reason=reason)


async def fetch_acquisition(
    query: query_module.DesmatamentoQuery,
) -> acquisition.DesmatamentoAcquisition:
    selection = query_module.revalidate(query)
    transport = _TransportContext()
    async with transport.session() as http:
        return await _collect(http, transport, selection)


async def fetch_hits(query: query_module.DesmatamentoQuery) -> int:
    selection = query_module.revalidate(query)
    transport = _TransportContext()
    async with transport.session() as http:
        url = _acquisition_url(selection, offset=0, count=0, hits=True)
        return acquisition.parse_hits(await transport.fetch(http, url, "hits_before"))


async def _collect(
    http: httpx.AsyncClient, transport: _TransportContext, query: query_module.DesmatamentoQuery
) -> acquisition.DesmatamentoAcquisition:
    before = await transport.fetch(
        http, _acquisition_url(query, offset=0, count=0, hits=True), "hits_before"
    )
    total = acquisition.parse_hits(before)
    del before
    target = total if query.max_records is None else min(total, query.max_records)
    frames: list[pd.DataFrame] = []
    geometries: list[dict[str, Any] | None] | None = [] if query.include_geometry else None
    pages: list[acquisition.AcceptedPage] = []
    page_details = []
    warnings = [
        "Contagem e continuidade observadas não garantem snapshot transacional ou identidade global."
    ]
    ambiguities = []
    windows: dict[str, tuple[int, int]] = {}
    accepted = received_total = overlap_total = retained_bytes = parse_ms = 0
    previous_signature = None
    while accepted < target:
        if len(pages) >= constants.DESMATAMENTO_MAX_PAGES:
            raise ResourceLimitError(
                "desmatamento",
                f"Limite operacional de {constants.DESMATAMENTO_MAX_PAGES} páginas excedido; "
                "restrinja a seleção ou reduza max_registros",
            )
        overlap = int(accepted > 0)
        offset = accepted - overlap
        count = min(query.page_size, target - accepted) + overlap
        content = await transport.fetch(
            http, _acquisition_url(query, offset=offset, count=count), "page"
        )
        start = time.monotonic()
        parsed = parser.parse_page(
            content,
            product=query.product,
            biome=query.biome,
            include_geometry=query.include_geometry,
        )
        del content
        records = parsed.records
        received = len(records)
        if (
            parsed.source_rows != received
            or parsed.reported_count != total
            or parsed.returned_count != received
        ):
            raise _page_error("Contagem da página diverge de hits/observações")
        if not overlap < received <= count or len(parsed.signatures) != received:
            raise _page_error("Página vazia, excessiva ou sem progresso")
        _validate_membership(records, query)
        if overlap and parsed.signatures[0] != previous_signature:
            raise _page_error("Ocorrência de continuidade mudou entre páginas")
        signature = hashlib.sha256(
            json.dumps(parsed.signatures, separators=(",", ":")).encode()
        ).hexdigest()
        if received > 1 and len(set(parsed.signatures)) == 1:
            ambiguities.append(
                {
                    "kind": "indistinguishable_occurrences_in_window",
                    "page_index": len(pages),
                    "offset": offset,
                    "received_rows": received,
                }
            )
        if signature in windows:
            previous_page, previous_offset = windows[signature]
            ambiguities.append(
                {
                    "kind": "repeated_indistinguishable_window",
                    "previous_page": previous_page,
                    "current_page": len(pages),
                    "previous_offset": previous_offset,
                    "current_offset": offset,
                }
            )
        else:
            windows[signature] = (len(pages), offset)
        previous_signature = parsed.signatures[-1]
        frame = parser.build_frame(records[overlap:], product=query.product, biome=query.biome)
        new_count = received - overlap
        if len(frame) != new_count:
            raise _page_error("Materialização alterou multiplicidade de ocorrências")
        retained_bytes += int(frame.memory_usage(index=True, deep=True).sum())
        if geometries is not None:
            if parsed.geometries is None or len(parsed.geometries) != received:
                raise _page_error("Quantidade de geometrias diverge das ocorrências")
            geo_page = parsed.geometries[overlap:]
            retained_bytes += _deep_size(geo_page)
            geometries.extend(geo_page)
            del geo_page
        if retained_bytes > constants.DESMATAMENTO_MAX_RETAINED_BYTES:
            raise ResourceLimitError(
                "desmatamento",
                "Limite operacional de memória estimada excedido; restrinja a seleção ou reduza "
                "max_registros",
            )
        frames.append(frame)
        pages.append(
            acquisition.AcceptedPage(
                index=len(pages),
                resource_index=len(transport.resources) - 1,
                requested_offset=offset,
                requested_count=count,
                received_rows=received,
                overlap_rows=overlap,
                accepted_start=accepted,
                accepted_rows=new_count,
                reported_total=total,
                layout_fingerprint=parsed.layout_fingerprint,
                sequence_sha256=signature,
                next_link=parsed.next_link,
                crs=parsed.crs,
                bbox=parsed.bbox,
            )
        )
        page_details.append({"page_index": len(pages) - 1, "details": parsed.details})
        warnings.extend(
            f"Página {len(pages) - 1}, offset {offset}: {warning}" for warning in parsed.warnings
        )
        accepted += new_count
        received_total += received
        overlap_total += overlap
        parse_ms += int((time.monotonic() - start) * 1000)
        del parsed, records
    after = await transport.fetch(
        http, _acquisition_url(query, offset=0, count=0, hits=True), "hits_after"
    )
    if acquisition.parse_hits(after) != total:
        raise _page_error("Contagem hits mudou durante a aquisição")
    del after
    frame = (
        pd.concat(frames, ignore_index=True)
        if frames
        else parser.build_frame([], product=query.product, biome=query.biome)
    )
    frames.clear()
    truncated = accepted < total
    if truncated:
        warnings.append(
            f"Limite local: retornadas {accepted} de {total} ocorrências; restrinja filtros ou use max_registros=None."
        )
    if ambiguities:
        warnings.append(
            "Janelas indistinguíveis em offsets diferentes; multiplicidade preservada sem prova de progresso semântico."
        )
    coverage = acquisition.DesmatamentoCoverage(
        expected_rows=total,
        received_rows_with_overlap=received_total,
        validated_rows_with_overlap=received_total,
        overlap_rows=overlap_total,
        accepted_rows=accepted,
        returned_rows=len(frame),
        local_limit=query.max_records,
        truncated=truncated,
        count_reconciled=not truncated,
        status="partial" if truncated else "reconciled",
        ambiguities=ambiguities,
    )
    return acquisition.DesmatamentoAcquisition(
        query=query,
        frame=frame,
        geometries=geometries,
        resources=transport.resources,
        pages=pages,
        coverage=coverage,
        warnings=warnings,
        fetch_duration_ms=transport.fetch_ms,
        parse_duration_ms=parse_ms,
        details={
            "access": "wfs2_json",
            "page_size_basis": "new_occurrences_plus_one_overlap_after_first_page",
            "known_decoded_bytes": transport.total_bytes,
            "retained_bytes_estimate": retained_bytes,
            "resource_index_base": 0,
            "occurrence_offset_base": 0,
            "logical_requests": transport.logical_index + 1,
            "physical_attempts": len(transport.resources),
            "pages": page_details,
            "year_bounds_discovery": False,
            "from_cache": False,
        },
    )
