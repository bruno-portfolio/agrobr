from __future__ import annotations

import time
from typing import Any, Literal

import httpx

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.utils import geo, wfs, wfs_collection

from . import _transport, acquisition, models, parser
from . import query as query_module


def acquisition_url(
    query: query_module.IncraQuery, *, offset: int = 0, count: int = 1, count_probe: bool = False
) -> str:
    properties = list(models.layout_properties())
    if query.fetch_geometry and not count_probe:
        properties.append(models.layout_geometry_column())
    url = geo.build_wfs_url(
        constants.URLS[constants.Fonte.INCRA]["geoserver"],
        constants.INCRA_NAMESPACE,
        constants.INCRA_LAYER,
        constants.INCRA_WFS_VERSION,
        properties,
        max_features=1 if count_probe else count,
        output_format="application/json",
        bbox=query.bbox,
        bbox_crs=query.bbox_crs,
        start_index=0 if count_probe else offset,
    )
    request = httpx.URL(url).copy_add_param(
        "sortBy", ",".join(name + " A" for name in constants.INCRA_SORT_FIELDS)
    )
    if query.fetch_crs is not None and not count_probe:
        request = request.copy_add_param("srsName", query.fetch_crs)
    return str(request)


def _error(reason: str) -> ParseError:
    return ParseError(source="incra", parser_version=2, reason=reason)


def _matches(feature: Any, query: query_module.IncraQuery) -> bool:
    properties = feature.properties.model_dump(by_alias=True)
    if query.uf is not None:
        raw = properties["sg_uf"]
        if not isinstance(raw, str) or raw.strip().upper() != query.uf:
            return False
    return query.fase is None or properties["ds_fase"] == query.fase


class _Collection(
    wfs_collection.ResultsCollection[
        acquisition.IncraPage, query_module.IncraQuery, acquisition.IncraAcquisition
    ]
):
    def __init__(self, query: query_module.IncraQuery, total: int) -> None:
        super().__init__(
            query,
            total,
            source="incra",
            parser_version=2,
            diagnostic_limit=lambda: constants.INCRA_MAX_DIAGNOSTIC_EXAMPLES,
            memory_limit=lambda: constants.INCRA_MAX_RETAINED_BYTES,
            parse_page=parser.parse_page,
            build_frame=parser.build_frame,
            page_factory=acquisition.IncraPage,
            result_factory=acquisition.IncraAcquisition,
            details={
                "continuity_basis": "all_21_properties_and_acquired_geometry_excluding_feature_id",
                "sort_text_collation_verified": False,
                "sort_validation": "numeric_first_sort_component_order_only_text_inversions_diagnostic",
            },
        )
        self.fases: set[str] = set()

    def _matches(self, feature: Any) -> bool:
        if self.query.fase is not None:
            fase = feature.properties.model_dump(by_alias=True)["ds_fase"]
            if isinstance(fase, str):
                self.fases.add(fase)
        return _matches(feature, self.query)

    def finish(
        self, transport: wfs_collection.TransportState, after: int
    ) -> acquisition.IncraAcquisition:
        result = super().finish(transport, after)
        result.local_filters["fases_observadas"] = sorted(self.fases)
        return result

    def _check_sort(self, parsed: Any) -> None:
        prior = self.previous_sort
        nulls = []
        text_inversions = []
        for index, feature in enumerate(parsed.records):
            properties = feature.properties.model_dump(by_alias=True)
            code, process, name = (properties[name] for name in constants.INCRA_SORT_FIELDS)
            if code is None or process is None or name is None:
                nulls.append(index)
            if code is None:
                continue
            if prior is not None:
                if code < prior[0]:
                    raise _error("Página contradiz ordenação numérica solicitada")
                if (
                    code == prior[0]
                    and process is not None
                    and prior[1] is not None
                    and (
                        process < prior[1]
                        or (
                            process == prior[1]
                            and name is not None
                            and prior[2] is not None
                            and name < prior[2]
                        )
                    )
                ):
                    text_inversions.append(index)
            prior = (code, process, name)
        self.previous_sort = prior
        maximum = constants.INCRA_MAX_DIAGNOSTIC_EXAMPLES
        parsed.diagnostics["nullable_sort_key"] = {
            "count": len(nulls),
            "examples": [{"row_index": index} for index in nulls[:maximum]],
            "examples_omitted": max(0, len(nulls) - maximum),
        }
        parsed.diagnostics["text_sort_python_inversion"] = {
            "count": len(text_inversions),
            "examples": [{"row_index": index} for index in text_inversions[:maximum]],
            "examples_omitted": max(0, len(text_inversions) - maximum),
        }


async def _fetch_count(
    http: httpx.AsyncClient,
    transport: _transport.Transport,
    selection: query_module.IncraQuery,
    role: Literal["count_before", "count_after"],
) -> tuple[acquisition.IncraCountCheck, int]:
    content = await transport.fetch(http, acquisition_url(selection, count_probe=True), role)
    start = time.monotonic()
    parsed = parser.parse_page(content, include_geometry=False)
    total = parsed.reported_count
    received = len(parsed.records)
    if (
        total is None
        or received != min(1, total)
        or parsed.returned_count != received
        or parsed.source_rows != received
    ):
        raise _error("Probe results exige numberMatched e zero ou uma ocorrência validada")
    check = acquisition.IncraCountCheck(
        role=role,
        resource_index=len(transport.resources) - 1,
        reported_total=total,
        received_rows=received,
        validated_rows=received,
        source_timestamp=parsed.source_timestamp,
        layout_fingerprint=parsed.layout_fingerprint,
    )
    return check, int((time.monotonic() - start) * 1000)


async def fetch_acquisition(query: query_module.IncraQuery) -> acquisition.IncraAcquisition:
    selection = query_module.validate_query(query)
    transport = _transport.Transport()
    async with transport.session() as http:
        before, parse_ms = await _fetch_count(http, transport, selection, "count_before")
        total = before.reported_total
        target = total if selection.max_records is None else min(total, selection.max_records)
        collected = _Collection(selection, total)
        collected.count_checks.append(before)
        collected.parse_ms += parse_ms
        collected.check_memory(transport)
        await wfs.collect_pages(
            collected,
            target=target,
            page_size=selection.page_size,
            max_pages=constants.INCRA_MAX_PAGES,
            source="incra",
            fetch=lambda offset, count: transport.fetch(
                http, acquisition_url(selection, offset=offset, count=count), "page"
            ),
            consume=lambda content, offset, count: collected.consume(
                content, transport, offset, count
            ),
        )
        after, parse_ms = await _fetch_count(http, transport, selection, "count_after")
        collected.count_checks.append(after)
        collected.parse_ms += parse_ms
        return collected.finish(transport, after.reported_total)
