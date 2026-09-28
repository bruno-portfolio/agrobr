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
    query: query_module.FunaiQuery, *, offset: int = 0, count: int = 1, count_probe: bool = False
) -> str:
    properties = list(models.layout_properties())
    if query.fetch_geometry and not count_probe:
        properties.append(models.layout_geometry_column())
    url = geo.build_wfs_url(
        constants.URLS[constants.Fonte.FUNAI]["geoserver"],
        constants.FUNAI_NAMESPACE,
        constants.FUNAI_LAYER,
        constants.FUNAI_WFS_VERSION,
        properties,
        max_features=1 if count_probe else count,
        output_format="application/json",
        bbox=query.bbox,
        bbox_crs=query.bbox_crs,
        start_index=0 if count_probe else offset,
    )
    request = httpx.URL(url).copy_add_param(
        "sortBy", ",".join(name + " A" for name in constants.FUNAI_SORT_FIELDS)
    )
    if query.fetch_crs is not None and not count_probe:
        request = request.copy_add_param("srsName", query.fetch_crs)
    return str(request)


def _error(reason: str) -> ParseError:
    return ParseError(source="funai", parser_version=3, reason=reason)


def _matches(feature: Any, query: query_module.FunaiQuery) -> bool:
    properties = feature.properties.model_dump(by_alias=True)
    if query.uf is not None:
        raw = properties["uf_sigla"]
        tokens = (
            {part.strip().upper() for part in raw.split(",")} if isinstance(raw, str) else set()
        )
        if query.uf not in tokens:
            return False
    return query.fase is None or properties["fase_ti"] == query.fase


class _Collection(
    wfs_collection.ResultsCollection[
        acquisition.FunaiPage, query_module.FunaiQuery, acquisition.FunaiAcquisition
    ]
):
    def __init__(self, query: query_module.FunaiQuery, total: int) -> None:
        super().__init__(
            query,
            total,
            source="funai",
            parser_version=3,
            diagnostic_limit=lambda: constants.FUNAI_MAX_DIAGNOSTIC_EXAMPLES,
            memory_limit=lambda: constants.FUNAI_MAX_RETAINED_BYTES,
            parse_page=parser.parse_page,
            build_frame=parser.build_frame,
            page_factory=acquisition.FunaiPage,
            result_factory=acquisition.FunaiAcquisition,
            details={
                "continuity_basis": "all_18_properties_and_acquired_geometry_excluding_feature_id",
            },
        )

    def _matches(self, feature: Any) -> bool:
        return _matches(feature, self.query)

    def _check_sort(self, parsed: Any) -> None:
        prior = self.previous_sort
        nulls = []
        for index, feature in enumerate(parsed.records):
            properties = feature.properties.model_dump(by_alias=True)
            code, gid = (properties[name] for name in constants.FUNAI_SORT_FIELDS)
            if code is None or gid is None:
                nulls.append(index)
            if code is None:
                continue
            if prior is not None:
                if code < prior[0] or (
                    code == prior[0] and gid is not None and prior[1] is not None and gid < prior[1]
                ):
                    raise _error("Página contradiz ordenação numérica solicitada")
                if code == prior[0] and gid is None:
                    continue
            prior = (code, gid)
        self.previous_sort = prior
        maximum = constants.FUNAI_MAX_DIAGNOSTIC_EXAMPLES
        parsed.diagnostics["nullable_sort_key"] = {
            "count": len(nulls),
            "examples": [{"row_index": index} for index in nulls[:maximum]],
            "examples_omitted": max(0, len(nulls) - maximum),
        }


async def _fetch_count(
    http: httpx.AsyncClient,
    transport: _transport.Transport,
    selection: query_module.FunaiQuery,
    role: Literal["count_before", "count_after"],
) -> tuple[acquisition.FunaiCountCheck, int]:
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
    check = acquisition.FunaiCountCheck(
        role=role,
        resource_index=len(transport.resources) - 1,
        reported_total=total,
        received_rows=received,
        validated_rows=received,
        source_timestamp=parsed.source_timestamp,
        layout_fingerprint=parsed.layout_fingerprint,
    )
    return check, int((time.monotonic() - start) * 1000)


async def fetch_acquisition(query: query_module.FunaiQuery) -> acquisition.FunaiAcquisition:
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
            max_pages=constants.FUNAI_MAX_PAGES,
            source="funai",
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
