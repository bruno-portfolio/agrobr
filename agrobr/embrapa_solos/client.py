from __future__ import annotations

import sys
import time
from typing import Any

import httpx
import pandas as pd

from agrobr import constants
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.normalize import regions
from agrobr.utils import geo, spatial, wfs
from agrobr.utils.memory import deep_size as _deep_size

from . import _transport, acquisition, models, parser
from . import query as query_module


def acquisition_url(
    query: query_module.SolosQuery, *, offset: int = 0, count: int = 1, hits: bool = False
) -> str:
    properties = list(models.layout_properties(query.product))
    if query.fetch_geometry and not hits:
        properties.append(models.layout_geometry_column(query.product))
    url = geo.build_wfs_url(
        constants.URLS[constants.Fonte.EMBRAPA_SOLOS]["geoserver"],
        constants.EMBRAPA_SOLOS_NAMESPACE,
        constants.EMBRAPA_SOLOS_LAYERS[query.product],
        constants.EMBRAPA_SOLOS_WFS_VERSION,
        properties,
        max_features=count,
        output_format="application/json",
        bbox=query.bbox,
        bbox_crs=query.bbox_crs,
        start_index=None if hits else offset,
        result_type="hits" if hits else None,
    )
    request = httpx.URL(url).copy_add_param(
        "sortBy", constants.EMBRAPA_SOLOS_SORT_FIELDS[query.product] + " A"
    )
    if hits:
        request = request.copy_remove_param("count")
    elif query.fetch_crs is not None:
        request = request.copy_add_param("srsName", query.fetch_crs)
    return str(request)


def _error(reason: str) -> ParseError:
    return ParseError(source="embrapa_solos", parser_version=3, reason=reason)


def _matches(feature: Any, query: query_module.SolosQuery) -> bool:
    properties = feature.properties.model_dump(by_alias=True)
    if query.uf is not None:
        raw = properties["uf"]
        normalized = raw.strip().upper() if isinstance(raw, str) else None
        if normalized not in regions.UFS_VALIDAS or normalized != query.uf:
            return False
    if query.ordem is not None:
        raw = properties["ordem1"]
        if not isinstance(raw, str) or raw.strip().upper() != query.ordem:
            return False
    return True


class _Collection(wfs.PageState[acquisition.SolosPage]):
    def __init__(self, query: query_module.SolosQuery, total: int) -> None:
        super().__init__(lambda: constants.EMBRAPA_SOLOS_MAX_DIAGNOSTIC_EXAMPLES)
        self.query, self.total = query, total
        self.frames: list[pd.DataFrame] = []
        self.geometries: list[dict[str, Any] | None] | None = [] if query.include_geometry else None
        self.frame_bytes = self.geometry_bytes = self.retained_bytes = 0
        self.previous_sort: int | None = None
        self.ordens: set[str] = set()

    def _continuity(self, parsed: Any, overlap: int, offset: int) -> str:
        if overlap and parsed.signatures[0] != self.previous_signature:
            raise _error("Ocorrência de continuidade mudou entre páginas")
        name = constants.EMBRAPA_SOLOS_SORT_FIELDS[self.query.product]
        prior = self.previous_sort
        for feature in parsed.records:
            value = feature.properties.model_dump(by_alias=True)[name]
            if value is not None and prior is not None and value < prior:
                raise _error("Página contradiz ordenação numérica solicitada")
            prior = value
        self.previous_sort = prior
        sequence = self._record_sequence(parsed.signatures, offset)
        return sequence

    def _bbox_selection(self, parsed: Any) -> list[bool]:
        selected = [True] * len(parsed.records)
        if not self.query.fetch_geometry:
            return selected
        if parsed.geometries is None or len(parsed.geometries) != len(parsed.records):
            raise _error("Geometrias divergem das ocorrências recebidas")
        if self.query.bbox is not None:
            for index, geometry in enumerate(parsed.geometries):
                if geometry is None:
                    raise _error(f"Geometria {index} nula impede seleção local por BBOX")
                selected[index] = spatial.geometry_intersects_bbox(geometry, self.query.bbox)
            rejected = [index for index, matches in enumerate(selected) if not matches]
            maximum = constants.EMBRAPA_SOLOS_MAX_DIAGNOSTIC_EXAMPLES
            parsed.diagnostics["bbox_disjoint"] = {
                "count": len(rejected),
                "examples": [{"row_index": index} for index in rejected[:maximum]],
                "examples_omitted": max(0, len(rejected) - maximum),
            }
        return selected

    def check_memory(self, transport: _transport.Transport) -> None:
        metadata = [
            self.pages,
            transport.resources,
            self.diagnostics,
            self.statistics,
            self.warnings,
            self.ambiguities,
            self.windows,
            self.query,
            self.ordens,
            self.accepted_statistics,
            self.accepted_diagnostics,
        ]
        self.retained_bytes = (
            self.frame_bytes
            + self.geometry_bytes
            + _deep_size(metadata)
            + sys.getsizeof(self.frames)
            + sys.getsizeof(self.geometries)
        )
        if self.retained_bytes > constants.EMBRAPA_SOLOS_MAX_RETAINED_BYTES:
            raise SourceUnavailableError(
                source="embrapa_solos",
                last_error="Limite operacional de retenção estimada excedido",
            )

    def consume(
        self, content: bytes, transport: _transport.Transport, offset: int, count: int
    ) -> None:
        start = time.monotonic()
        parsed = parser.parse_page(
            content, product=self.query.product, include_geometry=self.query.fetch_geometry
        )
        received = len(parsed.records)
        overlap = int(self.accepted > 0)
        if (
            parsed.source_rows != received
            or parsed.returned_count != received
            or parsed.reported_count != self.total
            or len(parsed.signatures) != received
        ):
            raise _error("Contagem da página diverge de hits/observações")
        if not overlap < received <= count:
            raise _error("Página vazia, excessiva ou sem progresso")
        bbox_selected = self._bbox_selection(parsed)
        sequence = self._continuity(parsed, overlap, offset)
        if self.query.ordem is not None:
            for feature in parsed.records[overlap:]:
                ordem = feature.properties.model_dump().get("ordem1")
                if isinstance(ordem, str):
                    self.ordens.add(ordem)
        positions = [
            index
            for index, feature in enumerate(parsed.records[overlap:])
            if bbox_selected[index + overlap] and _matches(feature, self.query)
        ]
        selected = [parsed.records[index + overlap] for index in positions]
        frame = parser.build_frame(selected, product=self.query.product)
        if len(frame) != len(positions):
            raise _error("Materialização alterou multiplicidade")
        self.frame_bytes += int(frame.memory_usage(index=True, deep=True).sum())
        if self.geometries is not None:
            if parsed.geometries is None:
                raise _error("Geometrias ausentes na página geo")
            geo_page = [parsed.geometries[index + overlap] for index in positions]
            self.geometry_bytes += _deep_size(geo_page)
        else:
            geo_page = None
        self._diagnostics(parsed, offset)
        self.pages.append(
            acquisition.SolosPage(
                index=len(self.pages),
                resource_index=len(transport.resources) - 1,
                requested_offset=offset,
                requested_count=count,
                received_rows=received,
                validated_rows=received,
                overlap_rows=overlap,
                accepted_start=self.accepted,
                accepted_rows=received - overlap,
                local_matched_rows=len(positions),
                local_rejected_rows=received - overlap - len(positions),
                selected_positions=positions,
                reported_total=self.total,
                sequence_sha256=sequence,
                layout_fingerprint=parsed.layout_fingerprint,
                next_link=parsed.next_link,
                crs=parsed.crs,
                bbox=parsed.bbox,
            )
        )
        self.check_memory(transport)
        self.frames.append(frame)
        if self.geometries is not None and geo_page is not None:
            self.geometries.extend(geo_page)
        self.accepted += received - overlap
        self.received += received
        self.overlap += overlap
        self.selected += len(positions)
        self.parse_ms += int((time.monotonic() - start) * 1000)

    def finish(self, transport: _transport.Transport, after: int) -> acquisition.SolosAcquisition:
        if after != self.total:
            raise _error("Contagem hits mudou durante aquisição")
        self.check_memory(transport)
        start = time.monotonic()
        frame = (
            pd.concat(self.frames, ignore_index=True)
            if self.frames
            else parser.build_frame([], product=self.query.product)
        )
        self.frames.clear()
        reparos, sem_reparo = parser.reparar_texto(frame)
        self.frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        self.check_memory(transport)
        self.parse_ms += int((time.monotonic() - start) * 1000)
        if len(frame) != self.selected:
            raise _error("Quadro final diverge das ocorrências selecionadas")
        truncated = self.accepted < self.total
        warnings = [
            "Hits e overlap não garantem snapshot transacional nem identidade global.",
            *self.warnings,
        ]
        if truncated:
            warnings.append(
                f"Prefixo remoto: {self.accepted} de {self.total} ocorrências; filtro local retornou {len(frame)} linhas e permanece sem completude comprovada."
            )
        if self.ambiguity_count:
            warnings.append(
                "Ocorrências/janelas indistinguíveis preservadas; progresso semântico não comprovado."
            )
        contagens = [
            f"{rotulo} por coluna: " + ", ".join(f"{coluna} {total}" for coluna, total in itens)
            for rotulo, itens in (
                ("reparado", reparos.items()),
                ("com a assinatura e sem reparo", sem_reparo.items()),
            )
            if itens
        ]
        if contagens:
            warnings.append(
                "Texto publicado pela Embrapa com dupla codificação (UTF-8 lido como Latin-1), "
                + "; ".join(contagens)
            )
        coverage = acquisition.SolosCoverage(
            remote=acquisition.RemoteCoverage(
                expected_before=self.total,
                expected_after=after,
                received_rows_with_overlap=self.received,
                validated_rows_with_overlap=self.received,
                overlap_rows=self.overlap,
                accepted_rows=self.accepted,
                max_records=self.query.max_records,
                truncated=truncated,
                counts_consistent=True,
                status="partial" if truncated else "reconciled",
            ),
            local=acquisition.LocalCoverage(
                evaluated_rows=self.accepted,
                matched_rows=self.selected,
                rejected_rows=self.accepted - self.selected,
                returned_rows=len(frame),
                status="unknown" if truncated else "reconciled",
                total_basis="observed_remote_prefix" if truncated else "reconciled_remote_scan",
            ),
            ambiguity_count=self.ambiguity_count,
            ambiguities=self.ambiguities,
            ambiguity_examples_omitted=self.ambiguity_count - len(self.ambiguities),
        )
        result = acquisition.SolosAcquisition(
            query=self.query,
            frame=frame,
            geometries=self.geometries,
            resources=transport.resources,
            pages=self.pages,
            coverage=coverage,
            local_filters={
                "uf": self.query.uf,
                "ordem": self.query.ordem,
                "ordens_observadas": sorted(self.ordens),
                "bbox": self.query.bbox,
                "bbox_predicate": "geometry_intersects" if self.query.bbox else None,
                "basis": "validated_remote_occurrences_before_output_materialization",
            },
            diagnostics=self.diagnostics,
            warnings=warnings,
            fetch_duration_ms=transport.fetch_ms,
            parse_duration_ms=self.parse_ms,
            details={
                "access": "wfs2_json",
                "from_cache": False,
                "resource_index_base": 0,
                "occurrence_offset_base": 0,
                "selected_positions_base": 0,
                "known_decoded_bytes": transport.total_bytes,
                "retained_bytes_estimate": self.retained_bytes,
                "physical_attempts": len(transport.resources),
                "logical_requests": transport.logical_index + 1,
                "statistics": self.statistics,
                "accepted_statistics": self.accepted_statistics,
                "accepted_diagnostics": self.accepted_diagnostics,
                "accepted_statistics_and_diagnostics_basis": "accepted_remote_occurrences_excluding_only_requested_overlap_before_local_filters",
                "statistics_and_diagnostics_basis": "all_validated_received_rows_including_overlap",
                "warning_count": self.warning_count,
                "warning_examples_omitted": self.warning_count - len(self.warnings),
                "bbox_membership": "local_geometry_intersection"
                if self.query.bbox
                else "not_requested",
                "remote_bbox_basis": "service_candidates" if self.query.bbox else "not_requested",
                "page_size_basis": "new_occurrences_plus_one_overlap_after_first_page",
            },
        )
        retained = (
            self.frame_bytes
            + self.geometry_bytes
            + sys.getsizeof(result.geometries)
            + _deep_size(
                [
                    result.query,
                    result.resources,
                    result.pages,
                    result.coverage,
                    result.local_filters,
                    result.diagnostics,
                    result.details,
                    result.warnings,
                ]
            )
        )
        if retained > constants.EMBRAPA_SOLOS_MAX_RETAINED_BYTES:
            raise SourceUnavailableError(
                source="embrapa_solos", last_error="Limite operacional de retenção final excedido"
            )
        result.details["retained_bytes_estimate"] = max(retained, self.retained_bytes)
        return result


async def fetch_acquisition(query: query_module.SolosQuery) -> acquisition.SolosAcquisition:
    selection = query_module.validate_query(query)
    transport = _transport.Transport()
    async with transport.session() as http:
        before = await transport.fetch(http, acquisition_url(selection, hits=True), "hits_before")
        total = acquisition.parse_hits(before)
        del before
        target = total if selection.max_records is None else min(total, selection.max_records)
        collected = _Collection(selection, total)
        await wfs.collect_pages(
            collected,
            target=target,
            page_size=selection.page_size,
            max_pages=constants.EMBRAPA_SOLOS_MAX_PAGES,
            source="embrapa_solos",
            fetch=lambda offset, count: transport.fetch(
                http, acquisition_url(selection, offset=offset, count=count), "page"
            ),
            consume=lambda content, offset, count: collected.consume(
                content, transport, offset, count
            ),
        )
        after = await transport.fetch(http, acquisition_url(selection, hits=True), "hits_after")
        return collected.finish(transport, acquisition.parse_hits(after))
