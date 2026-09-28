from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any, Generic, Protocol, TypeVar

import pandas as pd

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.utils import spatial, wfs
from agrobr.utils.memory import deep_size as _deep_size


class Query(Protocol):
    @property
    def include_geometry(self) -> bool: ...
    @property
    def fetch_geometry(self) -> bool: ...
    @property
    def bbox(self) -> tuple[float, float, float, float] | None: ...
    @property
    def max_records(self) -> int | None: ...
    @property
    def uf(self) -> str | None: ...
    @property
    def fase(self) -> str | None: ...


class TransportState(Protocol):
    resources: list[Any]
    fetch_ms: int
    total_bytes: int
    logical_index: int


class AcquisitionState(Protocol):
    query: Any
    geometries: list[dict[str, Any] | None] | None
    resources: list[Any]
    pages: list[Any]
    count_checks: list[Any]
    coverage: Any
    local_filters: dict[str, Any]
    diagnostics: dict[str, Any]
    details: dict[str, Any]
    warnings: list[str]


PageT = TypeVar("PageT")
QueryT = TypeVar("QueryT", bound=Query)
AcquisitionT = TypeVar("AcquisitionT", bound=AcquisitionState)


class ResultsCollection(wfs.PageState[PageT], Generic[PageT, QueryT, AcquisitionT]):
    def __init__(
        self,
        query: QueryT,
        total: int,
        *,
        source: str,
        parser_version: int,
        diagnostic_limit: Callable[[], int],
        memory_limit: Callable[[], int],
        parse_page: Callable[..., Any],
        build_frame: Callable[[list[Any]], pd.DataFrame],
        page_factory: Callable[..., PageT],
        result_factory: Callable[..., AcquisitionT],
        details: dict[str, Any],
    ) -> None:
        super().__init__(diagnostic_limit)
        self.source, self.parser_version = source, parser_version
        self._memory_limit = memory_limit
        self._parse_page, self._build_frame = parse_page, build_frame
        self._page_factory, self._result_factory = page_factory, result_factory
        self._details = details
        self.query, self.total = query, total
        self.frames: list[pd.DataFrame] = []
        self.geometries: list[dict[str, Any] | None] | None = [] if query.include_geometry else None
        self.count_checks: list[Any] = []
        self.frame_bytes = self.geometry_bytes = self.retained_bytes = 0
        self.previous_sort: tuple[Any, ...] | None = None
        self.previous_identifier_signature: str | None = None
        self.previous_identifier: str | None = None
        self.identifier_changes = 0
        self.identifier_examples: list[dict[str, Any]] = []

    def _error(self, reason: str) -> ParseError:
        return ParseError(source=self.source, parser_version=self.parser_version, reason=reason)

    def _check_sort(self, parsed: Any) -> None:
        raise NotImplementedError

    def _matches(self, feature: Any) -> bool:
        raise NotImplementedError

    def _continuity(self, parsed: Any, overlap: int, offset: int) -> str:
        if overlap and parsed.signatures[0] != self.previous_signature:
            raise self._error("Ocorrência de continuidade mudou entre páginas")
        if overlap and parsed.identifier_signatures[0] != self.previous_identifier_signature:
            self.identifier_changes += 1
            if len(self.identifier_examples) < self._diagnostic_limit():
                self.identifier_examples.append(
                    {
                        "page_index": len(self.pages),
                        "requested_offset": offset,
                        "previous_feature_id": self.previous_identifier,
                        "received_feature_id": parsed.records[0].id,
                    }
                )
        self._check_sort(parsed)
        sequence = self._record_sequence(parsed.signatures, offset)
        self.previous_identifier_signature = parsed.identifier_signatures[-1]
        self.previous_identifier = parsed.records[-1].id
        return sequence

    def _bbox_selection(self, parsed: Any) -> list[bool]:
        selected = [True] * len(parsed.records)
        if not self.query.fetch_geometry:
            return selected
        if parsed.geometries is None or len(parsed.geometries) != len(parsed.records):
            raise self._error("Geometrias divergem das ocorrências recebidas")
        if self.query.bbox is not None:
            for index, geometry in enumerate(parsed.geometries):
                if geometry is None:
                    raise self._error(f"Geometria {index} nula impede seleção local por BBOX")
                selected[index] = spatial.geometry_intersects_bbox(geometry, self.query.bbox)
            rejected = [index for index, matches in enumerate(selected) if not matches]
            maximum = self._diagnostic_limit()
            parsed.diagnostics["bbox_disjoint"] = {
                "count": len(rejected),
                "examples": [{"row_index": index} for index in rejected[:maximum]],
                "examples_omitted": max(0, len(rejected) - maximum),
            }
        return selected

    def check_memory(self, transport: TransportState) -> None:
        metadata = [
            self.pages,
            self.count_checks,
            transport.resources,
            self.diagnostics,
            self.statistics,
            self.warnings,
            self.ambiguities,
            self.windows,
            self.query,
            self.accepted_statistics,
            self.accepted_diagnostics,
            self.identifier_examples,
            self.previous_identifier,
            self.previous_identifier_signature,
            self.previous_signature,
            self.previous_sort,
        ]
        self.retained_bytes = (
            self.frame_bytes
            + self.geometry_bytes
            + _deep_size(metadata)
            + sys.getsizeof(self.frames)
            + sys.getsizeof(self.geometries)
        )
        if self.retained_bytes > self._memory_limit():
            raise SourceUnavailableError(
                source=self.source,
                last_error="Limite operacional de retenção estimada excedido",
            )

    def consume(self, content: bytes, transport: TransportState, offset: int, count: int) -> None:
        start = time.monotonic()
        parsed = self._parse_page(content, include_geometry=self.query.fetch_geometry)
        received = len(parsed.records)
        overlap = int(self.accepted > 0)
        if (
            parsed.source_rows != received
            or parsed.returned_count != received
            or parsed.reported_count != self.total
            or len(parsed.signatures) != received
            or len(parsed.identifier_signatures) != received
        ):
            raise self._error("Contagem da página diverge do probe/observações")
        if not overlap < received <= count:
            raise self._error("Página vazia, excessiva ou sem progresso")
        bbox_selected = self._bbox_selection(parsed)
        sequence = self._continuity(parsed, overlap, offset)
        positions = [
            index
            for index, feature in enumerate(parsed.records[overlap:])
            if bbox_selected[index + overlap] and self._matches(feature)
        ]
        selected = [parsed.records[index + overlap] for index in positions]
        frame = self._build_frame(selected)
        if len(frame) != len(positions):
            raise self._error("Materialização alterou multiplicidade")
        self.frame_bytes += int(frame.memory_usage(index=True, deep=True).sum())
        if self.geometries is not None:
            if parsed.geometries is None:
                raise self._error("Geometrias ausentes na página geo")
            geo_page = [parsed.geometries[index + overlap] for index in positions]
            self.geometry_bytes += _deep_size(geo_page)
        else:
            geo_page = None
        self._diagnostics(parsed, offset)
        self.pages.append(
            self._page_factory(
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
                source_timestamp=parsed.source_timestamp,
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

    def finish(self, transport: TransportState, after: int) -> AcquisitionT:
        if after != self.total:
            raise self._error("Contagem results mudou durante aquisição")
        self.check_memory(transport)
        start = time.monotonic()
        frame = pd.concat(self.frames, ignore_index=True) if self.frames else self._build_frame([])
        self.frames.clear()
        self.frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        self.check_memory(transport)
        self.parse_ms += int((time.monotonic() - start) * 1000)
        if len(frame) != self.selected:
            raise self._error("Quadro final diverge das ocorrências selecionadas")
        truncated = self.accepted < self.total
        warnings = [
            "Contagens e overlap não garantem snapshot transacional nem identidade global.",
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
        if self.identifier_changes:
            warnings.append(
                "Feature.id mudou em ocorrências de overlap com conteúdo igual; IDs recebidos foram preservados e não são usados como chave de continuidade."
            )
        coverage = {
            "remote": {
                "expected_before": self.total,
                "expected_after": after,
                "received_rows_with_overlap": self.received,
                "validated_rows_with_overlap": self.received,
                "overlap_rows": self.overlap,
                "accepted_rows": self.accepted,
                "max_records": self.query.max_records,
                "truncated": truncated,
                "counts_consistent": True,
                "status": "partial" if truncated else "reconciled",
            },
            "local": {
                "evaluated_rows": self.accepted,
                "matched_rows": self.selected,
                "rejected_rows": self.accepted - self.selected,
                "returned_rows": len(frame),
                "status": "unknown" if truncated else "reconciled",
                "total_basis": "observed_remote_prefix" if truncated else "reconciled_remote_scan",
            },
            "ambiguity_count": self.ambiguity_count,
            "ambiguities": self.ambiguities,
            "ambiguity_examples_omitted": self.ambiguity_count - len(self.ambiguities),
        }
        result = self._result_factory(
            query=self.query,
            frame=frame,
            geometries=self.geometries,
            resources=transport.resources,
            pages=self.pages,
            count_checks=self.count_checks,
            coverage=coverage,
            local_filters={
                "uf": self.query.uf,
                "fase": self.query.fase,
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
                "count_basis": "wfs2_results_number_matched",
                "page_counters_basis": "page_only",
                "count_probe_received_rows": sum(
                    check.received_rows for check in self.count_checks
                ),
                "count_probe_validated_rows": sum(
                    check.validated_rows for check in self.count_checks
                ),
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
                **self._details,
                "overlap_identifier_changes": {
                    "count": self.identifier_changes,
                    "examples": self.identifier_examples,
                    "examples_omitted": self.identifier_changes - len(self.identifier_examples),
                },
                "feature_id_stability_proven": False,
                "source_edition": None,
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
                    result.count_checks,
                    result.coverage,
                    result.local_filters,
                    result.diagnostics,
                    result.details,
                    result.warnings,
                ]
            )
        )
        if retained > self._memory_limit():
            raise SourceUnavailableError(
                source=self.source, last_error="Limite operacional de retenção final excedido"
            )
        result.details["retained_bytes_estimate"] = max(retained, self.retained_bytes)
        return result
