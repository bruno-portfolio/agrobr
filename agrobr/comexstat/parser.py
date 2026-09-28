from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

import structlog
from pydantic import ValidationError

from agrobr import constants
from agrobr.comexstat import _csv, _retention, _scan, models
from agrobr.exceptions import ResourceLimitError

if TYPE_CHECKING:
    from agrobr.comexstat.query import ComexQuery

logger = structlog.get_logger()

PARSER_VERSION = 2
ParsedResource = models.ParsedResource


def _columns(query: ComexQuery) -> tuple[tuple[str, ...], dict[str, str]]:
    names: tuple[str, ...]
    if query.agregacao == "mensal":
        names = ("ano", "mes", "ncm", "uf", "kg_liquido", "valor_fob_usd", "volume_ton")
        if query.fluxo == "importacao":
            names += ("valor_frete_usd", "valor_seguro_usd")
    else:
        raw = (
            constants.COMEXSTAT_EXPORT_PROPERTIES
            if query.fluxo == "exportacao"
            else constants.COMEXSTAT_IMPORT_PROPERTIES
        )
        names = tuple(constants.COMEXSTAT_RENAME_MAP[name] for name in raw)
    types = {
        name: (
            "Int64"
            if name in ("ano", "mes", "kg_liquido", "qtd_estatistica")
            else "float64"
            if name.startswith("valor_") or name == "volume_ton"
            else "string"
        )
        for name in names
    }
    return names, types


def parse_resource(file: BinaryIO, query: ComexQuery) -> models.ParsedResource:
    scanner = _scan.ResourceScan(query)
    encoding, attempts = _csv.detect_encoding(file)
    model: type[models.ExportRecord] = (
        models.ExportRecord if query.fluxo == "exportacao" else models.ImportRecord
    )
    expected = (
        constants.COMEXSTAT_EXPORT_PROPERTIES
        if query.fluxo == "exportacao"
        else constants.COMEXSTAT_IMPORT_PROPERTIES
    )
    with _csv.open_rows(file, encoding, expected) as rows:
        for ordinal, raw in rows:
            try:
                scanner.accept(model.model_validate(raw))
            except (ValidationError, ValueError) as exc:
                raise _csv.error(f"Registro {ordinal}: {exc}") from exc
        try:
            selected = scanner.output_rows()
        except ValueError as exc:
            raise _csv.error(f"Agregação não representável: {exc}") from exc
        columns, dtypes = _columns(query)
        frame = scanner.memory.build(selected, columns, dtypes)
        details: dict[str, object] = {
            "parser_version": PARSER_VERSION,
            "encoding": encoding,
            "encoding_attempts": attempts,
            "layout_fingerprint": rows.fingerprint,
            "source_columns": list(rows.header),
            "source_rows": rows.records,
            "validated_rows": scanner.validated_rows,
            "selected_rows": scanner.selected_rows,
            "output_rows": len(frame),
            "eof_reached": rows.eof,
            "aggregation": query.agregacao,
            "null_aggregation": "any_null_propagates",
            "statistics": scanner.statistics(),
            "retained_bytes_estimate": scanner.memory.peak,
            "frame_resident_bytes": scanner.memory.frame_bytes(frame),
            "memory_basis": "pooled_literal_objects_rows_groups_arrays_column_bridge_plus_8MiB_scratch_not_RSS",
            "max_linhas": query.max_linhas,
            "max_memoria_bytes": query.max_memoria_bytes,
            "row_limit_basis": "selected_occurrences_before_aggregation",
            "order": "source_occurrences" if query.agregacao == "detalhado" else "ano_mes_ncm_uf",
            "money_sum_basis": "exact_integer_coefficient_decimal_scale_then_binary64",
        }
    logger.info(
        "comexstat_parsed", source_rows=rows.records, output_rows=len(frame), encoding=encoding
    )
    return models.ParsedResource(frame, details)


def parse_dictionary(
    file: BinaryIO,
    *,
    tabela: str,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> models.ParsedResource:
    memory = _retention.Retention(max_memoria_bytes)
    encoding, attempts = _csv.detect_encoding(file)
    expected = constants.COMEXSTAT_DICTIONARY_PROPERTIES[tabela]
    model = models.DICTIONARY_MODELS[tabela]
    retained: list[tuple[object, ...]] = []
    with _csv.open_rows(file, encoding, expected) as rows:
        for ordinal, raw in rows:
            try:
                record = model.model_validate(raw)
            except ValidationError as exc:
                raise _csv.error(f"Registro {ordinal}: {exc}") from exc
            if max_linhas is not None and ordinal > max_linhas:
                raise ResourceLimitError(source="comexstat", reason="Dicionário excede max_linhas")
            memory.reserve(128 + len(expected) * 16)
            retained.append(
                tuple(
                    memory.intern(getattr(record, constants.COMEXSTAT_RENAME_MAP[name]))
                    for name in expected
                )
            )
        columns = tuple(constants.COMEXSTAT_RENAME_MAP[name] for name in expected)
        frame = memory.build(retained, columns, dict.fromkeys(columns, "string"))
        details: dict[str, object] = {
            "parser_version": PARSER_VERSION,
            "encoding": encoding,
            "encoding_attempts": attempts,
            "layout_fingerprint": rows.fingerprint,
            "source_columns": list(rows.header),
            "source_rows": rows.records,
            "validated_rows": rows.records,
            "selected_rows": rows.records,
            "output_rows": len(frame),
            "eof_reached": rows.eof,
            "tabela": tabela,
            "retained_bytes_estimate": memory.peak,
            "frame_resident_bytes": memory.frame_bytes(frame),
            "memory_basis": "pooled_literals_tuples_arrays_column_bridge_plus_8MiB_scratch_not_RSS",
            "order": "source_occurrences",
            "primary_key": [],
        }
    return models.ParsedResource(frame, details)
