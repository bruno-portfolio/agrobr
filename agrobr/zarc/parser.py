from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.normalize.encoding import detect_encoding_chain

from . import models
from ._buffers import ZarcBuffers

if TYPE_CHECKING:
    from .query import ZarcQuery

logger = _log.get_logger(__name__)
PARSER_VERSION = 2


@dataclass(frozen=True)
class ZarcParsedTable:
    frame: pd.DataFrame
    details: dict[str, Any]


def _fail(reason: str) -> ParseError:
    return ParseError(source="zarc", parser_version=PARSER_VERSION, reason=reason)


def _normalize_cultura(value: object) -> str:
    return models.normalize_cultura(value)


def _header(reader: Any) -> list[str]:
    try:
        headers: list[str] = next(reader)
    except StopIteration as exc:
        raise _fail("CSV sem cabeçalho") from exc
    if len(headers) != len(set(headers)):
        raise _fail("Cabeçalho CSV com nomes duplicados")
    missing = set(constants.ZARC_CSV_COLUMNS) - set(headers)
    extra = set(headers) - set(constants.ZARC_CSV_COLUMNS)
    if missing or extra:
        raise _fail(f"Cabeçalho: colunas ausentes {sorted(missing)}; extras {sorted(extra)}")
    return headers


def _record(values: dict[str, Any], index: int, line: int) -> models.ZarcRecord:
    try:
        return models.ZarcRecord.model_validate(values)
    except pydantic.ValidationError as exc:
        errors = []
        for error in exc.errors(include_url=False, include_input=False):
            location = error["loc"]
            field = ".".join(str(part) for part in location) or "SafraIni/Fin"
            if location and location[0] == "riscos" and len(location) > 1:
                field = f"dec{int(location[1]) + 1}"
            errors.append(f"{field}: {error['msg']}")
        raise _fail(f"Registro {index}, linha física {line}: {'; '.join(errors)}") from exc


def _matches(record: models.ZarcRecord, culture: str, query: ZarcQuery | None) -> bool:
    if query is None:
        return True
    if query.cultura is not None and culture != query.cultura:
        return False
    if query.uf is not None and record.uf != query.uf:
        return False
    if query.solo is not None and record.solo_codigo != query.solo:
        return False
    if query.ciclo is not None and record.ciclo_codigo != query.ciclo:
        return False
    if query.municipio is not None:
        value = query.municipio
        if isinstance(value, int) or value.isascii() and value.isdigit():
            return record.geocodigo == str(value)
        return models.normalize_municipio(value) in models.normalize_municipio(record.municipio)
    return True


def _fingerprint(headers: list[str]) -> dict[str, Any]:
    structure = {"headers": headers, "delimiter": ";", "version": 1, "parser_version": 2}
    raw = json.dumps(structure, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return {"algorithm": "sha256", **structure, "sha256": hashlib.sha256(raw.encode()).hexdigest()}


def _expected(record: models.ZarcRecord, season: str | None, index: int) -> None:
    if season is None:
        return
    observed = (
        "perene" if record.safra_inicio == "" else f"{record.safra_inicio}/{record.safra_fim}"
    )
    if season != observed:
        raise _fail(f"Registro {index}: safra publicada {observed!r} difere do recurso {season!r}")


def _missing_culture(
    culture: str, expected_safra: str | None, cultures: Collection[str]
) -> InvalidParameterError:
    hint = ""
    if expected_safra != "perene" and culture in models.CULTURAS_PERENES:
        hint = f"; {culture!r} está na tábua perene: use safra='perene'"
    elif culture in models.SAFRAS_POR_CULTURA:
        first, last = models.SAFRAS_POR_CULTURA[culture]
        seasons = f"na safra {first}" if first == last else f"nas safras {first} a {last}"
        hint = f"; {culture!r} aparece {seasons} (tábuas publicadas até 23/09/2026)"
    hint += _renamed_hint(culture, cultures)
    return InvalidParameterError(
        f"Cultura {culture!r} não encontrada na tábua; aliases disponíveis: {sorted(cultures)}{hint}"
    )


def _renamed_hint(culture: str, cultures: Collection[str]) -> str:
    renamed = models.RENOMEACOES_2024_2025
    equivalents = [
        f"{new!r} com {column} {value!r}"
        for old, (new, column, value) in renamed.items()
        if old == culture and new in cultures
    ] + [repr(old) for old, (new, _, _) in renamed.items() if new == culture and old in cultures]
    if not equivalents:
        return ""
    return (
        f"; nesta tábua, a mesma cultura sai como {' e '.join(equivalents)} "
        "(o ZARC renomeou as culturas na safra 2024/2025)"
    )


def _scan(
    reader: Any, headers: list[str], query: ZarcQuery | None, expected_safra: str | None
) -> ZarcParsedTable:
    identity = [
        (index, constants.ZARC_CSV_TO_OUTPUT[name])
        for index, name in enumerate(headers)
        if name not in models.DEC_COLS
    ]
    risks = [headers.index(name) for name in models.DEC_COLS]
    buffers = ZarcBuffers()
    distributions: list[Counter[int | None]] = [Counter() for _ in risks]
    cultures: Counter[str] = Counter()
    seasons: Counter[str] = Counter()
    culture_cache: dict[str, str] = {}
    source_rows = 0
    blank_lines = 0
    for source_rows, row in enumerate(reader, 1):
        if not row:
            blank_lines += 1
            continue
        if len(row) != len(headers):
            raise _fail(
                f"Registro {source_rows}, linha física {reader.line_num}: largura {len(row)}, esperada {len(headers)}"
            )
        values: dict[str, Any] = {name: row[index] for index, name in identity}
        values["riscos"] = tuple(row[index] for index in risks)
        record = _record(values, source_rows, reader.line_num)
        _expected(record, expected_safra, source_rows)
        culture = culture_cache.get(record.cultura_original)
        if culture is None:
            culture = models.normalize_cultura(record.cultura_original)
            culture_cache[record.cultura_original] = culture
        season = models.derive_safra(record.safra_inicio, record.safra_fim)
        cultures[culture] += 1
        seasons[season] += 1
        for distribution, value in zip(distributions, record.riscos, strict=True):
            distribution[value] += 1
        if _matches(record, culture, query):
            buffers.append(record, culture, season, source_rows)
    if query is not None and query.cultura is not None and query.cultura not in cultures:
        raise _missing_culture(query.cultura, expected_safra, cultures)
    frame = buffers.frame()
    details = {
        "parser_version": PARSER_VERSION,
        "layout_fingerprint": _fingerprint(headers),
        "source_rows": source_rows,
        "validated_rows": source_rows - blank_lines,
        "csv_records_read": source_rows,
        "selected_rows": len(frame),
        "blank_physical_lines": blank_lines,
        "physical_lines": reader.line_num,
        "eof_reached": True,
        "risk_statistics": {
            name: {
                "values": {
                    str(value): count for value, count in distribution.items() if value is not None
                },
                "empty_count": distribution[None],
                "zero_count": distribution[0],
            }
            for name, distribution in zip(models.DEC_COLS, distributions, strict=True)
        },
        "cultures": dict(cultures),
        "culturas_observadas": sorted(cultures),
        "seasons": dict(seasons),
        "expected_resource_season": expected_safra,
        "origin_position": {
            "base": 1,
            "scope": "CSV body SHA256",
            "assigned_before_filter": True,
            "blank_physical_lines_excluded": False,
        },
        "multiplicity": "all occurrences preserved; semantic uniqueness not asserted",
        "warnings": [] if source_rows else ["CSV com cabeçalho íntegro e sem registros"],
    }
    return ZarcParsedTable(frame, details)


def parse_tabua_risco_bundle(
    content: bytes, *, query: ZarcQuery | None = None, expected_safra: str | None = None
) -> ZarcParsedTable:
    if not content:
        raise _fail("CSV sem bytes")
    try:
        encoding = detect_encoding_chain(content)
        with io.TextIOWrapper(io.BytesIO(content), encoding=encoding, newline="") as stream:
            reader = csv.reader(stream, delimiter=";", strict=True)
            result = _scan(reader, _header(reader), query, expected_safra)
    except (csv.Error, UnicodeError, LookupError) as exc:
        raise _fail(f"CSV inválido: {type(exc).__name__}") from exc
    result.details["encoding"] = encoding
    logger.info(
        "zarc_parse_ok", records=len(result.frame), validated=result.details["validated_rows"]
    )
    return result


def parse_tabua_risco(content: bytes) -> pd.DataFrame:
    return parse_tabua_risco_bundle(content).frame
