from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from typing import Literal

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.utils.warnings import warn_once

from . import sgs_acquisition, sgs_models


def format_date(value: date) -> str:
    return f"{value.day:02d}/{value.month:02d}/{value.year:04d}"


def _parse_date(value: str | None, field: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or re.fullmatch(constants.BCB_SGS_DATE_PATTERN, value) is None:
        raise InvalidParameterError(f"{field} deve ter formato DD/MM/AAAA")
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        raise InvalidParameterError(f"{field} contém data inválida") from None


def _resolve_code(codigo: int | str) -> tuple[int, str | None]:
    if isinstance(codigo, str):
        if codigo in sgs_models.SGS_ALIASES_DEPRECIADOS:
            canonico, motivo = sgs_models.SGS_ALIASES_DEPRECIADOS[codigo]
            warn_once(
                f"bcb_sgs_alias:{codigo}",
                f"codigo='{codigo}' está depreciado: {motivo}. Use codigo='{canonico}'",
                category=FutureWarning,
            )
            codigo = canonico
        if codigo not in sgs_models.SGS_SERIES:
            raise InvalidParameterError(f"Serie '{codigo}' nao encontrada")
        return sgs_models.SGS_SERIES[codigo], codigo
    if isinstance(codigo, bool) or not isinstance(codigo, int) or codigo <= 0:
        raise InvalidParameterError("codigo deve ser inteiro positivo ou alias SGS")
    name = next((key for key, value in sgs_models.SGS_SERIES.items() if value == codigo), None)
    return codigo, name


def default_start(reference_date: date) -> date:
    target_year = reference_date.year - constants.BCB_SGS_MAX_WINDOW_YEARS
    if target_year < 1:
        raise InvalidParameterError("Referência não permite o intervalo padrão SGS")
    try:
        return reference_date.replace(year=target_year)
    except ValueError:
        return reference_date.replace(year=target_year, day=28)


def build_query(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    reference_date: date,
) -> sgs_acquisition.SGSQuery:
    code, name = _resolve_code(codigo)
    if type(reference_date) is not date:
        raise InvalidParameterError("reference_date deve ser data civil")
    if ultimos is not None and (
        isinstance(ultimos, bool) or not isinstance(ultimos, int) or ultimos <= 0
    ):
        raise InvalidParameterError("ultimos deve ser inteiro positivo")
    start, end = _parse_date(data_inicial, "data_inicial"), _parse_date(data_final, "data_final")
    original_start, original_end = start, end
    defaults: list[str] = []
    mode: Literal["range", "latest", "server_start"] = "range"
    if start is None and end is None and ultimos is not None:
        mode = "latest"
    elif start is None and end is not None:
        mode = "server_start"
    else:
        if start is None:
            start = default_start(reference_date)
            defaults.append("data_inicial")
        if end is None:
            end = reference_date
            defaults.append("data_final")
    try:
        return sgs_acquisition.SGSQuery(
            codigo=code,
            nome_serie=name,
            mode=mode,
            requested_start=original_start,
            requested_end=original_end,
            inicio=start,
            fim=end,
            ultimos=ultimos,
            reference_date=reference_date,
            defaulted_fields=defaults,
        )
    except pydantic.ValidationError:
        raise InvalidParameterError("Seleção SGS inválida ou intervalo invertido") from None


def _block(
    query: sgs_acquisition.SGSQuery, start: date | None, end: date | None
) -> sgs_acquisition.SGSBlock:
    payload = [query.codigo, query.mode, str(start), str(end), query.ultimos]
    identifier = hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()
    return sgs_acquisition.SGSBlock(
        id=identifier,
        mode=query.mode,
        inicio=start,
        fim=end,
        ultimos=query.ultimos if query.mode == "latest" else None,
    )


def plan_query(query: sgs_acquisition.SGSQuery) -> list[sgs_acquisition.SGSBlock]:
    if query.mode != "range":
        return [_block(query, query.inicio, query.fim)]
    assert query.inicio is not None and query.fim is not None
    result = []
    start = query.inicio
    while start <= query.fim:
        year = min(start.year + constants.BCB_SGS_MAX_WINDOW_YEARS - 1, date.max.year)
        end = min(query.fim, date(year, 12, 31))
        result.append(_block(query, start, end))
        if end == query.fim:
            break
        start = date(end.year + 1, 1, 1)
    return result
