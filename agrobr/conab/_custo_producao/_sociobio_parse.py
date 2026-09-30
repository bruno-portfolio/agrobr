from __future__ import annotations

import math
import re
from typing import Any, Literal

import pandas as pd
from pydantic import ValidationError

from agrobr.contracts import conab_custos
from agrobr.exceptions import ParseError
from agrobr.normalize import numeric

from . import models
from ._context import key
from ._parse import percentage_format
from ._sociobio_workbook import AbaSociobio, fail


def columns(sheet: AbaSociobio) -> tuple[int, dict[str, tuple[int, int]], dict[str, str]]:
    item = [
        (r, c, value)
        for r, row in enumerate(sheet.linhas[:25])
        for c, value in enumerate(row)
        if isinstance(value, str) and key(value).startswith(("DISCRIMINACAO", "ESPECIFICACAO"))
    ]
    if len(item) != 1:
        raise fail(sheet.nome, "Cabeçalho de descrição ausente ou ambíguo")
    header_row, item_col, _ = item[0]
    mapping = {
        "item": sheet.mesclas_cabecalho.get((header_row, item_col), (item_col, item_col + 1))
    }
    units = {}
    monetary: dict[int, tuple[int, str]] = {}
    start = header_row + 1
    for r in range(header_row, min(header_row + 4, len(sheet.linhas))):
        for c, value in enumerate(sheet.linhas[r]):
            if not isinstance(value, str):
                continue
            literal = " ".join(value.split())
            normalized = key(literal)
            if re.match(r"\(?(?:R\$\s*/|CUSTO\s+(?:POR|/)\s*)", normalized):
                monetary[c] = (r, literal)
                start = max(start, r + 1)
            field = (
                "participacao_ct_pct"
                if "PARTICIPACAO" in normalized and "CT" in normalized
                else "participacao_pct"
                if normalized in {"%", "(%)"} or "PARTICIPACAO" in normalized and "CV" in normalized
                else None
            )
            if field is not None:
                span = sheet.mesclas_cabecalho.get((r, c), (c, c + 1))
                if field in mapping and mapping[field] != span:
                    raise fail(sheet.nome, f"Cabeçalho duplicado: {field}")
                mapping[field] = span
                start = max(start, r + 1)
        if monetary and "participacao_pct" in mapping:
            break
    if not 1 <= len(monetary) <= 2:
        raise fail(sheet.nome, f"Cabeçalhos monetários não reconhecidos: {len(monetary)}")
    for field, (col, (row, literal)) in zip(
        ("valor", "valor_unidade_produto"), sorted(monetary.items())
    ):
        mapping[field] = sheet.mesclas_cabecalho.get((row, col), (col, col + 1))
        units[field] = literal
    _validate_header_columns(sheet, header_row, start, mapping)
    return start, mapping, units


def _validate_header_columns(
    sheet: AbaSociobio, first_row: int, end_row: int, mapping: dict[str, tuple[int, int]]
) -> None:
    used = {col for first, end in mapping.values() for col in range(first, end)}
    for row_number in range(first_row, end_row):
        for col, value in enumerate(sheet.linhas[row_number]):
            if min(used) <= col <= max(used) and col not in used and value not in (None, ""):
                raise fail(
                    sheet.nome,
                    f"Coluna sem mapeamento em R{row_number + 1}C{col + 1}: {value!r}",
                )


def _number(value: Any, sheet: AbaSociobio, row: int, col: int, percentage: bool) -> float | None:
    if value is None or value == "" or isinstance(value, str) and value.strip() == "-":
        return None
    result = (
        None
        if isinstance(value, bool) or not isinstance(value, (str, int, float))
        else numeric.parse_numeric_br(value)
    )
    if result is None or not math.isfinite(result):
        raise fail(sheet.nome, f"Medida inválida em R{row + 1}C{col + 1}: {value!r}")
    try:
        if (
            percentage
            and not isinstance(value, str)
            and percentage_format(sheet.formatos.get((row, col), ""))
        ):
            result *= 100
    except ParseError as error:
        raise fail(sheet.nome, str(error)) from error
    if not math.isfinite(result):
        raise fail(sheet.nome, f"Medida não finita em R{row + 1}C{col + 1}")
    return result


def _row_measures(
    sheet: AbaSociobio, row_number: int, mapping: dict[str, tuple[int, int]]
) -> tuple[dict[str, float | None], dict[str, str]]:
    row = sheet.linhas[row_number]
    values = {}
    cells = {}
    for field, (first, end) in mapping.items():
        if field == "item":
            continue
        occupied = [
            (col, row[col])
            for col in range(first, min(end, len(row)))
            if row[col] not in (None, "")
        ]
        if len(occupied) > 1:
            raise fail(sheet.nome, f"Mais de um valor na coluna {field}, linha {row_number + 1}")
        col, value = occupied[0] if occupied else (first, None)
        values[field] = _number(value, sheet, row_number, col, field.startswith("participacao"))
        if occupied:
            cells[field] = f"R{row_number + 1}C{col + 1}"
    used = {col for first, end in mapping.values() for col in range(first, end)}
    for col, value in enumerate(row):
        if (
            col not in used
            and value not in (None, "")
            and (not isinstance(value, str) or numeric.parse_numeric_br(value) is not None)
        ):
            raise fail(
                sheet.nome,
                f"Medida fora dos cabeçalhos mapeados em R{row_number + 1}C{col + 1}: {value!r}",
            )
    return values, cells


def _line_type(
    label: str, values: dict[str, float | None], sheet: str, row: int
) -> Literal["item", "total", "secao"] | None:
    norm = key(label)
    if re.match(r"^[IVX]+\s*[-–]", norm) or norm == "GESTAO DA PROPRIEDADE FAMILIAR":
        return "secao"
    if re.match(r"^\d+(?:\.\d+)?(?:\s*[-–]|\s+\D)", norm):
        return "item"
    if re.match(r"^(TOTAL|CUSTO)\b", norm):
        return "total"
    if any(value is not None for value in values.values()):
        raise fail(sheet, f"Linha com medida sem tipo reconhecido: {row}: {label!r}")
    return None


def parse_selected(
    sheet: AbaSociobio, context: models.ContextoSociobio
) -> models.ResultadoSociobio:
    start, mapping, units = columns(sheet)
    base = context.model_dump(include=set(models.DadosContextoSociobio.model_fields))
    observations = []
    source_cells = []
    notes = []
    section = None
    for row_number in range(start, len(sheet.linhas)):
        row = sheet.linhas[row_number]
        label = row[mapping["item"][0]]
        values, cells = _row_measures(sheet, row_number, mapping)
        if label in (None, "") and not any(value is not None for value in values.values()):
            continue
        if not isinstance(label, str) or not label.strip():
            raise fail(sheet.nome, f"Medida sem descrição na linha {row_number + 1}")
        kind = _line_type(label, values, sheet.nome, row_number + 1)
        if kind is None:
            notes.append({"linha": row_number + 1, "texto": label})
            continue
        if kind == "secao":
            section = label
        try:
            observations.append(
                models.ObservacaoSociobio(
                    **base,
                    secao=section,
                    item=label,
                    tipo_linha=kind,
                    linha=row_number + 1,
                    valor=values.get("valor"),
                    unidade_valor=units["valor"],
                    valor_unidade_produto=values.get("valor_unidade_produto"),
                    unidade_produto=units.get("valor_unidade_produto"),
                    participacao_pct=values.get("participacao_pct"),
                    participacao_ct_pct=values.get("participacao_ct_pct"),
                )
            )
        except ValidationError as error:
            raise fail(sheet.nome, f"Linha {row_number + 1}: {error}") from error
        source_cells.append({"linha": row_number + 1, "celulas": cells})
    if not observations:
        raise fail(sheet.nome, "Aba sem observações reconhecidas")
    return models.ResultadoSociobio(
        contexto=context,
        observacoes=observations,
        detalhes={
            "unidades": units,
            "colunas": mapping,
            "celulas_valores": source_cells,
            "notas": notes,
        },
    )


def frame(observations: list[models.ObservacaoSociobio]) -> pd.DataFrame:
    if not observations:
        return conab_custos.CONAB_SOCIOBIO_V1.empty_frame()
    result = pd.DataFrame.from_records(
        [row.model_dump() for row in observations], columns=conab_custos.SOCIOBIO_COLUMNS
    )
    for name in result:
        if name in conab_custos.SOCIOBIO_MEASURES:
            result[name] = result[name].astype("float64")
        elif name in {"ano", "linha"}:
            result[name] = result[name].astype("Int64")
        elif name == "data_precos":
            result[name] = pd.to_datetime(result[name]).astype("datetime64[ns]")
        else:
            result[name] = result[name].astype(conab_custos.TEXTO)
    return result
