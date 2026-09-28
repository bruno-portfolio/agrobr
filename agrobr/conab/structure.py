from __future__ import annotations

from io import BytesIO

import pandas as pd

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.normalize import regions
from agrobr.utils import io


def normalize_label(value: str) -> str:
    return " ".join(regions.remover_acentos(value).casefold().split())


def find_sheet(data: BytesIO, expected: str, *, parser_version: int) -> str | None:
    label = normalize_label(expected)
    with io.open_excel_safe(data, source="conab", parser_version=parser_version) as book:
        matches = [str(name) for name in book.sheet_names if normalize_label(str(name)) == label]
    if len(matches) > 1:
        raise ParseError(
            source="conab",
            parser_version=parser_version,
            reason=f"Abas ambíguas para {expected}: {matches}",
        )
    return matches[0] if matches else None


def find_sheets(data: BytesIO, labels: tuple[str, ...], *, parser_version: int) -> list[str]:
    with io.open_excel_safe(data, source="conab", parser_version=parser_version) as book:
        names = [str(name) for name in book.sheet_names]
    found: list[str] = []
    for label in labels:
        matches = [name for name in names if normalize_label(name) == normalize_label(label)]
        if len(matches) > 1:
            raise ParseError(
                source="conab",
                parser_version=parser_version,
                reason=f"Abas ambíguas para {label}: {matches}",
            )
        found += matches
    return found


def resolve_sheet(data: BytesIO, expected: str, *, parser_version: int) -> str:
    selected = find_sheet(data, expected, parser_version=parser_version)
    if selected is None:
        raise ParseError(
            source="conab",
            parser_version=parser_version,
            reason=f"Aba obrigatória ausente: {expected}",
        )
    return selected


def supply_columns(header: pd.Series, parser_version: int) -> dict[str, int]:
    fields = constants.CONAB_SUPRIMENTO_HEADERS
    columns: dict[str, int] = {}
    for position, value in enumerate(header):
        label = normalize_label(str(value)) if pd.notna(value) else ""
        field = fields.get(label)
        if field is None:
            if position >= 3 and label:
                raise ParseError(
                    source="conab",
                    parser_version=parser_version,
                    reason=f"Layout da aba Suprimento mudou: coluna {position}, {value!r}",
                )
            continue
        if field in columns:
            raise ParseError(
                source="conab",
                parser_version=parser_version,
                reason=f"Coluna ambígua em Suprimento: {field}",
            )
        columns[field] = position
    missing = set(fields.values()) - {"demanda_total"} - columns.keys()
    if missing:
        raise ParseError(
            source="conab",
            parser_version=parser_version,
            reason=f"Layout da aba Suprimento mudou: colunas ausentes {sorted(missing)}",
        )
    return columns
