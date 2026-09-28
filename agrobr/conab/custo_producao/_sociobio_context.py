from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any

from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.normalize import numeric
from agrobr.utils import validation

from . import _sociobio_parse, models
from ._context import key
from ._sociobio_workbook import AbaSociobio, WorkbookSociobio, fail


def _year(token: str, sheet: str) -> int:
    match = re.fullmatch(r"(\d{4})(?:/(\d{2}|\d{4}))?", token.strip())
    if match is None:
        raise fail(sheet, f"Ano da safra não reconhecido: {token!r}")
    return int(match[1])


def _place(value: str, sheet: str) -> tuple[str, str, dict[str, str]]:
    match = re.fullmatch(r"(.+?)\s*(?:[-–]\s*([A-Za-z]{2})|\(([A-Za-z]{2})\))\s*", value.strip())
    evidence = {}
    if match is None:
        label = re.fullmatch(r".+-([A-Z]{2})[-_]\d{4}", key(sheet))
        local = re.sub(r"^REGI[ÃA]O:\s*", "", value.strip(), flags=re.I)
        if label is None or not local:
            raise fail(sheet, f"Local/UF não reconhecido: {value!r}")
        uf = label[1]
        evidence["uf_origem"] = "nome_da_aba"
    else:
        local = match[1].strip()
        uf = (match[2] or match[3]).upper()
    try:
        validation.validate_uf(uf)
    except InvalidParameterError as error:
        raise fail(sheet, str(error)) from error
    return local, uf, evidence


def _productivity(sheet: AbaSociobio) -> tuple[float | None, str | None, dict[str, str]]:
    matches = []
    unit: str | None
    for row_number, row in enumerate(sheet.linhas[:12]):
        for column, cell in enumerate(row):
            if not isinstance(cell, str) or not key(cell).startswith("PRODUTIVIDADE"):
                continue
            suffix = cell.partition(":")[2].strip()
            remaining = [value for value in row[column + 1 :] if value not in (None, "")]
            if suffix:
                value, unit = _inline_productivity(suffix, sheet.nome)
            elif remaining and isinstance(remaining[0], str):
                value, unit = _inline_productivity(remaining[0], sheet.nome)
            elif remaining:
                raw_value = remaining[0]
                if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                    raise fail(sheet.nome, "Produtividade não numérica")
                value = float(raw_value)
                unit = str(remaining[1]).strip() if len(remaining) > 1 else None
            else:
                value, unit = None, None
            if value is not None and not math.isfinite(value):
                raise fail(sheet.nome, "Produtividade não finita")
            matches.append((value, unit, {f"R{row_number + 1}C{column + 1}": cell}))
    if len(matches) > 1:
        raise fail(sheet.nome, "Produtividade ambígua")
    return matches[0] if matches else (None, None, {})


def _inline_productivity(value: str, sheet: str) -> tuple[float, str]:
    match = re.fullmatch(r"([+-]?[\d.,]+)\s+(.+)", value.strip())
    number = numeric.parse_numeric_br(match[1]) if match else None
    if match is None or number is None:
        raise fail(sheet, f"Produtividade não reconhecida: {value!r}")
    return number, match[2].strip()


def _header_cells(sheet: AbaSociobio) -> list[tuple[int, int, str]]:
    return [
        (r, c, value)
        for r, row in enumerate(sheet.linhas[:12])
        for c, value in enumerate(row)
        if isinstance(value, str) and value.strip()
    ]


def _system(
    sheet: AbaSociobio, cells: list[tuple[int, int, str]]
) -> tuple[str, bool, dict[str, str], str]:
    modern = [
        (r, c, value) for r, c, value in cells if key(value).startswith("SOCIOBIODIVERSIDADE -")
    ]
    titles = [(r, c, value) for r, c, value in cells if key(value).startswith("CUSTO DE PRODUCAO")]
    if modern:
        if len(modern) != 1:
            raise fail(sheet.nome, "Sistema ambíguo")
        r, c, value = modern[0]
        pieces = re.split(r"\s+-\s+", value.strip(), maxsplit=2)
        if len(pieces) != 3 or not pieces[2].strip():
            raise fail(sheet.nome, "Sistema de sociobiodiversidade não reconhecido")
        return pieces[2].strip(), True, {f"R{r + 1}C{c + 1}": value}, titles[0][2] if titles else ""
    if len(titles) != 1:
        raise fail(sheet.nome, "Título de custo ausente ou ambíguo")
    row, col, title = titles[0]
    systems = [
        (c, value)
        for c, value in enumerate(sheet.linhas[row + 1])
        if isinstance(value, str) and value.strip()
    ]
    if len(systems) != 1:
        raise fail(sheet.nome, "Sistema ausente ou ambíguo")
    column, system = systems[0]
    return (
        system.strip(),
        False,
        {f"R{row + 2}C{column + 1}": system, f"R{row + 1}C{col + 1}": title},
        title,
    )


def _season_parts(
    sheet: AbaSociobio, cells: list[tuple[int, int, str]]
) -> tuple[str, str, dict[str, str]]:
    seasons = [
        (r, c, value)
        for r, c, value in cells
        if re.match(r"^(?:SAFRA|ANO(?:-SAFRA)?)(?:\s|:|$)", key(value))
    ]
    if len(seasons) != 1:
        raise fail(sheet.nome, "Safra ausente ou ambígua")
    row, col, season = seasons[0]
    evidence = {f"R{row + 1}C{col + 1}": season}
    modern = re.fullmatch(r"SAFRA\s+ANUAL\s*-\s*([\d/]+)\s*-\s*(.+)", season.strip(), re.I)
    if modern:
        return modern[1], modern[2], evidence
    old = re.fullmatch(
        r"(?:SAFRA|ANO(?:-SAFRA)?)\s*:?(?:\s+DE\s+(?:VER[ÃA]O|EXTRA[ÇC][ÃA]O)?)?\s*-?\s*([\d/]+)",
        season.strip(),
        re.I,
    )
    places = [(r, c, value) for r, c, value in cells if key(value).startswith("LOCAL:")]
    if not places:
        places = [(r, c, value) for r, c, value in cells if r == row + 1]
    if old is None or len(places) != 1:
        raise fail(sheet.nome, f"Safra/local não reconhecidos: {season!r}")
    row, col, place = places[0]
    evidence[f"R{row + 1}C{col + 1}"] = place
    return old[1], place.partition(":")[2] if key(place).startswith("LOCAL:") else place, evidence


def _report_reference(
    sheet: AbaSociobio, cells: list[tuple[int, int, str]], title: str
) -> tuple[str | None, str | None, datetime | None]:
    report_type = next(
        (
            value.partition(":")[2].strip()
            for _, _, value in cells
            if key(value).startswith("TIPO DO RELATORIO:")
        ),
        None,
    )
    if report_type is None and "ESTIMADO" in key(title):
        report_type = "ESTIMADO"
    reference = next(
        (
            value.partition(":")[2].strip()
            for _, _, value in cells
            if key(value).startswith("MES/ANO:")
        ),
        None,
    )
    price_date = None
    for row, col, label in cells:
        if key(label) != "A PRECOS DE:":
            continue
        values = [value for value in sheet.linhas[row][col + 1 :] if value not in (None, "")]
        if values and isinstance(values[0], datetime):
            price_date = values[0]
        elif values and isinstance(values[0], date):
            price_date = datetime.combine(values[0], datetime.min.time())
        elif values and isinstance(values[0], str):
            reference = values[0]
    return report_type, reference, price_date


def _label_conflict(name: str, local: str, uf: str, year: int) -> dict[str, str]:
    label = re.fullmatch(r"(.+)-([A-Z]{2})[-_](\d{4})", key(name))
    if label is None:
        return {}
    same_local = label[1] == key(local) or label[1].endswith("-" + key(local))
    if same_local and label[2] == uf and int(label[3]) == year:
        return {}
    return {"conflito_rotulo": f"nome da aba: {name}"}


def context(
    sheet: AbaSociobio, resource: models.RecursoCusto, index: int
) -> models.ContextoSociobio:
    cells = _header_cells(sheet)
    system, modern, evidence, title = _system(sheet, cells)
    raw_year, raw_place, geography = _season_parts(sheet, cells)
    year = _year(raw_year, sheet.nome)
    local, uf, uf_origin = _place(raw_place, sheet.nome)
    productivity, unit, productivity_cells = _productivity(sheet)
    report_type, reference, price_date = _report_reference(sheet, cells, title)
    return models.ContextoSociobio(
        produto=resource.cultura,
        local=local,
        uf=uf,
        ano=year,
        safra_publicada=raw_year.strip() if "/" in raw_year else None,
        sistema=system,
        tipo_relatorio=report_type,
        mes_ano_referencia=reference,
        data_precos=price_date,
        produtividade=productivity,
        unidade_produtividade=unit,
        planilha=resource.planilha,
        aba=sheet.nome,
        indice_aba=index,
        layout="novo" if modern else "antigo",
        celulas_contexto={
            **evidence,
            **geography,
            **uf_origin,
            **productivity_cells,
            **_label_conflict(sheet.nome, local, uf, year),
        },
    )


def unresolved_selectors(sheet: AbaSociobio) -> dict[str, Any]:
    try:
        token, raw_place, _ = _season_parts(sheet, _header_cells(sheet))
        local, uf, _ = _place(raw_place, sheet.nome)
    except ParseError:
        return {}
    match = re.fullmatch(r"(\d{4})(?:/(\d{2}|\d{4}))?", token)
    years = []
    if match:
        years = [int(match[1])]
        if match[2]:
            years.append(int(match[1][:2] + match[2]) if len(match[2]) == 2 else int(match[2]))
    return {"local": local, "uf": uf, "anos_publicados": sorted(set(years))}


def inventory(
    book: WorkbookSociobio, resource: models.RecursoCusto
) -> tuple[list[models.ContextoSociobio], list[dict[str, Any]]]:
    identified = []
    unresolved = []
    for index, name in enumerate(book.names):
        if key(name) in {"INDICE", "INDEX", "SUMARIO"}:
            continue
        sheet = book.read(name, head=True)
        try:
            resolved = context(sheet, resource, index)
            _sociobio_parse.columns(sheet)
            identified.append(resolved)
        except ParseError as error:
            unresolved.append(
                {
                    "aba": name,
                    "indice_aba": index,
                    "error": str(error),
                    **unresolved_selectors(sheet),
                }
            )
    return identified, unresolved


def select(
    contexts: list[models.ContextoSociobio], query: models.ConsultaSociobio
) -> list[models.ContextoSociobio]:
    selected = [
        candidate
        for candidate in contexts
        if (query.aba is None or query.aba == candidate.aba)
        and (query.local is None or key(query.local) == key(candidate.local))
        and (query.uf is None or key(query.uf) == candidate.uf)
        and (query.ano is None or query.ano == candidate.ano)
    ]
    if not selected:
        raise InvalidParameterError(
            "Nenhuma aba identificada corresponde aos filtros; consulte catalogo_sociobiodiversidade(produto)"
        )
    return selected
