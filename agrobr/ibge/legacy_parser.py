from __future__ import annotations

import io
import re
import struct
import unicodedata
from typing import Any

import pandas as pd
import xlrd
from bs4 import BeautifulSoup
from pydantic import BaseModel
from xlrd import compdoc

from agrobr.exceptions import ParseError
from agrobr.ibge import legacy_xls
from agrobr.normalize import encoding, regions
from agrobr.normalize.numeric import safe_float

PARSER_VERSION = 2
TEMAS_LEGADO = [
    "tecnologia",
    "pessoal_ocupado",
    "maquinas",
    "producao_animal",
    "valor_producao",
    "financeiro",
]
_OUTPUT_COLS = [
    "ano",
    "localidade",
    "localidade_cod",
    "uf",
    "tema",
    "categoria",
    "variavel",
    "valor",
    "unidade",
    "fonte",
    "nivel_geo",
]
_UNITS = re.compile(r"\(\s*(mil reais|mil litros|mil dz|mil cabeças|ha|t)\s*\)", re.I)
_TITLES = {
    "tecnologia": ("tecnologias utilizadas", "assistencia tecnica"),
    "pessoal_ocupado": ("pessoal ocupado",),
    "maquinas": ("tratores", "maquinaria"),
    "producao_animal": ("efetivos de animais e producao", "producao de leite"),
    "valor_producao": ("valor da producao",),
    "financeiro": ("despesas", "receitas", "investimentos"),
}


class LegacyRecord(BaseModel):
    ano: int = 1995
    localidade: str
    localidade_cod: int | None
    uf: str | None
    tema: str
    categoria: str
    variavel: str
    valor: float | None
    unidade: str
    fonte: str = "ibge_censo_agro_legado"
    nivel_geo: str


def _plain(text: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", text.casefold())
        if not unicodedata.combining(char)
    )


def _error(reason: str) -> ParseError:
    return ParseError(source="ibge_censo_agro_legado", parser_version=PARSER_VERSION, reason=reason)


def _check_title(title: str, tema: str) -> None:
    if tema not in _TITLES:
        raise _error(f"Tema não suportado: {tema}. Disponíveis: {TEMAS_LEGADO}")
    if not any(token in _plain(title) for token in _TITLES[tema]):
        raise _error(f"Cabeçalho não corresponde ao tema {tema}: {title}")


def _geography(title: str) -> tuple[str, int, str]:
    normalized = _plain(title.strip())
    if normalized == "brasil":
        return "Brasil", 1, "brasil"
    for info in regions.UFS.values():
        if normalized == _plain(str(info["nome"])):
            return str(info["nome"]), int(info["ibge"]), "uf"
    raise _error(f"Geografia não reconhecida no cabeçalho: {title}")


def _detect_nivel_geo(raw_loc: str) -> str:
    if _plain(raw_loc.strip()) in {"total", "totais"}:
        return "uf"
    leading = len(raw_loc) - len(raw_loc.lstrip(" "))
    if leading >= 6:
        return "municipio"
    if leading >= 3:
        return "microrregiao"
    return "mesorregiao"


def _metric(headers: list[str], tema: str, title: str) -> tuple[str, str]:
    headers = list(dict.fromkeys(legacy_xls.clean_text(header) for header in headers))
    units = _UNITS.findall(" ".join(headers))
    leaf = _plain(headers[-1])
    if "informantes" in leaf or "estabelecimentos" in leaf:
        unit = "estabelecimentos"
    elif units:
        unit = units[-1].lower()
    else:
        unit = {
            "tecnologia": "estabelecimentos",
            "pessoal_ocupado": "pessoas",
            "maquinas": "unidades",
            "producao_animal": "cabeças",
        }.get(tema, "unidades")
    names = [_UNITS.sub("", header).strip() for header in headers]
    if tema == "financeiro" and names == ["Informantes"]:
        names.insert(0, "Despesas" if "despesas" in _plain(title) else "Receitas")
    return " / ".join(names), unit


def _records(
    rows: list[list[Any]],
    label_column: int,
    metrics: dict[int, tuple[str, str]],
    tema: str,
    geography: tuple[str, int, str],
) -> pd.DataFrame:
    records = []
    uf = next((key for key, info in regions.UFS.items() if info["ibge"] == geography[1]), None)
    for row in rows:
        if pd.isna(row[label_column]):
            continue
        raw_label = str(row[label_column]).strip("\r\n").replace("\xa0", " ")
        label = raw_label.strip()
        if not label:
            continue
        values = {column: safe_float(row[column]) for column in metrics}
        if all(value is None for value in values.values()):
            continue
        location, geography_code, level = geography
        code: int | None = geography_code
        category = label if level == "brasil" else "Total"
        if level != "brasil":
            level = _detect_nivel_geo(raw_label)
            if level != "uf":
                location, code = label, None
        for column, (variable, unit) in metrics.items():
            records.append(
                LegacyRecord(
                    localidade=location,
                    localidade_cod=code,
                    uf=uf,
                    tema=tema,
                    categoria=category,
                    variavel=variable,
                    valor=values[column],
                    unidade=unit,
                    nivel_geo=level,
                ).model_dump()
            )
    result = pd.DataFrame(records, columns=_OUTPUT_COLS)
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce").astype("float64")
    result["localidade_cod"] = result["localidade_cod"].astype("Int64")
    return result


def parse_legacy_xls(data: bytes, tema: str, filename: str = "") -> pd.DataFrame:
    try:
        boxes = legacy_xls.read_text_boxes(data)
        rows = legacy_xls.read_rows(data)
    except (xlrd.XLRDError, compdoc.CompDocError, ValueError, UnicodeError, struct.error) as exc:
        raise _error(f"Falha ao ler XLS ({filename}): {exc}") from exc
    titles = [box.text for box in boxes if box.name.startswith("TitTab")]
    pages = [box.text for box in boxes if box.name.startswith("TitPag")]
    labels = [box for box in boxes if box.name.startswith("Indic")]
    if not titles or not pages or not labels:
        raise _error(f"Cabeçalhos XLS ausentes ou não reconhecidos: {filename}")
    title = titles[0]
    _check_title(title, tema)
    geography = _geography(pages[0].split(" - ", 1)[-1])
    label_column = int(labels[0].column_start)
    metrics = {}
    for column in range(len(rows[0])):
        headers = legacy_xls.column_headers(boxes, column)
        if headers:
            metrics[column] = _metric(headers, tema, title)
    if not metrics:
        raise _error(f"Colunas de medidas não reconhecidas: {filename}")
    return _records(rows, label_column, metrics, tema, geography)


def parse_legacy_html(data: bytes, tema: str, uf: str, filename: str = "") -> pd.DataFrame:
    text, _ = encoding.decode_content(data, source="ibge_censo_agro_legado")
    soup = BeautifulSoup(text, "lxml")
    try:
        frame = pd.read_html(io.StringIO(text), flavor="lxml", decimal=",", thousands=" ")[0]
    except (ValueError, IndexError) as exc:
        raise _error(f"Tabela HTML não reconhecida: {filename}") from exc
    title = str(frame.columns[0][0])
    _check_title(title, tema)
    geography = _geography(str(regions.UFS[uf]["nome"]))
    metrics = {}
    for column in range(1, len(frame.columns)):
        headers = [
            str(part)
            for part in frame.columns[column]
            if not str(part).startswith(("Tabela", "Unnamed:"))
        ]
        metrics[column] = _metric(headers, tema, title)
    rows = []
    for label in soup.select('td[align="left"]'):
        parent = label.find_parent("tr")
        cells = parent.find_all("td") if parent else []
        if len(cells) != len(frame.columns):
            raise _error(f"Linha HTML com quantidade inesperada de colunas: {filename}")
        values = [cell.get_text().strip("\r\n") for cell in cells]
        rows.append([values[0], *("0" if value.strip() == "-" else value for value in values[1:])])
    if not rows:
        raise _error(f"Geografias HTML não reconhecidas: {filename}")
    return _records(rows, 0, metrics, tema, geography)
