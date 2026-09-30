from __future__ import annotations

import io
import re
from typing import Any

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize.dates import month_to_number
from agrobr.normalize.numeric import safe_float

logger = _log.get_logger(__name__)

PARSER_VERSION = 3

_MIN_CELL_LINES_TO_EXPAND = 5
_SECTION_TITLE_MIN_LEN = 30
_RE_EDICAO = re.compile(r"^(Janeiro a \S+|Total(?: do Ano)?)\s+\d")


def _check_pdfplumber() -> Any:
    try:
        import pdfplumber

        return pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber é necessário para processar PDFs ANDA. "
            "Instale com: pip install agrobr[pdf] ou pip install pdfplumber"
        ) from None


def _detect_month(text: str) -> int | None:
    s = text.strip().lower()

    try:
        n = int(s)
        if 1 <= n <= 12:
            return n
    except ValueError:
        pass

    return month_to_number(s)


def extract_tables_from_pdf(pdf_bytes: bytes) -> list[list[list[str | None]]]:
    pdfplumber = _check_pdfplumber()

    tables: list[list[list[str | None]]] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables.extend(page.extract_tables())

    logger.info("anda_pdf_tables", count=len(tables))
    return tables


def _clean_cells(table: list[list[str | None]]) -> list[list[str]]:
    return [[str(c).strip() if c else "" for c in row] for row in table]


def _max_cell_lines(rows: list[list[str]]) -> int:
    return max((cell.count("\n") + 1 for row in rows for cell in row), default=0)


def _expand_newline_cells(table: list[list[str | None]]) -> list[list[str]]:
    """Desmembra células multi-linha quando o pdfplumber colapsa várias linhas numa só.

    Só expande quando alguma célula acumula >= _MIN_CELL_LINES_TO_EXPAND quebras de
    linha (sinal do colapso); tabelas normais passam intactas.
    """
    clean = _clean_cells(table)
    if _max_cell_lines(clean) < _MIN_CELL_LINES_TO_EXPAND:
        return clean

    expanded: list[list[str]] = []
    for row in clean:
        splits = [cell.split("\n") for cell in row]
        n_lines = max(len(s) for s in splits)
        for i in range(n_lines):
            new_row = [s[i].strip() if i < len(s) else "" for s in splits]
            expanded.append(new_row)

    return expanded


def parse_entregas_table(
    table: list[list[str | None]],
    ano: int,
) -> list[dict[str, Any]]:
    if not table or len(table) < 2:
        return []
    return _parse_indicadores(_expand_newline_cells(table), ano)


def _make_record(
    ano: int,
    mes: int,
    uf: str,
    vol: float | None,
) -> dict[str, Any] | None:
    if vol is None or not vol > 0:
        return None
    return {
        "ano": ano,
        "mes": mes,
        "uf": uf,
        "produto_fertilizante": "total",
        "volume_ton": vol,
    }


def _entregas_section(table: list[list[str]]) -> list[list[str]]:
    title = "fertilizantes entregues ao mercado (em toneladas de produto)"
    starts = [
        index
        for index, row in enumerate(table)
        if any(" ".join(cell.casefold().split()) == title for cell in row)
    ]
    if not starts:
        return []
    if len(starts) != 1:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason="Múltiplas seções de entregas de fertilizantes na mesma tabela",
        )
    start = starts[0] + 1
    end = next(
        (
            index
            for index in range(start, len(table))
            if any(
                len(cell.strip()) > _SECTION_TITLE_MIN_LEN and "\n" not in cell
                for cell in table[index]
            )
        ),
        len(table),
    )
    return table[start:end]


def _find_year_anchor(table: list[list[str]], ano_str: str) -> tuple[int, int] | None:
    for i, row in enumerate(table):
        for j, cell in enumerate(row):
            if cell.strip() == ano_str:
                return i, j
    return None


def _find_month_col(rows: list[list[str]]) -> int | None:
    for row in rows:
        for j, cell in enumerate(row):
            if _detect_month(cell) is not None:
                return j
    return None


def _parse_indicadores(
    table: list[list[str]],
    ano: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    ano_str = str(ano)

    section = _entregas_section(table)
    if not section:
        return records
    anchor = _find_year_anchor(section, ano_str)
    if anchor is None:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason=f"Ano {ano} ausente na seção de entregas de fertilizantes",
        )
    header_row_idx, ano_col_idx = anchor

    data_rows = section[header_row_idx + 1 :]
    mes_col_idx = _find_month_col(data_rows)
    if mes_col_idx is None:
        return records

    max_col = max(mes_col_idx, ano_col_idx)

    for row in data_rows:
        if len(row) <= max_col:
            continue

        cell_mes = row[mes_col_idx]

        if cell_mes and len(cell_mes.strip()) > _SECTION_TITLE_MIN_LEN:
            break
        if row[ano_col_idx].strip() == ano_str and cell_mes.strip() == "":
            break

        mes = _detect_month(cell_mes)
        if mes is None:
            continue

        rec = _make_record(ano, mes, "BR", safe_float(row[ano_col_idx]))
        if rec is not None:
            records.append(rec)

    return records


def parse_entregas_pdf(
    pdf_bytes: bytes,
    ano: int,
) -> pd.DataFrame:
    tables = extract_tables_from_pdf(pdf_bytes)

    if not tables:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason="Nenhuma tabela encontrada no PDF",
        )

    all_records: list[dict[str, Any]] = []
    for table in tables:
        records = parse_entregas_table(table, ano)
        all_records.extend(records)

    if not all_records:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum registro válido extraído de {len(tables)} tabelas",
        )

    df = pd.DataFrame(all_records)

    df = df.sort_values(["mes", "uf"]).reset_index(drop=True)

    logger.info(
        "anda_parsed",
        ano=ano,
        produto="total",
        records=len(df),
        ufs=sorted(df["uf"].unique().tolist()),
    )

    return df


def edicao_impressa(pdf_bytes: bytes) -> str | None:
    pdfplumber = _check_pdfplumber()

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        texto = (pdf.pages[0].extract_text() or "") if pdf.pages else ""
    for linha in texto.splitlines():
        rotulo = _RE_EDICAO.match(linha.strip())
        if rotulo:
            return rotulo.group(1)
    return None


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = ["ano", "mes", "produto_fertilizante"]
    agg_cols = {"volume_ton": "sum"}

    df_agg = df.groupby(group_cols, as_index=False).agg(agg_cols)
    return df_agg.sort_values(["ano", "mes"]).reset_index(drop=True)
