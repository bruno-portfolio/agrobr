from __future__ import annotations

import re
from typing import Any

import pandas as pd
import structlog

from agrobr.anda.models import ANDA_UFS
from agrobr.exceptions import ParseError
from agrobr.normalize.dates import month_to_number
from agrobr.normalize.numeric import safe_float

logger = structlog.get_logger()

PARSER_VERSION = 2

_UF_PATTERNS = re.compile(r"^(UF|Estado|Unidade\s*da\s*Federa)", re.IGNORECASE)
_MES_PATTERNS = re.compile(r"^(M[eê]s|Per[ií]odo|Month)", re.IGNORECASE)
_VOLUME_PATTERNS = re.compile(r"(tonelada|volume|ton\.|entrega|quantidade|total)", re.IGNORECASE)

_MIN_CELL_LINES_TO_EXPAND = 5
_MIN_UFS_FOR_LAYOUT = 3
_SECTION_TITLE_MIN_LEN = 30


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

    _ACUMULADO_PATTERNS = (" a ", "/dez", "total", "acumulado", "anual", "ano")
    if any(p in s for p in _ACUMULADO_PATTERNS):
        return None

    try:
        n = int(s)
        if 1 <= n <= 12:
            return n
    except ValueError:
        pass

    return month_to_number(s)


def _is_uf(text: str) -> bool:
    return text.strip().upper() in ANDA_UFS


def extract_tables_from_pdf(pdf_bytes: bytes) -> list[list[list[str | None]]]:
    pdfplumber = _check_pdfplumber()

    import io

    tables: list[list[list[str | None]]] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)

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
    if len(clean) < 2:
        return clean

    if _max_cell_lines(clean) < _MIN_CELL_LINES_TO_EXPAND:
        return clean

    expanded: list[list[str]] = []
    for row in clean:
        splits = [cell.split("\n") for cell in row]
        n_lines = max(len(s) for s in splits)
        if n_lines < 2:
            expanded.append(row)
        else:
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

    records: list[dict[str, Any]] = []

    clean_table = _expand_newline_cells(table)

    header = clean_table[0]
    first_col_values = [row[0] for row in clean_table[1:] if row]

    uf_in_rows = sum(1 for v in first_col_values if _is_uf(v))
    uf_in_cols = sum(1 for v in header[1:] if _is_uf(v))

    if uf_in_rows >= _MIN_UFS_FOR_LAYOUT:
        records = _parse_uf_rows(clean_table, ano)

    if not records and uf_in_cols >= _MIN_UFS_FOR_LAYOUT:
        records = _parse_uf_cols(clean_table, ano)

    if not records:
        records = _parse_generic(clean_table, ano)

    if not records:
        records = _parse_indicadores(clean_table, ano)

    return records


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


def _parse_uf_rows(
    table: list[list[str]],
    ano: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    header = table[0]

    month_cols: dict[int, int] = {}
    for i, h in enumerate(header[1:], 1):
        month = _detect_month(h)
        if month is not None:
            month_cols[i] = month

    if not month_cols:
        return records

    for row in table[1:]:
        if not row or not row[0]:
            continue
        uf_candidate = row[0].strip().upper()
        if not _is_uf(uf_candidate):
            continue

        for col_idx, mes in month_cols.items():
            if col_idx >= len(row):
                continue
            rec = _make_record(ano, mes, uf_candidate, safe_float(row[col_idx]))
            if rec is not None:
                records.append(rec)

    return records


def _parse_uf_cols(
    table: list[list[str]],
    ano: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    header = table[0]

    uf_cols: dict[int, str] = {}
    for i, h in enumerate(header[1:], 1):
        if _is_uf(h):
            uf_cols[i] = h.strip().upper()

    if not uf_cols:
        return records

    for row in table[1:]:
        if not row or not row[0]:
            continue
        mes = _detect_month(row[0])
        if mes is None:
            continue

        for col_idx, uf in uf_cols.items():
            if col_idx >= len(row):
                continue
            rec = _make_record(ano, mes, uf, safe_float(row[col_idx]))
            if rec is not None:
                records.append(rec)

    return records


def _parse_generic(
    table: list[list[str]],
    ano: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    header = table[0]

    uf_col: int | None = None
    mes_col: int | None = None
    vol_col: int | None = None

    for i, h in enumerate(header):
        if _UF_PATTERNS.match(h):
            uf_col = i
        elif _MES_PATTERNS.match(h):
            mes_col = i
        elif _VOLUME_PATTERNS.search(h):
            vol_col = i

    if uf_col is None or vol_col is None:
        return records

    max_col = max(c for c in (uf_col, mes_col, vol_col) if c is not None)

    for row in table[1:]:
        if len(row) <= max_col:
            continue

        uf_val = row[uf_col].strip().upper()
        if not _is_uf(uf_val):
            continue

        mes_val = 0
        if mes_col is not None:
            detected = _detect_month(row[mes_col])
            mes_val = detected if detected is not None else 0

        rec = _make_record(ano, mes_val, uf_val, safe_float(row[vol_col]))
        if rec is not None:
            records.append(rec)

    return records


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

    anchor = _find_year_anchor(table, ano_str)
    if anchor is None:
        return records
    header_row_idx, ano_col_idx = anchor

    data_rows = table[header_row_idx + 1 :]
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


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = ["ano", "mes", "produto_fertilizante"]
    agg_cols = {"volume_ton": "sum"}

    df_agg = df.groupby(group_cols, as_index=False).agg(agg_cols)
    return df_agg.sort_values(["ano", "mes"]).reset_index(drop=True)
