from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize.dates import month_to_number
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import open_excel_safe
from agrobr.utils.result import ATRIBUTO_AVISOS

from .models import normalize_produto

logger = _log.get_logger(__name__)

PARSER_VERSION = 2


def _detect_month(text: Any) -> int | None:
    if text is None:
        return None

    s = str(text).strip().lower()

    skip_patterns = ["total", "acumulad", "anual", " a ", "/"]
    if any(p in s for p in skip_patterns):
        return None

    try:
        n = int(s)
        return n if 1 <= n <= 12 else None
    except ValueError:
        pass

    return month_to_number(s)


def _detect_produto_from_header(header: str) -> str | None:
    h = header.strip().lower()

    if (
        any(k in h for k in ["grão", "grao", "grain", "soybean"])
        and "farelo" not in h
        and "óleo" not in h
        and "oleo" not in h
        and "meal" not in h
        and "oil" not in h
    ):
        return "grao"
    if any(k in h for k in ["farelo", "meal"]):
        return "farelo"
    if any(k in h for k in ["óleo", "oleo", "oil"]):
        return "oleo"
    if any(k in h for k in ["milho", "corn"]):
        return "milho"
    if "total" in h:
        return "total"

    return None


def parse_exportacao_excel(
    data: bytes,
    ano: int | None = None,
) -> pd.DataFrame:
    xls = open_excel_safe(data, source="abiove", parser_version=PARSER_VERSION)

    all_records: list[dict[str, Any]] = []

    for sheet_name in xls.sheet_names:
        all_records.extend(_parse_sheet(xls, str(sheet_name), ano))

    if not all_records:
        raise ParseError(
            source="abiove",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum dado extraído. Sheets: {xls.sheet_names}",
        )

    df = pd.DataFrame(all_records)

    if "produto" in df.columns:
        df["produto"] = df["produto"].apply(normalize_produto)

    sort_cols = [c for c in ["ano", "mes", "produto"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info(
        "abiove_parse_ok",
        records=len(df),
        sheets_parsed=len(xls.sheet_names),
    )

    return df


def _parse_sheet(
    xls: pd.ExcelFile,
    sheet_name: str,
    ano: int | None,
) -> list[dict[str, Any]]:
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    return _parse_meses_rows(df_raw, ano, sheet_name)


def _find_month_col(df: pd.DataFrame) -> int:
    for col in (0, 1):
        if col >= len(df.columns):
            continue
        hits = 0
        for idx in range(len(df)):
            cell = str(df.iloc[idx, col]).strip() if pd.notna(df.iloc[idx, col]) else ""
            if _detect_month(cell) is not None:
                hits += 1
                if hits >= 3:
                    return col
    return 0


def _parse_meses_rows(
    df: pd.DataFrame,
    ano: int | None,
    sheet_name: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    month_col = _find_month_col(df)
    for col in range(month_col + 1, len(df.columns)):
        if df.iloc[:, col].isna().all():
            df = df.iloc[:, :col]
            break

    month_rows: list[tuple[int, int]] = []
    for idx in range(len(df)):
        cell = str(df.iloc[idx, month_col]).strip() if pd.notna(df.iloc[idx, month_col]) else ""
        month = _detect_month(cell)
        if month is not None:
            month_rows.append((idx, month))

    if len(month_rows) < 3:
        return []

    sections = _split_sections(df, month_col, month_rows, sheet_name, ano)

    for produto, sec_months, data_cols in sections:
        if not any(tipo in ("volume", "volume_mil_t") for tipo in data_cols.values()):
            continue
        for row_idx, month in sec_months:
            rec: dict[str, Any] = {
                "ano": ano or 0,
                "mes": month,
                "produto": produto,
                "volume_ton": None,
                "receita_usd_mil": None,
            }
            for col_idx, tipo in data_cols.items():
                value = safe_float(df.iloc[row_idx, col_idx])
                if value is None:
                    continue
                if tipo in ("volume", "volume_mil_t"):
                    rec["volume_ton"] = value * (1000 if tipo == "volume_mil_t" else 1)
                elif tipo == "receita":
                    rec["receita_usd_mil"] = value
            if rec["volume_ton"] is not None or rec["receita_usd_mil"] is not None:
                records.append(rec)

    return records


def _split_sections(
    df: pd.DataFrame,
    month_col: int,
    month_rows: list[tuple[int, int]],
    sheet_name: str,
    ano: int | None = None,
) -> list[tuple[str, list[tuple[int, int]], dict[int, str]]]:
    groups: list[tuple[int, list[tuple[int, int]]]] = []
    current: list[tuple[int, int]] = []

    for _i, (row_idx, month) in enumerate(month_rows):
        if current and row_idx - current[-1][0] > 4:
            groups.append((current[0][0], list(current)))
            current = []
        current.append((row_idx, month))

    if current:
        groups.append((current[0][0], list(current)))

    sections: list[tuple[str, list[tuple[int, int]], dict[int, str]]] = []

    for first_row, grp_months in groups:
        produto = _detect_section_produto(df, month_col, first_row, sheet_name)
        data_cols = _detect_data_cols(df, month_col, first_row, ano)
        sections.append((produto, grp_months, data_cols))

    return sections


def _detect_section_produto(
    df: pd.DataFrame,
    _month_col: int,
    first_month_row: int,
    sheet_name: str,
) -> str:
    for offset in range(1, 6):
        check_row = first_month_row - offset
        if check_row < 0:
            break
        for col in range(min(3, len(df.columns))):
            val = df.iloc[check_row, col]
            if pd.isna(val):
                continue
            produto = _detect_produto_from_header(str(val))
            if produto:
                return produto

    raise ParseError(
        source="abiove",
        parser_version=PARSER_VERSION,
        reason=f"Seção sem produto identificado na aba {sheet_name}",
    )


def _detect_data_cols(
    df: pd.DataFrame,
    month_col: int,
    first_month_row: int,
    ano: int | None = None,
) -> dict[int, str]:
    col_map: dict[int, str] = {}

    for offset in range(1, 5):
        hdr_row = first_month_row - offset
        if hdr_row < 0:
            break
        recognized = False
        for col_idx in range(month_col + 1, len(df.columns)):
            val = df.iloc[hdr_row, col_idx]
            if pd.isna(val):
                continue
            tipo = _column_metric(str(val))
            if tipo is None or tipo == "preco":
                continue
            recognized = True
            target = _pick_latest_year_col(df, hdr_row, col_idx, ano)
            if target is not None:
                col_map.setdefault(target, tipo)
        if recognized:
            return col_map

    return col_map


def _column_metric(header: str) -> str | None:
    text = header.strip().lower()
    if any(word in text for word in ("preço", "preco", "price", "us$/t", "usd/t")):
        return "preco"
    if any(word in text for word in ("peso", "volume", "ton", "mil t", "quantidade")):
        return "volume_mil_t" if "mil t" in text else "volume"
    if any(word in text for word in ("valor", "fob", "receita", "us$", "usd")):
        return "receita"
    return None


def _header_year(value: Any) -> int | None:
    if isinstance(value, (date, datetime)):
        return value.year
    try:
        year = int(float(str(value)))
    except (ValueError, TypeError):
        return None
    return year if 2000 <= year <= 2100 else None


def _pick_latest_year_col(
    df: pd.DataFrame,
    header_row: int,
    group_start: int,
    ano: int | None = None,
) -> int | None:
    year_row = header_row + 1
    if year_row >= len(df):
        return group_start

    group_end = next(
        (
            col
            for col in range(group_start + 1, len(df.columns))
            if pd.notna(df.iloc[header_row, col])
        ),
        len(df.columns),
    )
    years = {
        year: col
        for col in range(group_start, group_end)
        if (year := _header_year(df.iloc[year_row, col])) is not None
    }
    if not years:
        return group_start
    return years.get(ano) if ano is not None else years[max(years)]


def _soma_completa(valores: pd.Series) -> Any:
    return valores.sum(min_count=len(valores))


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = ["ano", "mes"]
    medidas = [c for c in ("volume_ton", "receita_usd_mil") if c in df.columns]
    grupos = df.groupby(group_cols)[medidas]
    result = grupos.agg(_soma_completa).reset_index()
    result["produto"] = "total"
    result = result.sort_values(group_cols).reset_index(drop=True)

    nulos = df[medidas].isna().groupby([df[c] for c in group_cols])
    mistos = (nulos.any() & ~nulos.all()).sum()
    avisos = [
        f"abiove: {medida} sai nulo em {int(mistos[medida])} mês(es) com produto sem o valor; "
        "a soma das partes conhecidas não é o total"
        for medida in medidas
        if mistos[medida]
    ]
    if avisos:
        result.attrs[ATRIBUTO_AVISOS] = avisos
    return result
