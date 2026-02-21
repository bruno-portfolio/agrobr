from __future__ import annotations

import re
from typing import Any

import pandas as pd
import structlog

from agrobr.anda.models import ANDA_UFS, normalize_fertilizante
from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 1

_MESES_MAP: dict[str, int] = {
    "janeiro": 1,
    "jan": 1,
    "fevereiro": 2,
    "fev": 2,
    "março": 3,
    "mar": 3,
    "marco": 3,
    "abril": 4,
    "abr": 4,
    "maio": 5,
    "mai": 5,
    "junho": 6,
    "jun": 6,
    "julho": 7,
    "jul": 7,
    "agosto": 8,
    "ago": 8,
    "setembro": 9,
    "set": 9,
    "outubro": 10,
    "out": 10,
    "novembro": 11,
    "nov": 11,
    "dezembro": 12,
    "dez": 12,
}

_UF_PATTERNS = re.compile(r"^(UF|Estado|Unidade\s*da\s*Federa)", re.IGNORECASE)
_MES_PATTERNS = re.compile(r"^(M[eê]s|Per[ií]odo|Month)", re.IGNORECASE)
_VOLUME_PATTERNS = re.compile(r"(tonelada|volume|ton\.|entrega|quantidade|total)", re.IGNORECASE)


def _check_pdfplumber() -> Any:
    try:
        import pdfplumber

        return pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber é necessário para processar PDFs ANDA. "
            "Instale com: pip install agrobr[pdf] ou pip install pdfplumber"
        ) from None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if not s or s in ("-", "–", "—", "...", "n.d.", "n/d"):
        return None

    s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


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

    for key, val in _MESES_MAP.items():
        if s.startswith(key):
            return val

    return None


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


def _expand_newline_cells(table: list[list[str | None]]) -> list[list[str]]:
    if not table or len(table) < 2:
        return [[str(c).strip() if c else "" for c in row] for row in table]

    clean = [[str(c).strip() if c else "" for c in row] for row in table]

    max_lines = 0
    for row in clean:
        for cell in row:
            n = cell.count("\n") + 1
            if n > max_lines:
                max_lines = n

    if max_lines < 5:
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
    produto: str = "total",
) -> list[dict[str, Any]]:
    if not table or len(table) < 2:
        return []

    records: list[dict[str, Any]] = []

    clean_table = _expand_newline_cells(table)

    header = clean_table[0]
    first_col_values = [row[0] for row in clean_table[1:] if row]

    uf_in_rows = sum(1 for v in first_col_values if _is_uf(v))
    uf_in_cols = sum(1 for v in header[1:] if _is_uf(v))

    if uf_in_rows >= 3:
        records = _parse_uf_rows(clean_table, ano, produto)

    if not records and uf_in_cols >= 3:
        records = _parse_uf_cols(clean_table, ano, produto)

    if not records:
        records = _parse_generic(clean_table, ano, produto)

    if not records:
        records = _parse_indicadores(clean_table, ano, produto)

    return records


def _parse_uf_rows(
    table: list[list[str]],
    ano: int,
    produto: str,
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
            vol = _safe_float(row[col_idx])
            if vol is not None and vol > 0:
                records.append(
                    {
                        "ano": ano,
                        "mes": mes,
                        "uf": uf_candidate,
                        "produto_fertilizante": normalize_fertilizante(produto),
                        "volume_ton": vol,
                    }
                )

    return records


def _parse_uf_cols(
    table: list[list[str]],
    ano: int,
    produto: str,
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
            vol = _safe_float(row[col_idx])
            if vol is not None and vol > 0:
                records.append(
                    {
                        "ano": ano,
                        "mes": mes,
                        "uf": uf,
                        "produto_fertilizante": normalize_fertilizante(produto),
                        "volume_ton": vol,
                    }
                )

    return records


def _parse_generic(
    table: list[list[str]],
    ano: int,
    produto: str,
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

    for row in table[1:]:
        if len(row) <= max(c for c in [uf_col, mes_col, vol_col] if c is not None):
            continue

        uf_val = row[uf_col].strip().upper() if uf_col is not None else ""
        if not _is_uf(uf_val):
            continue

        mes_val = 0
        if mes_col is not None:
            detected = _detect_month(row[mes_col])
            mes_val = detected if detected is not None else 0

        vol = _safe_float(row[vol_col]) if vol_col is not None else None
        if vol is not None and vol > 0:
            record: dict[str, Any] = {
                "ano": ano,
                "mes": mes_val,
                "uf": uf_val,
                "produto_fertilizante": normalize_fertilizante(produto),
                "volume_ton": vol,
            }
            records.append(record)

    return records


def _parse_indicadores(
    table: list[list[str]],
    ano: int,
    produto: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    ano_str = str(ano)
    header_row_idx = None
    ano_col_idx = None

    for i, row in enumerate(table):
        for j, cell in enumerate(row):
            if cell.strip() == ano_str:
                header_row_idx = i
                ano_col_idx = j
                break
        if ano_col_idx is not None:
            break

    if header_row_idx is None or ano_col_idx is None:
        return records

    mes_col_idx = None
    for row in table[header_row_idx + 1 :]:
        for j, cell in enumerate(row):
            if _detect_month(cell) is not None:
                mes_col_idx = j
                break
        if mes_col_idx is not None:
            break

    if mes_col_idx is None:
        return records

    for row in table[header_row_idx + 1 :]:
        if len(row) <= max(mes_col_idx, ano_col_idx):
            continue

        cell_mes = row[mes_col_idx]

        if cell_mes and len(cell_mes.strip()) > 30:
            break
        if row[ano_col_idx].strip() == ano_str and cell_mes.strip() == "":
            break

        mes = _detect_month(cell_mes)
        if mes is None:
            continue

        vol = _safe_float(row[ano_col_idx])
        if vol is not None and vol > 0:
            records.append(
                {
                    "ano": ano,
                    "mes": mes,
                    "uf": "BR",
                    "produto_fertilizante": normalize_fertilizante(produto),
                    "volume_ton": vol,
                }
            )

    return records


def parse_entregas_pdf(
    pdf_bytes: bytes,
    ano: int,
    produto: str = "total",
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
        records = parse_entregas_table(table, ano, produto)
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
        produto=produto,
        records=len(df),
        ufs=sorted(df["uf"].unique().tolist()),
    )

    return df


def parse_entregas_excel(
    excel_bytes: bytes,
    ano: int,
    produto: str = "total",
) -> pd.DataFrame:
    import io

    try:
        df_raw = pd.read_excel(io.BytesIO(excel_bytes), header=None)
    except Exception as e:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler Excel: {e}",
        ) from e

    table = df_raw.fillna("").astype(str).values.tolist()

    records = parse_entregas_table(table, ano, produto)

    if not records:
        raise ParseError(
            source="anda",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro válido extraído do Excel",
        )

    df = pd.DataFrame(records)
    df = df.sort_values(["mes", "uf"]).reset_index(drop=True)
    return df


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = ["ano", "mes", "produto_fertilizante"]
    agg_cols = {"volume_ton": "sum"}

    df_agg = df.groupby(group_cols, as_index=False).agg(agg_cols)
    return df_agg.sort_values(["ano", "mes"]).reset_index(drop=True)
