from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import (
    COLUNAS_SAIDA,
    estado_para_uf,
    parse_cultura_header,
    parse_operacao_header,
)

logger = structlog.get_logger()

PARSER_VERSION = 1

SHEET_NAME = "Progresso de safra"


def _safe_float(val: object) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", ".").rstrip("%").strip()
    if not s:
        return None
    try:
        v = float(s)
        if "%" in str(val) and v > 1:
            return v / 100.0
        return v
    except ValueError:
        return None


def _parse_date(val: object) -> str:
    if val is None:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    return str(val).strip()


def parse_progresso_xlsx(data: bytes) -> pd.DataFrame:
    try:
        wb_sheets = pd.ExcelFile(io.BytesIO(data), engine="openpyxl").sheet_names
    except Exception as e:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao abrir XLSX: {e}",
        ) from e

    target_sheet: str | None = None
    for name in wb_sheets:
        name_str = str(name)
        if "progresso" in name_str.lower():
            target_sheet = name_str
            break
    if target_sheet is None:
        target_sheet = str(wb_sheets[0]) if wb_sheets else None

    if target_sheet is None:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Nenhuma sheet encontrada no XLSX",
        )

    try:
        df_raw = pd.read_excel(
            io.BytesIO(data),
            sheet_name=target_sheet,
            header=None,
            engine="openpyxl",
        )
    except Exception as e:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler sheet '{target_sheet}': {e}",
        ) from e

    if df_raw.empty:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Sheet vazia",
        )

    records: list[dict[str, object]] = []
    cultura_atual: str | None = None
    safra_atual: str | None = None
    operacao_atual: str | None = None
    semana_atual: str = ""
    in_data_rows = False
    ncols = len(df_raw.columns)

    for _, row in df_raw.iterrows():
        vals = [v if pd.notna(v) else None for v in row]
        while len(vals) < 6:
            vals.append(None)

        col_1 = str(vals[1]).strip() if vals[1] is not None else ""

        parsed_cultura = parse_cultura_header(col_1)
        if parsed_cultura:
            cultura_atual, safra_atual = parsed_cultura
            operacao_atual = None
            in_data_rows = False
            continue

        parsed_op = parse_operacao_header(col_1)
        if parsed_op:
            operacao_atual = parsed_op
            in_data_rows = False
            continue

        if col_1 == "Estado" and cultura_atual and operacao_atual:
            in_data_rows = False
            continue

        if col_1 == "" and vals[2] is not None and isinstance(vals[2], int):
            year_val = vals[2]
            if isinstance(year_val, int) and 2000 <= year_val <= 2100:
                continue

        date_vals = [vals[i] for i in range(2, min(5, ncols)) if vals[i] is not None]
        if date_vals and all(isinstance(d, datetime) for d in date_vals):
            semana_atual = _parse_date(date_vals[-1]) if date_vals else ""
            in_data_rows = True
            continue

        if not in_data_rows or not cultura_atual or not operacao_atual:
            continue

        estado_raw = col_1
        if not estado_raw:
            continue

        if estado_raw.startswith("*") or estado_raw.startswith("("):
            continue
        if "estados" in estado_raw.lower() or "brasil" in estado_raw.lower():
            continue
        if estado_raw.lower().startswith("valores") or estado_raw.lower().startswith("percentual"):
            in_data_rows = False
            continue
        if estado_raw.lower().startswith("estimativa"):
            continue

        uf = estado_para_uf(estado_raw)

        pct_ano_ant = _safe_float(vals[2])
        pct_sem_ant = _safe_float(vals[3])
        pct_sem_atual = _safe_float(vals[4])
        pct_media_5 = _safe_float(vals[5])

        records.append(
            {
                "cultura": cultura_atual,
                "safra": safra_atual,
                "operacao": operacao_atual,
                "estado": uf,
                "semana_atual": semana_atual,
                "pct_ano_anterior": pct_ano_ant,
                "pct_semana_anterior": pct_sem_ant,
                "pct_semana_atual": pct_sem_atual,
                "pct_media_5_anos": pct_media_5,
            }
        )

    if not records:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro extraido do XLSX",
        )

    result = pd.DataFrame(records, columns=COLUNAS_SAIDA)
    logger.info("conab_progresso_parse_ok", records=len(result))
    return result
