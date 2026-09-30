from __future__ import annotations

import re
from datetime import datetime
from typing import cast

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import open_excel_safe

from .models import (
    COLUNAS_SAIDA,
    ESTADO_MEDIA,
    ESTADOS_PARA_UF,
    estado_para_uf,
    parse_agregado,
    parse_cobertura,
    parse_cultura_header,
    parse_operacao_header,
)

logger = _log.get_logger(__name__)

PARSER_VERSION = 3


def _parse_pct(val: object) -> float | None:
    if isinstance(val, str):
        val = re.sub(r"\*\s*$", "", val).strip()
    has_pct = isinstance(val, str) and "%" in val
    v = safe_float(val, strip="%")
    if v is not None and has_pct:
        return v / 100.0
    return v


def _read_xlsx_sheet(data: bytes) -> pd.DataFrame:
    xls = open_excel_safe(
        data, source="conab_progresso", parser_version=PARSER_VERSION, engine="openpyxl"
    )

    matches = [str(name) for name in xls.sheet_names if "progresso" in str(name).lower()]
    if len(matches) > 1 or (not matches and len(xls.sheet_names) > 1):
        xls.close()
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason=f"Abas de progresso ambíguas: {matches or xls.sheet_names}",
        )
    target_sheet = matches[0] if matches else str(xls.sheet_names[0])

    try:
        df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None)
    except Exception as e:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler sheet '{target_sheet}': {e}",
        ) from e
    finally:
        xls.close()

    if df_raw.empty:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Sheet vazia",
        )

    return df_raw


def _build_record(
    cultura: str,
    safra: str | None,
    operacao: str,
    uf: str,
    semana: str,
    vals: list[object],
    n_estados: int | None = None,
    cobertura: float | None = None,
) -> dict[str, object]:
    percentages = [_parse_pct(value) for value in vals[2:6]]
    revised = (
        any(isinstance(value, str) and value.rstrip().endswith("*") for value in vals[2:6])
        if any(value is not None for value in percentages)
        else None
    )
    return {
        "cultura": cultura,
        "safra": safra,
        "operacao": operacao,
        "estado": uf,
        "semana_atual": semana,
        "pct_ano_anterior": percentages[0],
        "pct_semana_anterior": percentages[1],
        "pct_semana_atual": percentages[2],
        "pct_media_5_anos": percentages[3],
        "revisado": revised,
        "n_estados": n_estados,
        "cobertura_area_pct": cobertura,
    }


def _validate_block(cultura: str | None, operacao: str | None, start: int, count: int) -> None:
    if cultura is not None and (operacao is None or start == count):
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason=f"Bloco sem operação ou observações: {cultura}, {operacao}",
        )


def parse_progresso_xlsx(data: bytes) -> pd.DataFrame:
    df_raw = _read_xlsx_sheet(data)

    records: list[dict[str, object]] = []
    cultura_atual: str | None = None
    safra_atual: str | None = None
    cobertura_atual: tuple[int, float] | None = None
    operacao_atual: str | None = None
    semana_atual: str = ""
    in_data_rows = False
    ncols = len(df_raw.columns)
    block_start = 0

    for _, row in df_raw.iterrows():
        vals = [v if pd.notna(v) else None for v in row]
        while len(vals) < 6:
            vals.append(None)

        col_1 = str(vals[1]).strip() if vals[1] is not None else ""

        parsed_cultura = parse_cultura_header(col_1)
        if parsed_cultura:
            _validate_block(cultura_atual, operacao_atual, block_start, len(records))
            block_start = len(records)
            cultura_atual, safra_atual = parsed_cultura
            cobertura_atual = None
            operacao_atual = None
            in_data_rows = False
            continue

        nota = parse_cobertura(col_1)
        if nota is not None and cultura_atual is not None and operacao_atual is None:
            cobertura_atual = nota
            continue

        parsed_op = parse_operacao_header(col_1)
        if parsed_op:
            if operacao_atual is not None:
                _validate_block(cultura_atual, operacao_atual, block_start, len(records))
            block_start = len(records)
            operacao_atual = parsed_op
            in_data_rows = False
            continue

        if col_1 in {"Estado", "Unidade da Federação"} and cultura_atual and operacao_atual:
            in_data_rows = False
            continue

        date_vals = [vals[i] for i in range(2, min(5, ncols)) if vals[i] is not None]
        if date_vals and all(isinstance(d, datetime) for d in date_vals):
            if (
                len(date_vals) != 3
                or cast(datetime, vals[3]) >= cast(datetime, vals[4])
                or cast(datetime, vals[2]) >= cast(datetime, vals[3])
            ):
                raise ParseError(
                    source="conab_progresso",
                    parser_version=PARSER_VERSION,
                    reason="Colunas semanais incompletas ou fora de ordem",
                )
            semana_atual = cast(datetime, vals[4]).strftime("%Y-%m-%d")
            in_data_rows = True
            continue

        if not in_data_rows or not cultura_atual or not operacao_atual:
            continue

        estado_raw = col_1
        if not estado_raw:
            continue

        if estado_raw.startswith("*") or estado_raw.startswith("("):
            continue
        if re.fullmatch(r"brasil", estado_raw, re.I):
            records.append(
                _build_record(cultura_atual, safra_atual, operacao_atual, "BR", semana_atual, vals)
            )
            continue
        n_estados = parse_agregado(estado_raw)
        if n_estados is not None:
            if cobertura_atual is not None and cobertura_atual[0] != n_estados:
                raise ParseError(
                    source="conab_progresso",
                    parser_version=PARSER_VERSION,
                    reason=(
                        f"Agregado de {n_estados} estados sob a nota de {cobertura_atual[0]} "
                        f"estados: {cultura_atual}"
                    ),
                )
            records.append(
                _build_record(
                    cultura_atual,
                    safra_atual,
                    operacao_atual,
                    ESTADO_MEDIA,
                    semana_atual,
                    vals,
                    n_estados,
                    cobertura_atual[1] if cobertura_atual is not None else None,
                )
            )
            continue
        if estado_raw.lower().startswith("valores") or estado_raw.lower().startswith("percentual"):
            in_data_rows = False
            continue
        if estado_raw.lower().startswith("estimativa"):
            continue

        uf = estado_para_uf(estado_raw)
        if uf not in ESTADOS_PARA_UF.values():
            logger.warning("conab_progresso_estado_desconhecido", estado=estado_raw)
            continue
        records.append(
            _build_record(cultura_atual, safra_atual, operacao_atual, uf, semana_atual, vals)
        )

    _validate_block(cultura_atual, operacao_atual, block_start, len(records))
    if not records:
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro extraido do XLSX",
        )

    result = pd.DataFrame(records, columns=COLUNAS_SAIDA)
    if result.duplicated(["cultura", "safra", "operacao", "estado", "semana_atual"]).any():
        raise ParseError(
            source="conab_progresso",
            parser_version=PARSER_VERSION,
            reason="Observações de progresso duplicadas",
        )
    result["revisado"] = result["revisado"].astype("boolean")
    result["n_estados"] = result["n_estados"].astype("Int64")
    result["cobertura_area_pct"] = result["cobertura_area_pct"].astype("float64")
    logger.info("conab_progresso_parse_ok", records=len(result))
    return result
