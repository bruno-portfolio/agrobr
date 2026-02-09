"""Parser para dados DERAL (planilha PC.xls semanal).

PARSER_VERSION = 1: Parse do Painel de Culturas (PC.xls),
detecção dinâmica de sheets por produto, extração de
condição/estágio/progresso.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pandas as pd
import structlog

from .models import DERAL_PRODUTOS, normalize_condicao, normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 1


def _safe_float(val: Any) -> float | None:
    """Converte valor para float, retornando None se inválido."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if pd.isna(val):
            return None
        return float(val)
    s = str(val).strip()
    if not s or s in ("-", "–", "...", "n.d.", "n.d", "*"):
        return None
    s = s.replace("%", "").strip()
    # BR format: 1.234,56
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _detect_produto_from_sheet(sheet_name: str) -> str | None:
    """Detecta produto a partir do nome da sheet."""
    name = sheet_name.strip().lower()
    for alias, canonical in sorted(
        _build_sheet_map().items(),
        key=lambda x: -len(x[0]),
    ):
        if alias in name:
            return canonical
    return None


def _build_sheet_map() -> dict[str, str]:
    """Mapa de substrings -> produto canônico para detecção de sheet."""
    m: dict[str, str] = {}
    for key, label in DERAL_PRODUTOS.items():
        m[label.lower()] = key
        m[key] = key
    # Aliases extras
    m["safrinha"] = "milho_2"
    m["milho verão"] = "milho_1"
    m["milho verao"] = "milho_1"
    return m


def parse_pc_xls(data: bytes) -> pd.DataFrame:
    """Parseia planilha PC.xls do DERAL.

    Extrai dados de condição das lavouras de todas as sheets.

    Args:
        data: Bytes do arquivo .xls.

    Returns:
        DataFrame: produto, data, condicao, pct, plantio_pct, colheita_pct.
    """
    try:
        xls = pd.ExcelFile(io.BytesIO(data))
    except Exception as exc:
        logger.error("deral_parse_error", error=str(exc))
        return _empty_df()

    all_records: list[dict[str, Any]] = []

    for sheet_name in xls.sheet_names:
        produto = _detect_produto_from_sheet(str(sheet_name))
        if produto is None:
            logger.debug("deral_skip_sheet", sheet=sheet_name)
            continue

        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        except Exception as exc:
            logger.warning("deral_sheet_error", sheet=sheet_name, error=str(exc))
            continue

        records = _extract_condicao_from_sheet(df, produto)
        all_records.extend(records)

    if not all_records:
        return _empty_df()

    result = pd.DataFrame(all_records)

    # Ordenar
    sort_cols = [c for c in ["produto", "data", "condicao"] if c in result.columns]
    if sort_cols:
        result = result.sort_values(sort_cols).reset_index(drop=True)

    logger.info("deral_parse_ok", records=len(result))
    return result


def _extract_condicao_from_sheet(
    df: pd.DataFrame,
    produto: str,
) -> list[dict[str, Any]]:
    """Extrai registros de condição de uma sheet do PC.xls.

    Procura padrões de:
    - Linhas com "boa"/"média"/"ruim" e percentuais
    - Linhas com "plantio" ou "colheita" e percentuais
    """
    records: list[dict[str, Any]] = []
    data_ref = _find_data_referencia(df)

    for row_idx in range(len(df)):
        row = df.iloc[row_idx]
        row_values = [str(v).strip().lower() for v in row if pd.notna(v)]

        # Procurar padrão condição (boa/média/ruim) com percentuais
        for col_idx in range(len(row)):
            cell = row.iloc[col_idx]
            if pd.isna(cell):
                continue
            cell_str = str(cell).strip().lower()

            if cell_str in ("boa", "bom", "média", "media", "ruim", "má", "ma"):
                pct = _find_pct_near(row, col_idx)
                records.append(
                    {
                        "produto": normalize_produto(produto),
                        "data": data_ref,
                        "condicao": normalize_condicao(cell_str),
                        "pct": pct,
                        "plantio_pct": None,
                        "colheita_pct": None,
                    }
                )

        # Procurar progresso plantio/colheita
        row_text = " ".join(row_values)
        if "plantio" in row_text or "semeadura" in row_text:
            pct = _find_pct_in_row(row)
            if pct is not None:
                records.append(
                    {
                        "produto": normalize_produto(produto),
                        "data": data_ref,
                        "condicao": "",
                        "pct": None,
                        "plantio_pct": pct,
                        "colheita_pct": None,
                    }
                )

        if "colheita" in row_text:
            pct = _find_pct_in_row(row)
            if pct is not None:
                records.append(
                    {
                        "produto": normalize_produto(produto),
                        "data": data_ref,
                        "condicao": "",
                        "pct": None,
                        "plantio_pct": None,
                        "colheita_pct": pct,
                    }
                )

    return records


def _find_data_referencia(df: pd.DataFrame) -> str:
    """Tenta encontrar data de referência na planilha."""
    for row_idx in range(min(10, len(df))):
        for col_idx in range(min(10, len(df.columns))):
            cell = df.iloc[row_idx, col_idx]
            if pd.isna(cell):
                continue
            cell_str = str(cell).strip()
            # Padrão dd/mm/yyyy ou dd/mm/yy
            match = re.search(r"\d{2}/\d{2}/\d{2,4}", cell_str)
            if match:
                return match.group(0)
    return ""


def _find_pct_near(row: pd.Series, col_idx: int) -> float | None:
    """Procura percentual na célula adjacente à condição."""
    # Procura na próxima coluna
    for offset in [1, -1, 2, -2]:
        idx = col_idx + offset
        if 0 <= idx < len(row):
            val = _safe_float(row.iloc[idx])
            if val is not None and 0 <= val <= 100:
                return val
    return None


def _find_pct_in_row(row: pd.Series) -> float | None:
    """Procura primeiro percentual numérico na row."""
    for val in row:
        if pd.isna(val):
            continue
        num = _safe_float(val)
        if num is not None and 0 <= num <= 100:
            return num
    return None


def filter_by_produto(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    """Filtra DataFrame por produto."""
    if df.empty or not produto:
        return df
    key = normalize_produto(produto)
    return df[df["produto"] == key].reset_index(drop=True)


def _empty_df() -> pd.DataFrame:
    """Retorna DataFrame vazio com schema correto."""
    return pd.DataFrame(
        columns=[
            "produto",
            "data",
            "condicao",
            "pct",
            "plantio_pct",
            "colheita_pct",
        ]
    )
