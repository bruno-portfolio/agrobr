from __future__ import annotations

import re
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import open_excel_safe

from .models import DERAL_PRODUTOS, normalize_condicao, normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 1


def _detect_produto_from_sheet(sheet_name: str) -> str | None:
    name = sheet_name.strip().lower()
    for alias, canonical in sorted(
        _build_sheet_map().items(),
        key=lambda x: -len(x[0]),
    ):
        if alias in name:
            return canonical
    return None


def _build_sheet_map() -> dict[str, str]:
    m: dict[str, str] = {}
    for key, label in DERAL_PRODUTOS.items():
        m[label.lower()] = key
        m[key] = key
    m["safrinha"] = "milho_2"
    m["milho verão"] = "milho_1"
    m["milho verao"] = "milho_1"
    return m


def parse_pc_xls(data: bytes) -> pd.DataFrame:
    try:
        xls = open_excel_safe(data, source="deral", parser_version=PARSER_VERSION)
    except Exception as exc:
        raise ParseError(
            source="deral",
            parser_version=PARSER_VERSION,
            reason=f"Falha ao abrir PC.xls: {exc}",
        ) from exc

    all_records: list[dict[str, Any]] = []

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        except Exception as exc:
            logger.warning("deral_sheet_error", sheet=sheet_name, error=str(exc))
            continue

        produto = _detect_produto_from_sheet(str(sheet_name))
        if produto is not None:
            records = _extract_condicao_from_sheet(df, produto)
            all_records.extend(records)
        elif _is_multi_produto_sheet(df):
            records = _extract_multi_produto_sheet(df, str(sheet_name))
            all_records.extend(records)
        else:
            logger.debug("deral_skip_sheet", sheet=sheet_name)

    if not all_records:
        raise ParseError(
            source="deral",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum registro reconhecido nas abas: {xls.sheet_names}",
        )

    result = pd.DataFrame(all_records)

    sort_cols = [c for c in ["produto", "data", "condicao"] if c in result.columns]
    if sort_cols:
        result = result.sort_values(sort_cols).reset_index(drop=True)

    logger.info("deral_parse_ok", records=len(result))
    return result


def _is_multi_produto_sheet(df: pd.DataFrame) -> bool:
    if len(df) < 6 or len(df.columns) < 7:
        return False

    for row_idx in range(min(8, len(df))):
        row_text = " ".join(str(v).lower() for v in df.iloc[row_idx] if pd.notna(v))
        if "condi" in row_text and ("boa" in row_text or "ruim" in row_text):
            return True
        if "plantada" in row_text and "colhida" in row_text:
            return True
    return False


def _extract_multi_produto_sheet(
    df: pd.DataFrame,
    sheet_name: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    header_row = -1
    col_ruim = col_media = col_boa = -1
    col_plantada = col_colhida = -1

    for row_idx in range(min(10, len(df))):
        for col_idx in range(len(df.columns)):
            cell = df.iloc[row_idx, col_idx]
            if pd.isna(cell):
                continue
            cell_str = str(cell).strip().lower()
            if cell_str == "ruim":
                col_ruim = col_idx
                header_row = row_idx
            elif cell_str in ("média", "media", "m\xe9dia"):
                col_media = col_idx
            elif cell_str == "boa":
                col_boa = col_idx
            elif cell_str == "plantada":
                col_plantada = col_idx
            elif cell_str == "colhida":
                col_colhida = col_idx

    if header_row < 0 or col_boa < 0:
        return []

    data_ref = _find_data_referencia(df)
    if not data_ref:
        data_ref = sheet_name

    for row_idx in range(header_row + 1, len(df)):
        cell0 = df.iloc[row_idx, 0]
        if pd.isna(cell0):
            continue
        cell_str = str(cell0).strip()

        if not cell_str or cell_str.upper().startswith("SAFRA"):
            continue

        produto = _detect_produto_from_row_label(cell_str)
        if produto is None:
            continue

        for col_idx, condicao in [
            (col_ruim, "ruim"),
            (col_media, "media"),
            (col_boa, "boa"),
        ]:
            if col_idx < 0 or col_idx >= len(df.columns):
                continue
            pct = safe_float(df.iloc[row_idx, col_idx], strip="%")
            records.append(
                {
                    "produto": normalize_produto(produto),
                    "data": data_ref,
                    "condicao": condicao,
                    "pct": pct,
                    "plantio_pct": (
                        safe_float(df.iloc[row_idx, col_plantada], strip="%")
                        if col_plantada >= 0
                        else None
                    ),
                    "colheita_pct": (
                        safe_float(df.iloc[row_idx, col_colhida], strip="%")
                        if col_colhida >= 0
                        else None
                    ),
                }
            )

    return records


def _detect_produto_from_row_label(label: str) -> str | None:
    s = label.strip().lower()

    from .models import _PRODUTO_ALIASES

    without_parentheses = re.sub(r"[()]", "", s)
    if without_parentheses in _PRODUTO_ALIASES:
        return _PRODUTO_ALIASES[without_parentheses]

    safra_match = re.search(r"([12])\s*[ªaºo�]?\s*safra", without_parentheses)
    base = re.sub(r"[12]\s*[ªaºo�]?\s*safra", "", without_parentheses).strip()
    if base in _PRODUTO_ALIASES:
        canonical = _PRODUTO_ALIASES[base]
        if safra_match and canonical in {"feijao", "milho"}:
            return f"{canonical}_{safra_match.group(1)}"
        if safra_match and canonical == "soja" and safra_match.group(1) == "2":
            return None
        return canonical

    for alias, canonical in _PRODUTO_ALIASES.items():
        if alias in without_parentheses:
            return canonical

    return None


def _extract_condicao_from_sheet(
    df: pd.DataFrame,
    produto: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    data_ref = _find_data_referencia(df)

    for row_idx in range(len(df)):
        row = df.iloc[row_idx]
        row_values = [str(v).strip().lower() for v in row if pd.notna(v)]

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
    for row_idx in range(min(10, len(df))):
        for col_idx in range(min(10, len(df.columns))):
            cell = df.iloc[row_idx, col_idx]
            if pd.isna(cell):
                continue
            cell_str = str(cell).strip()
            match = re.search(r"\d{2}/\d{2}/\d{2,4}", cell_str)
            if match:
                return match.group(0)
    return ""


def _find_pct_near(row: pd.Series, col_idx: int) -> float | None:
    for offset in [1, -1, 2, -2]:
        idx = col_idx + offset
        if 0 <= idx < len(row):
            val = safe_float(row.iloc[idx], strip="%")
            if val is not None and 0 <= val <= 100:
                return val
    return None


def _find_pct_in_row(row: pd.Series) -> float | None:
    for val in row:
        if pd.isna(val):
            continue
        num = safe_float(val, strip="%")
        if num is not None and 0 <= num <= 100:
            return num
    return None


def filter_by_produto(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    if df.empty or not produto:
        return df
    key = normalize_produto(produto)
    if key in {"feijao", "milho"}:
        return df[df["produto"].isin([key, f"{key}_1", f"{key}_2"])].reset_index(drop=True)
    return df[df["produto"] == key].reset_index(drop=True)
