from __future__ import annotations

import re
from io import BytesIO
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import CustoTotal, ItemCusto, classify_categoria, normalize_cultura

logger = structlog.get_logger()

PARSER_VERSION = 1

_COE_PATTERN = re.compile(r"custo\s*operacional\s*efetivo|c\.?\s*o\.?\s*e\.?", re.IGNORECASE)
_COT_PATTERN = re.compile(r"custo\s*operacional\s*total|c\.?\s*o\.?\s*t\.?", re.IGNORECASE)
_CT_PATTERN = re.compile(r"custo\s*total(?!\s*operacional)|c\.?\s*t\.?\s*$", re.IGNORECASE)

_SECTION_HEADERS = re.compile(
    r"^(i+\s*[-–.]|[abc]\s*[-–.]|\d+\s*[-–.])\s*",
    re.IGNORECASE,
)

MIN_COLUMNS = 4


def _find_header_row(df_raw: pd.DataFrame) -> int:
    keywords = {
        "item",
        "especificação",
        "especificacao",
        "valor",
        "unidade",
        "quantidade",
        "preço",
        "preco",
        "participação",
        "participacao",
        "r$/ha",
        "total/ha",
    }

    for idx in range(min(20, len(df_raw))):
        row_values = [str(v).lower().strip() for v in df_raw.iloc[idx] if pd.notna(v)]
        row_text = " ".join(row_values)

        matches = sum(1 for kw in keywords if kw in row_text)
        if matches >= 2:
            return idx

    raise ParseError(
        source="conab_custo",
        parser_version=PARSER_VERSION,
        reason="Não foi possível encontrar linha de cabeçalho na planilha",
    )


def _identify_columns(headers: list[str]) -> dict[str, int]:
    mapping: dict[str, int] = {}

    for i, h in enumerate(headers):
        h_lower = h.lower().strip()

        if any(
            w in h_lower
            for w in ("item", "componente", "especificação", "especificacao", "discriminação")
        ):
            if "item" not in mapping:
                mapping["item"] = i

        elif any(w in h_lower for w in ("unidade", "unid")):
            mapping["unidade"] = i

        elif any(w in h_lower for w in ("quantidade", "qtd", "qtde", "quant")):
            mapping["quantidade_ha"] = i

        elif any(
            w in h_lower for w in ("preço unitário", "preco unitario", "preço unit", "vlr. unit")
        ):
            mapping["preco_unitario"] = i

        elif any(
            w in h_lower for w in ("valor total", "total/ha", "valor/ha", "vlr. total", "r$/ha")
        ):
            mapping["valor_ha"] = i

        elif any(w in h_lower for w in ("participação", "participacao", "part.", "%")):
            mapping["participacao_pct"] = i

    return mapping


def _safe_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".").replace("R$", "").replace("%", "").strip()
        if not cleaned or cleaned == "-":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def parse_planilha(
    xlsx: BytesIO,
    cultura: str,
    uf: str,
    safra: str,
    tecnologia: str = "alta",
    sheet_name: int | str = 0,
) -> tuple[list[ItemCusto], CustoTotal | None]:
    cultura_norm = normalize_cultura(cultura)

    try:
        df_raw = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)
    except Exception as e:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler Excel: {e}",
        ) from e

    if df_raw.empty or len(df_raw.columns) < MIN_COLUMNS:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Planilha vazia ou com poucas colunas ({len(df_raw.columns)})",
        )

    header_idx = _find_header_row(df_raw)
    headers = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[header_idx]]
    col_map = _identify_columns(headers)

    if "item" not in col_map or "valor_ha" not in col_map:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatórias não encontradas: item={col_map.get('item')}, valor_ha={col_map.get('valor_ha')}. Headers: {headers}",
        )

    items: list[ItemCusto] = []
    coe_value: float | None = None
    cot_value: float | None = None
    ct_value: float | None = None
    current_category = "outros"

    data_start = header_idx + 1

    for row_idx in range(data_start, len(df_raw)):
        row = df_raw.iloc[row_idx]

        item_name = str(row.iloc[col_map["item"]]) if pd.notna(row.iloc[col_map["item"]]) else ""
        item_name = item_name.strip()

        if not item_name:
            continue

        valor = _safe_float(row.iloc[col_map["valor_ha"]])

        if _COE_PATTERN.search(item_name):
            if valor is not None:
                coe_value = valor
            continue

        if _COT_PATTERN.search(item_name):
            if valor is not None:
                cot_value = valor
            continue

        if _CT_PATTERN.search(item_name):
            if valor is not None:
                ct_value = valor
            continue

        if _SECTION_HEADERS.match(item_name):
            current_category = classify_categoria(item_name)
            if valor is None or valor == 0.0:
                continue

        if valor is None:
            possible_cat = classify_categoria(item_name)
            if possible_cat != "outros":
                current_category = possible_cat
            continue

        categoria = classify_categoria(item_name)
        if categoria == "outros":
            categoria = current_category

        item = ItemCusto(
            cultura=cultura_norm,
            uf=uf.upper(),
            safra=safra,
            tecnologia=tecnologia,
            categoria=categoria,
            item=item_name,
            unidade=str(row.iloc[col_map["unidade"]]).strip()
            if "unidade" in col_map and pd.notna(row.iloc[col_map["unidade"]])
            else None,
            quantidade_ha=_safe_float(row.iloc[col_map["quantidade_ha"]])
            if "quantidade_ha" in col_map
            else None,
            preco_unitario=_safe_float(row.iloc[col_map["preco_unitario"]])
            if "preco_unitario" in col_map
            else None,
            valor_ha=valor,
            participacao_pct=_safe_float(row.iloc[col_map["participacao_pct"]])
            if "participacao_pct" in col_map
            else None,
        )

        items.append(item)

    if not items:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum item de custo extraído da planilha (cultura={cultura}, uf={uf})",
        )

    custo_total: CustoTotal | None = None
    if coe_value is not None:
        custo_total = CustoTotal(
            cultura=cultura_norm,
            uf=uf.upper(),
            safra=safra,
            tecnologia=tecnologia,
            coe_ha=coe_value,
            cot_ha=cot_value,
            ct_ha=ct_value,
        )
    elif items:
        coe_categorias = {"insumos", "operacoes", "mao_de_obra"}
        coe_from_items = sum(item.valor_ha for item in items if item.categoria in coe_categorias)
        if coe_from_items > 0:
            custo_total = CustoTotal(
                cultura=cultura_norm,
                uf=uf.upper(),
                safra=safra,
                tecnologia=tecnologia,
                coe_ha=coe_from_items,
                cot_ha=cot_value,
                ct_ha=ct_value,
            )

    logger.info(
        "conab_custo_parsed",
        cultura=cultura_norm,
        uf=uf.upper(),
        safra=safra,
        items=len(items),
        coe=coe_value,
    )

    return items, custo_total


def items_to_dataframe(items: list[ItemCusto]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()

    records = [item.model_dump() for item in items]
    df = pd.DataFrame(records)

    numeric_cols = ["quantidade_ha", "preco_unitario", "valor_ha", "participacao_pct"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["cultura", "uf", "safra", "categoria", "item"]).reset_index(drop=True)
    return df
