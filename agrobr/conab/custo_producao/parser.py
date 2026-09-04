from __future__ import annotations

import re
from io import BytesIO

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.normalize.regions import UFS_VALIDAS
from agrobr.utils.io import open_excel_safe, read_excel_safe

from .models import CustoTotal, ItemCusto, classify_categoria, normalize_cultura

logger = structlog.get_logger()

PARSER_VERSION = 2

_COE_PATTERN = re.compile(
    r"custo\s*operacional\s*efetivo|c\.?\s*o\.?\s*e\.?|custo\s*variável|custo\s*variavel|total\s+das\s+despesas\s+de\s+custeio",
    re.IGNORECASE,
)
_COT_PATTERN = re.compile(
    r"custo\s*operacional\s*total|c\.?\s*o\.?\s*t\.?|custo\s*operacional\s*\(",
    re.IGNORECASE,
)
_CT_PATTERN = re.compile(r"custo\s*total(?!\s*operacional)|c\.?\s*t\.?\s*$", re.IGNORECASE)

_SECTION_HEADERS = re.compile(
    r"^(i+\s*[-–.]|[abc]\s*[-–.]|\d+\s*[-–.])\s*",
    re.IGNORECASE,
)

MIN_COLUMNS = 4


def _find_header(df_raw: pd.DataFrame) -> tuple[int, list[str]]:
    keywords = {
        "item",
        "especificação",
        "especificacao",
        "discriminação",
        "discriminacao",
        "valor",
        "unidade",
        "quantidade",
        "preço",
        "preco",
        "participação",
        "participacao",
        "r$/ha",
        "total/ha",
        "custo por ha",
        "custo",
    }
    limit = min(20, len(df_raw))

    for idx in range(limit):
        row_values = [str(v).lower().strip() for v in df_raw.iloc[idx] if pd.notna(v)]
        row_text = " ".join(row_values)
        if sum(1 for kw in keywords if kw in row_text) >= 2:
            headers = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[idx]]
            return idx + 1, headers

    best: tuple[int, list[str]] | None = None
    best_quality = (0, 0)
    for idx in range(limit - 1):
        r1 = df_raw.iloc[idx]
        r2 = df_raw.iloc[idx + 1]
        merged = [
            str(r2.iloc[c])
            if pd.notna(r2.iloc[c])
            else (str(r1.iloc[c]) if pd.notna(r1.iloc[c]) else "")
            for c in range(len(r1))
        ]
        combined_text = " ".join(v.lower().strip() for v in merged if v)
        if sum(1 for kw in keywords if kw in combined_text) < 2:
            continue
        col_map = _identify_columns(merged)
        has_required = "item" in col_map and "valor_ha" in col_map
        quality = (int(has_required), len(col_map))
        if quality > best_quality:
            best_quality = quality
            best = (idx + 2, merged)

    if best is not None:
        return best

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
            for w in (
                "item",
                "componente",
                "especificação",
                "especificacao",
                "discriminação",
                "discriminacao",
            )
        ):
            if "item" not in mapping:
                mapping["item"] = i

        elif any(w in h_lower for w in ("unidade", "unid")):
            if "unidade" not in mapping:
                mapping["unidade"] = i

        elif any(w in h_lower for w in ("quantidade", "qtd", "qtde", "quant")):
            if "quantidade_ha" not in mapping:
                mapping["quantidade_ha"] = i

        elif any(
            w in h_lower for w in ("preço unitário", "preco unitario", "preço unit", "vlr. unit")
        ):
            if "preco_unitario" not in mapping:
                mapping["preco_unitario"] = i

        elif any(
            w in h_lower
            for w in (
                "valor total",
                "total/ha",
                "valor/ha",
                "vlr. total",
                "r$/ha",
                "custo por ha",
                "custo/ha",
                "custo por hectare",
            )
        ):
            if "valor_ha" not in mapping:
                mapping["valor_ha"] = i

        elif h_lower.startswith("custo/") and "preco_unitario" not in mapping:
            mapping["preco_unitario"] = i

        elif (
            any(w in h_lower for w in ("participação", "participacao", "part.", "%"))
            and "participacao_pct" not in mapping
        ):
            mapping["participacao_pct"] = i

    return mapping


_NON_DATA_SHEET = re.compile(r"ndice|index|sumario|sumário", re.IGNORECASE)


def select_data_sheet(
    xlsx: bytes | BytesIO,
    uf: str | None = None,
    safra: str | None = None,
) -> str:
    xf = open_excel_safe(xlsx, source="conab_custo", parser_version=PARSER_VERSION)
    names: list[str] = [str(n) for n in xf.sheet_names]
    if len(names) == 1:
        return names[0]

    sheets = [n for n in names if not _NON_DATA_SHEET.search(n)]
    if not sheets:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Nenhuma sheet de dados encontrada (sheets={names})",
        )

    if uf:
        pat = re.compile(rf"\b{re.escape(uf)}\b", re.IGNORECASE)
        filtered = [s for s in sheets if pat.search(s)]
        if filtered:
            sheets = filtered

    if safra:
        years = re.findall(r"\d{4}", safra)
        short_match = re.search(r"(\d{4})/(\d{2})\b", safra)
        if short_match:
            full_year = short_match.group(1)[:2] + short_match.group(2)
            if full_year not in years:
                years.append(full_year)
        if years:
            filtered = [s for s in sheets if any(y in s for y in years)]
            if filtered:
                sheets = filtered

    return sheets[-1]


def _parse_sheet_info(sheet_name: str) -> tuple[str | None, str | None]:
    uf: str | None = None
    year: str | None = None
    uf_match = re.search(r"\b([A-Z]{2})\b", sheet_name)
    if uf_match and uf_match.group(1) in UFS_VALIDAS:
        uf = uf_match.group(1)
    year_match = re.search(r"\b(20\d{2})\b", sheet_name)
    if year_match:
        year = year_match.group(1)
    return uf, year


def available_sheet_ufs(xlsx: bytes | BytesIO) -> list[str]:
    xf = open_excel_safe(xlsx, source="conab_custo", parser_version=PARSER_VERSION)
    return sorted(
        {uf for name in xf.sheet_names if (uf := _parse_sheet_info(str(name))[0]) is not None}
    )


def _refine_valor_column(
    df_raw: pd.DataFrame,
    data_start: int,
    col_map: dict[str, int],
) -> None:
    if "valor_ha" not in col_map:
        return
    col_idx = col_map["valor_ha"]
    end = min(data_start + 5, len(df_raw))
    sample = df_raw.iloc[data_start:end, col_idx] if data_start < end else pd.Series(dtype=object)
    if any(safe_float(v) is not None for v in sample):
        return
    for offset in (1, 2):
        candidate = col_idx + offset
        if candidate >= len(df_raw.columns):
            break
        sample_c = df_raw.iloc[data_start:end, candidate]
        if any(safe_float(v) is not None for v in sample_c):
            col_map["valor_ha"] = candidate
            return


def _cell_float(row: pd.Series, col_map: dict[str, int], key: str) -> float | None:
    if key not in col_map:
        return None
    return safe_float(row.iloc[col_map[key]], strip=("R$", "%"))


def _cell_str(row: pd.Series, col_map: dict[str, int], key: str) -> str | None:
    if key not in col_map or pd.isna(row.iloc[col_map[key]]):
        return None
    return str(row.iloc[col_map[key]]).strip()


def parse_planilha(
    xlsx: BytesIO,
    cultura: str,
    uf: str,
    safra: str,
    tecnologia: str = "alta",
    sheet_name: int | str = 0,
) -> tuple[list[ItemCusto], CustoTotal | None]:
    cultura_norm = normalize_cultura(cultura)

    df_raw = read_excel_safe(
        xlsx,
        source="conab_custo",
        parser_version=PARSER_VERSION,
        label="Excel custo",
        sheet_name=sheet_name,
        header=None,
    )

    if df_raw.empty or len(df_raw.columns) < MIN_COLUMNS:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Planilha vazia ou com poucas colunas ({len(df_raw.columns)})",
        )

    data_start, headers = _find_header(df_raw)
    col_map = _identify_columns(headers)
    _refine_valor_column(df_raw, data_start, col_map)

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

    for row_idx in range(data_start, len(df_raw)):
        row = df_raw.iloc[row_idx]

        item_name = _cell_str(row, col_map, "item") or ""

        if not item_name:
            continue

        valor = _cell_float(row, col_map, "valor_ha")

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
            unidade=_cell_str(row, col_map, "unidade"),
            quantidade_ha=_cell_float(row, col_map, "quantidade_ha"),
            preco_unitario=_cell_float(row, col_map, "preco_unitario"),
            valor_ha=valor,
            participacao_pct=_cell_float(row, col_map, "participacao_pct"),
        )

        items.append(item)

    if not items:
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum item de custo extraído da planilha (cultura={cultura}, uf={uf})",
        )

    coe = coe_value
    if coe is None:
        coe_categorias = {"insumos", "operacoes", "mao_de_obra"}
        coe_from_items = sum(item.valor_ha for item in items if item.categoria in coe_categorias)
        if coe_from_items > 0:
            coe = coe_from_items

    custo_total: CustoTotal | None = None
    if coe is not None:
        custo_total = CustoTotal(
            cultura=cultura_norm,
            uf=uf.upper(),
            safra=safra,
            tecnologia=tecnologia,
            coe_ha=coe,
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
