"""Parser para planilhas de exportação ABIOVE.

PARSER_VERSION = 1: Parse dinâmico de Excel com detecção de layout.
Suporta dois formatos:
  - Meses nas linhas, produtos nas colunas (mais comum)
  - Formato tabular com colunas nomeadas
"""

from __future__ import annotations

import io
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import MESES_PT, normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 1


def _safe_float(value: Any) -> float | None:
    """Converte valor para float, tratando formatos brasileiros.

    Formatos aceitos:
    - Inteiro/float Python
    - "150.000" (milhares BR) -> 150000.0
    - "1.234,56" (decimal BR) -> 1234.56
    - "-", "–", "n.d.", "" -> None
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    s = str(value).strip()
    if not s or s in ("-", "–", "—", "n.d.", "n/d", "...", "nd"):
        return None

    # Formato BR: 1.234.567,89 -> 1234567.89
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        # Múltiplos pontos = separador de milhar: 1.234.567
        s = s.replace(".", "")
    elif "." in s:
        # Ponto único: verificar se parece separador de milhar BR
        # (exatamente 3 dígitos após o ponto)
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def _detect_month(text: Any) -> int | None:
    """Detecta mês a partir de texto (nome ou número).

    Args:
        text: Texto com nome ou número do mês.

    Returns:
        Número do mês (1-12) ou None se não reconhecido.
    """
    if text is None:
        return None

    s = str(text).strip().lower()

    # Exclui acumulados e totais
    skip_patterns = ["total", "acumulad", "anual", " a ", "/"]
    if any(p in s for p in skip_patterns):
        return None

    # Numérico
    try:
        n = int(s)
        return n if 1 <= n <= 12 else None
    except ValueError:
        pass

    return MESES_PT.get(s)


def _detect_produto_from_header(header: str) -> str | None:
    """Detecta produto a partir de texto do cabeçalho.

    Args:
        header: Texto do cabeçalho da coluna ou sheet.

    Returns:
        Nome canônico do produto ou None.
    """
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
    """Parseia planilha Excel de exportação ABIOVE.

    Detecta dinamicamente a estrutura da planilha e extrai
    dados de exportação do complexo soja.

    Args:
        data: Bytes do arquivo Excel.
        ano: Ano de referência (para preencher coluna ano).

    Returns:
        DataFrame: ano, mes, produto, volume_ton, receita_usd_mil.

    Raises:
        ParseError: Se não conseguir extrair dados.
    """
    try:
        xls = pd.ExcelFile(io.BytesIO(data))
    except Exception as e:
        raise ParseError(
            source="abiove",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao abrir Excel: {e}",
        ) from e

    all_records: list[dict[str, Any]] = []

    for sheet_name in xls.sheet_names:
        try:
            records = _parse_sheet(xls, str(sheet_name), ano)
            all_records.extend(records)
        except Exception:
            logger.warning("abiove_sheet_parse_error", sheet=sheet_name)
            continue

    if not all_records:
        raise ParseError(
            source="abiove",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum dado extraído. Sheets: {xls.sheet_names}",
        )

    df = pd.DataFrame(all_records)

    # Normalizar produto
    if "produto" in df.columns:
        df["produto"] = df["produto"].apply(normalize_produto)

    # Ordenar
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
    """Parseia uma sheet do Excel ABIOVE."""
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    if df_raw.empty or len(df_raw) < 2:
        return []

    # Estratégia 1: meses nas linhas, produtos nas colunas
    records = _parse_meses_rows(df_raw, ano, sheet_name)
    if records:
        return records

    # Estratégia 2: formato tabular simples
    records = _parse_tabular(df_raw, ano)
    if records:
        return records

    return []


def _find_month_col(df: pd.DataFrame) -> int:
    """Detecta qual coluna contém os nomes de mês.

    Testa colunas 0 e 1 procurando >= 3 meses reconhecíveis.

    Returns:
        Índice da coluna com meses (0 ou 1), default 0.
    """
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
    """Parseia formato com meses nas linhas.

    Suporta dois layouts:
      - Clássico: meses na coluna 0, produto por sheet
      - Multi-seção: meses na coluna 1, seções produto dentro da sheet
        (ex: "1.1. Exportações de soja em grão", depois meses Jan-Dez)
    """
    records: list[dict[str, Any]] = []

    month_col = _find_month_col(df)

    month_rows: list[tuple[int, int]] = []
    for idx in range(len(df)):
        cell = str(df.iloc[idx, month_col]).strip() if pd.notna(df.iloc[idx, month_col]) else ""
        month = _detect_month(cell)
        if month is not None:
            month_rows.append((idx, month))

    if len(month_rows) < 3:
        return []

    first_month_idx = month_rows[0][0]
    if first_month_idx > 0:
        prev_row = df.iloc[first_month_idx - 1]
        prev_vals = " ".join(str(v).strip().lower() for v in prev_row if pd.notna(v))
        tabular_keywords = ["produto", "product", "ncm"]
        if any(k in prev_vals for k in tabular_keywords):
            return []

    sections = _split_sections(df, month_col, month_rows, sheet_name)

    for produto, sec_months, data_cols in sections:
        for row_idx, month in sec_months:
            rec: dict[str, Any] = {
                "ano": ano or 0,
                "mes": month,
                "produto": produto,
                "volume_ton": 0.0,
                "receita_usd_mil": None,
            }
            for col_idx, tipo in data_cols.items():
                if col_idx >= len(df.columns):
                    continue
                value = _safe_float(df.iloc[row_idx, col_idx])
                if value is None:
                    continue
                if tipo == "volume":
                    rec["volume_ton"] = value
                elif tipo == "receita":
                    rec["receita_usd_mil"] = value
            if rec["volume_ton"] != 0.0 or rec["receita_usd_mil"] is not None:
                records.append(rec)

    return records


def _split_sections(
    df: pd.DataFrame,
    month_col: int,
    month_rows: list[tuple[int, int]],
    sheet_name: str,
) -> list[tuple[str, list[tuple[int, int]], dict[int, str]]]:
    """Divide month_rows em seções por produto.

    Dois modos:
      - Multi-coluna (clássico): um bloco de meses, múltiplos produtos nas colunas
      - Multi-seção (novo): blocos separados de meses, cada um com seu produto
    """
    groups: list[tuple[int, list[tuple[int, int]]]] = []
    current: list[tuple[int, int]] = []

    for _i, (row_idx, month) in enumerate(month_rows):
        if current and row_idx - current[-1][0] > 4:
            groups.append((current[0][0], list(current)))
            current = []
        current.append((row_idx, month))

    if current:
        groups.append((current[0][0], list(current)))

    if len(groups) == 1:
        first_row = groups[0][0]
        col_product_map = _detect_column_products(df, month_col, first_row)
        if col_product_map:
            return _build_column_sections(
                col_product_map,
                groups[0][1],
                df,
                month_col,
                first_row,
            )

    sections: list[tuple[str, list[tuple[int, int]], dict[int, str]]] = []

    for first_row, grp_months in groups:
        produto = _detect_section_produto(df, month_col, first_row, sheet_name)
        data_cols = _detect_data_cols(df, month_col, first_row)
        sections.append((produto, grp_months, data_cols))

    return sections


def _detect_column_products(
    df: pd.DataFrame,
    month_col: int,
    first_month_row: int,
) -> dict[int, str]:
    """Detecta produtos mapeados a colunas (formato clássico multi-coluna)."""
    col_products: dict[int, str] = {}
    for offset in range(1, 5):
        hdr_row = first_month_row - offset
        if hdr_row < 0:
            break
        for col_idx in range(month_col + 1, len(df.columns)):
            val = df.iloc[hdr_row, col_idx]
            if pd.isna(val):
                continue
            produto = _detect_produto_from_header(str(val))
            if produto and col_idx not in col_products:
                col_products[col_idx] = produto
    return col_products


def _build_column_sections(
    col_products: dict[int, str],
    month_rows: list[tuple[int, int]],
    df: pd.DataFrame,
    month_col: int,
    first_month_row: int,
) -> list[tuple[str, list[tuple[int, int]], dict[int, str]]]:
    """Constrói seções a partir de produtos mapeados por coluna."""
    produto_cols: dict[str, list[int]] = {}
    for col_idx, produto in sorted(col_products.items()):
        produto_cols.setdefault(produto, []).append(col_idx)

    type_map = _detect_col_types(df, month_col, first_month_row)

    sections: list[tuple[str, list[tuple[int, int]], dict[int, str]]] = []

    for produto, cols in produto_cols.items():
        data_cols: dict[int, str] = {}
        for c in cols:
            data_cols[c] = type_map.get(c, "volume" if not data_cols else "receita")
        sections.append((produto, month_rows, data_cols))

    return sections


def _detect_col_types(
    df: pd.DataFrame,
    month_col: int,
    first_month_row: int,
) -> dict[int, str]:
    """Detecta tipos de coluna (volume/receita) a partir dos sub-headers."""
    type_map: dict[int, str] = {}
    for offset in range(1, 4):
        hdr_row = first_month_row - offset
        if hdr_row < 0:
            break
        for col_idx in range(month_col + 1, len(df.columns)):
            if col_idx in type_map:
                continue
            val = df.iloc[hdr_row, col_idx]
            if pd.isna(val):
                continue
            val_str = str(val).strip().lower()
            if any(k in val_str for k in ["volume", "ton", "peso", "mil t", "quantidade"]):
                type_map[col_idx] = "volume"
            elif any(k in val_str for k in ["us$", "usd", "valor", "fob", "receita"]):
                type_map[col_idx] = "receita"
    return type_map


def _detect_section_produto(
    df: pd.DataFrame,
    _month_col: int,
    first_month_row: int,
    sheet_name: str,
) -> str:
    """Detecta produto de uma seção olhando linhas acima do primeiro mês."""
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

    produto = _detect_produto_from_header(sheet_name)
    return produto or "total"


def _detect_data_cols(
    df: pd.DataFrame,
    month_col: int,
    first_month_row: int,
) -> dict[int, str]:
    """Mapeia colunas de dados para tipo (volume/receita).

    Procura sub-cabeçalhos como "Peso Líquido" (volume) e
    "Valor FOB" (receita) nas linhas acima dos meses.
    Assume que cada grupo tem colunas de ano consecutivas;
    pega a coluna do ano mais recente (segunda do grupo).
    """
    col_map: dict[int, str] = {}

    for offset in range(1, 5):
        hdr_row = first_month_row - offset
        if hdr_row < 0:
            break
        for col_idx in range(month_col + 1, len(df.columns)):
            val = df.iloc[hdr_row, col_idx]
            if pd.isna(val):
                continue
            val_str = str(val).strip().lower()

            if any(k in val_str for k in ["peso", "volume", "ton", "mil t", "quantidade"]):
                target = _pick_latest_year_col(df, hdr_row, col_idx)
                col_map[target] = "volume"
            elif any(k in val_str for k in ["valor", "fob", "receita", "us$", "usd"]):
                target = _pick_latest_year_col(df, hdr_row, col_idx)
                col_map[target] = "receita"

    if not col_map:
        start = month_col + 1
        if start < len(df.columns):
            col_map[start] = "receita"
        if start + 1 < len(df.columns):
            col_map[start + 1] = "volume"

    return col_map


def _pick_latest_year_col(
    df: pd.DataFrame,
    header_row: int,
    group_start: int,
) -> int:
    """Dentro de um grupo de sub-colunas (ex: 2024 | 2025 | Var.%), retorna
    a coluna do ano mais recente com dados numéricos.

    Olha a linha abaixo do header_row para encontrar anos.
    """
    year_row = header_row + 1
    if year_row >= len(df):
        return group_start

    best_col = group_start
    best_year = 0

    for col_idx in range(group_start, min(group_start + 4, len(df.columns))):
        val = df.iloc[year_row, col_idx]
        if pd.isna(val):
            continue
        try:
            yr = int(float(str(val)))
            if 2000 <= yr <= 2100 and yr > best_year:
                best_year = yr
                best_col = col_idx
        except (ValueError, TypeError):
            pass

    return best_col


def _parse_tabular(
    df: pd.DataFrame,
    ano: int | None,
) -> list[dict[str, Any]]:
    """Parseia formato tabular simples com colunas nomeadas."""
    for hdr_idx in range(min(10, len(df))):
        row = df.iloc[hdr_idx]
        cols = [str(v).strip().lower() for v in row if pd.notna(v)]
        joined = " ".join(cols)

        has_mes = any(k in joined for k in ["mes", "mês", "month"])
        has_vol = any(k in joined for k in ["volume", "ton", "quantidade", "qtd"])

        if has_mes and has_vol:
            df_data = df.iloc[hdr_idx + 1 :].copy()
            df_data.columns = [str(v).strip().lower() for v in df.iloc[hdr_idx]]
            return _extract_tabular_records(df_data, ano)

    return []


def _extract_tabular_records(
    df: pd.DataFrame,
    ano: int | None,
) -> list[dict[str, Any]]:
    """Extrai registros de DataFrame tabular."""
    records: list[dict[str, Any]] = []

    mes_col = next((c for c in df.columns if "mes" in c or "mês" in c), None)
    vol_col = next(
        (c for c in df.columns if any(k in c for k in ["volume", "ton", "qtd"])),
        None,
    )
    receita_col = next(
        (c for c in df.columns if any(k in c for k in ["receita", "valor", "usd", "us$"])),
        None,
    )
    produto_col = next(
        (c for c in df.columns if any(k in c for k in ["produto", "product"])),
        None,
    )

    if not mes_col or not vol_col:
        return []

    for _, row in df.iterrows():
        mes = _detect_month(str(row.get(mes_col, "")))
        if mes is None:
            continue

        volume = _safe_float(row.get(vol_col))
        if volume is None:
            continue

        produto = "total"
        if produto_col and pd.notna(row.get(produto_col)):
            produto = normalize_produto(str(row[produto_col]))

        record: dict[str, Any] = {
            "ano": ano or 0,
            "mes": mes,
            "produto": produto,
            "volume_ton": volume,
            "receita_usd_mil": (_safe_float(row.get(receita_col)) if receita_col else None),
        }
        records.append(record)

    return records


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados por mês (soma volumes de todos os produtos).

    Args:
        df: DataFrame com dados detalhados.

    Returns:
        DataFrame agregado por ano/mês com produto="total".
    """
    if df.empty:
        return df

    group_cols = ["ano", "mes"]
    agg_cols: dict[str, str] = {"volume_ton": "sum"}

    if "receita_usd_mil" in df.columns and df["receita_usd_mil"].notna().any():
        agg_cols["receita_usd_mil"] = "sum"

    result = df.groupby(group_cols, as_index=False).agg(agg_cols)
    result["produto"] = "total"

    return result.sort_values(group_cols).reset_index(drop=True)
