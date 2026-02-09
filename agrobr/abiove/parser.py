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
            records = _parse_sheet(xls, sheet_name, ano)
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


def _parse_meses_rows(
    df: pd.DataFrame,
    ano: int | None,
    sheet_name: str,
) -> list[dict[str, Any]]:
    """Parseia formato com meses nas linhas.

    Formato esperado:
    Row 0-N:  Cabeçalhos (produto, volume/valor)
    Row N+1:  Janeiro    | val | val | val | val
    Row N+2:  Fevereiro  | val | val | val | val
    ...
    """
    records: list[dict[str, Any]] = []

    # Encontrar linhas com nomes de meses
    month_rows: list[tuple[int, int]] = []
    for idx in range(len(df)):
        first_col = str(df.iloc[idx, 0]).strip() if pd.notna(df.iloc[idx, 0]) else ""
        month = _detect_month(first_col)
        if month is not None:
            month_rows.append((idx, month))

    if len(month_rows) < 3:
        return []

    # Detectar cabeçalhos nas linhas anteriores aos meses
    header_start = max(0, month_rows[0][0] - 3)
    header_end = month_rows[0][0]

    # Mapear colunas para produtos: col_idx -> (produto, tipo)
    col_map: dict[int, tuple[str, str]] = {}

    for hdr_idx in range(header_start, header_end):
        for col_idx in range(1, len(df.columns)):
            val = df.iloc[hdr_idx, col_idx]
            if pd.isna(val):
                continue
            val_str = str(val).strip()

            produto = _detect_produto_from_header(val_str)
            if produto:
                col_map[col_idx] = (produto, "volume")
                if col_idx + 1 < len(df.columns):
                    col_map[col_idx + 1] = (produto, "receita")

    # Se não detectou cabeçalhos, tentar do nome da sheet
    if not col_map:
        produto = _detect_produto_from_header(sheet_name)
        if produto and len(df.columns) >= 2:
            col_map[1] = (produto, "volume")
            if len(df.columns) >= 3:
                col_map[2] = (produto, "receita")

    if not col_map:
        return []

    # Extrair dados dos meses
    for row_idx, month in month_rows:
        for col_idx, (produto, tipo) in col_map.items():
            if col_idx >= len(df.columns):
                continue

            value = _safe_float(df.iloc[row_idx, col_idx])
            if value is None:
                continue

            # Encontrar registro existente para este mês/produto ou criar novo
            existing = None
            for r in records:
                if r["mes"] == month and r["produto"] == produto:
                    existing = r
                    break

            if existing is None:
                existing = {
                    "ano": ano or 0,
                    "mes": month,
                    "produto": produto,
                    "volume_ton": 0.0,
                    "receita_usd_mil": None,
                }
                records.append(existing)

            if tipo == "volume":
                existing["volume_ton"] = value
            elif tipo == "receita":
                existing["receita_usd_mil"] = value

    return records


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
