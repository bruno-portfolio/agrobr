"""Parser para dados SICOR (crédito rural BCB).

Converte resposta JSON da API Olinda em DataFrames normalizados.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 1

# Mapeamento de colunas API → nomes agrobr
COLUNAS_MAP: dict[str, str] = {
    "Safra": "safra",
    "AnoEmissao": "ano_emissao",
    "MesEmissao": "mes_emissao",
    "cdUF": "cd_uf",
    "UF": "uf",
    "cdMunicipio": "cd_municipio",
    "Municipio": "municipio",
    "Produto": "produto",
    "Finalidade": "finalidade",
    "Fonte": "fonte_recurso",
    "Programa": "programa",
    "Valor": "valor",
    "AreaFinanciada": "area_financiada",
    "QtdContratos": "qtd_contratos",
    "VlrMedio": "valor_medio",
}


def parse_credito_rural(
    dados: list[dict[str, Any]],
    finalidade: str = "custeio",
) -> pd.DataFrame:
    """Parseia dados brutos SICOR em DataFrame normalizado.

    Args:
        dados: Lista de dicts retornada pela API Olinda.
        finalidade: Finalidade para metadado (custeio, investimento, etc).

    Returns:
        DataFrame com colunas normalizadas.

    Raises:
        ParseError: Se dados estiverem vazios ou malformados.
    """
    if not dados:
        raise ParseError(
            source="bcb",
            parser_version=PARSER_VERSION,
            reason="Resposta SICOR vazia",
        )

    df = pd.DataFrame(dados)

    rename = {k: v for k, v in COLUNAS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    for col in ("valor", "area_financiada", "valor_medio"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("ano_emissao", "mes_emissao", "qtd_contratos"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "produto" in df.columns:
        df["produto"] = df["produto"].str.lower().str.strip()

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    if "municipio" in df.columns:
        df["municipio"] = df["municipio"].str.strip()

    if "finalidade" not in df.columns:
        df["finalidade"] = finalidade

    sort_cols = [c for c in ("safra", "uf", "municipio", "produto") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info(
        "bcb_parsed",
        records=len(df),
        columns=df.columns.tolist(),
    )

    return df


def agregar_por_uf(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados de crédito por UF.

    Args:
        df: DataFrame parseado (nível município).

    Returns:
        DataFrame agregado por (safra, uf, produto) com somas de valor,
        area_financiada e qtd_contratos.
    """
    if df.empty:
        return df

    group_cols = [c for c in ("safra", "uf", "produto") if c in df.columns]
    if not group_cols:
        return df

    agg_dict: dict[str, str | tuple[str, str]] = {}
    if "valor" in df.columns:
        agg_dict["valor"] = "sum"
    if "area_financiada" in df.columns:
        agg_dict["area_financiada"] = "sum"
    if "qtd_contratos" in df.columns:
        agg_dict["qtd_contratos"] = "sum"

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False).agg(agg_dict)

    return result.sort_values(group_cols).reset_index(drop=True)
