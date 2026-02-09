"""Parser para dados IMEA (normalização de resposta JSON).

PARSER_VERSION = 1: Mapeamento de colunas API -> agrobr,
normalização de nomes e tipos.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from .models import IMEA_COLUMNS_MAP, cadeia_name

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_cotacoes(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """Parseia resposta JSON da API de cotações IMEA.

    Args:
        records: Lista de dicts da resposta JSON.

    Returns:
        DataFrame: cadeia, localidade, valor, variacao, safra,
                   unidade, unidade_descricao, data_publicacao.
    """
    if not records:
        return pd.DataFrame(
            columns=[
                "cadeia",
                "localidade",
                "valor",
                "variacao",
                "safra",
                "unidade",
                "unidade_descricao",
                "data_publicacao",
            ]
        )

    df = pd.DataFrame(records)

    # Renomear colunas
    rename = {k: v for k, v in IMEA_COLUMNS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Adicionar nome da cadeia
    if "cadeia_id" in df.columns:
        df["cadeia"] = df["cadeia_id"].apply(lambda x: cadeia_name(int(x)) if pd.notna(x) else "")

    # Converter tipos
    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    if "variacao" in df.columns:
        df["variacao"] = pd.to_numeric(df["variacao"], errors="coerce")

    # Remover colunas auxiliares
    drop_cols = ["cadeia_id", "indicador_id", "tipo_localidade_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Ordenar
    sort_cols = [c for c in ["cadeia", "localidade", "unidade"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info("imea_parse_ok", records=len(df))

    return df


def filter_by_unidade(df: pd.DataFrame, unidade: str) -> pd.DataFrame:
    """Filtra DataFrame por unidade de medida.

    Args:
        df: DataFrame com dados IMEA.
        unidade: Sigla da unidade (ex: "R$/sc", "R$/t", "%").

    Returns:
        DataFrame filtrado.
    """
    if df.empty or not unidade:
        return df
    return df[df["unidade"] == unidade].reset_index(drop=True)


def filter_by_safra(df: pd.DataFrame, safra: str) -> pd.DataFrame:
    """Filtra DataFrame por safra.

    Args:
        df: DataFrame com dados IMEA.
        safra: Safra no formato "24/25" ou similar.

    Returns:
        DataFrame filtrado.
    """
    if df.empty or not safra:
        return df
    return df[df["safra"] == safra].reset_index(drop=True)
