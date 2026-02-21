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

    rename = {k: v for k, v in IMEA_COLUMNS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    if "cadeia_id" in df.columns:
        df["cadeia"] = df["cadeia_id"].apply(lambda x: cadeia_name(int(x)) if pd.notna(x) else "")

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    if "variacao" in df.columns:
        df["variacao"] = pd.to_numeric(df["variacao"], errors="coerce")

    drop_cols = ["cadeia_id", "indicador_id", "tipo_localidade_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    sort_cols = [c for c in ["cadeia", "localidade", "unidade"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info("imea_parse_ok", records=len(df))

    return df


def filter_by_unidade(df: pd.DataFrame, unidade: str) -> pd.DataFrame:
    if df.empty or not unidade:
        return df
    return df[df["unidade"] == unidade].reset_index(drop=True)


def filter_by_safra(df: pd.DataFrame, safra: str) -> pd.DataFrame:
    if df.empty or not safra:
        return df
    return df[df["safra"] == safra].reset_index(drop=True)
