from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr.exceptions import ParseError
from agrobr.normalize import dates

from . import models


def parse_cot(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        contagens = {*models.POSITION_COLUMNS, *models.CHANGE_COLUMNS, "managed_money_net"}
        return pd.DataFrame(
            {
                coluna: pd.Series(
                    dtype="datetime64[ns]"
                    if coluna == "data"
                    else "Int64"
                    if coluna in contagens
                    else "str"
                )
                for coluna in models.COLUNAS_SAIDA
            }
        )
    df = pd.DataFrame(records)

    missing = [
        original
        for original, renamed in models.COLUMN_MAP.items()
        if original not in df.columns and renamed not in models.CHANGE_COLUMNS
    ]
    if missing:
        raise ParseError(
            source="cftc",
            parser_version=models.PARSER_VERSION,
            reason=f"Campos ausentes na resposta Socrata: {missing}",
        )

    df = df.reindex(columns=list(models.COLUMN_MAP)).rename(columns=models.COLUMN_MAP)
    dates.converter_coluna(df, "data", fonte="cftc", formato="ISO8601")

    for col in models.POSITION_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in models.CHANGE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["commodity"] = df["codigo_cftc"].map(models.CFTC_CONTRACTS).fillna(df["codigo_cftc"])

    _validate(df)

    for col in models.POSITION_COLUMNS:
        df[col] = df[col].astype("Int64")
    df["managed_money_net"] = df["managed_money_long"] - df["managed_money_short"]

    return df[models.COLUNAS_SAIDA].sort_values(["data", "commodity"]).reset_index(drop=True)


def _validate(df: pd.DataFrame) -> None:
    if df["data"].isna().any():
        raise ParseError(
            source="cftc",
            parser_version=models.PARSER_VERSION,
            reason="Datas inválidas na resposta",
        )

    nulas = [c for c in models.POSITION_COLUMNS if df[c].isna().any()]
    if nulas:
        raise ParseError(
            source="cftc",
            parser_version=models.PARSER_VERSION,
            reason=f"Posições nulas nas colunas: {nulas}",
        )

    negativas = [c for c in models.POSITION_COLUMNS if (df[c] < 0).any()]
    if negativas:
        raise ParseError(
            source="cftc",
            parser_version=models.PARSER_VERSION,
            reason=f"Posições negativas nas colunas: {negativas}",
        )
