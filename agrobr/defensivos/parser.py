from __future__ import annotations

import re

import pandas as pd

from agrobr.exceptions import ParseError
from agrobr.utils.io import read_csv_safe

from .models import (
    AUTORIZACOES_COLS,
    FORMULADOS_COLS_DROP,
    FORMULADOS_PRODUCT_COLS,
    FORMULADOS_RENAME,
    TECNICOS_COLS,
    TECNICOS_COLS_DROP,
    TECNICOS_RENAME,
)

PARSER_VERSION = 2

_REQUIRED_FORMULADOS = {"MARCA_COMERCIAL", "INGREDIENTE_ATIVO", "CULTURA"}
_REQUIRED_TECNICOS = {"CLASSE"}

_COMPOSITE_IA_COL = "INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)"
_RE_COMPOSITE_IA = re.compile(r"^(.+?)\s*\(([^)]*)\)\s*\(([^)]*)\)$")


def _strip_all_str_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.replace("\x96", "–", regex=False).str.strip()
    return df


def parse_formulados_csv(data: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not data.strip():
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason="CSV formulados vazio",
        )

    df = read_csv_safe(
        data,
        source="defensivos",
        parser_version=PARSER_VERSION,
        label="CSV formulados",
        sep=";",
        dtype=str,
        keep_default_na=False,
    )

    missing = _REQUIRED_FORMULADOS - set(df.columns)
    if missing:
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason=f"Colunas faltando no CSV formulados: {sorted(missing)}",
        )

    drop_cols = [c for c in FORMULADOS_COLS_DROP if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    rename_map = {k: v for k, v in FORMULADOS_RENAME.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    df = _strip_all_str_cols(df)
    df = df.replace("", pd.NA)

    product_cols = [c for c in FORMULADOS_PRODUCT_COLS if c in df.columns]
    product_df = df[product_cols].drop_duplicates(subset="nr_registro", keep="first")
    product_df = product_df.reset_index(drop=True)

    auth_cols = [c for c in AUTORIZACOES_COLS if c in df.columns]
    auth_df = df[auth_cols].reset_index(drop=True)

    return product_df, auth_df


def _split_composite_ia(value: str) -> tuple[str, str]:
    m = _RE_COMPOSITE_IA.match(value)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return value.strip(), ""


def parse_tecnicos_csv(data: bytes) -> pd.DataFrame:
    if not data.strip():
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason="CSV tecnicos vazio",
        )

    df = read_csv_safe(
        data,
        source="defensivos",
        parser_version=PARSER_VERSION,
        label="CSV técnicos",
        sep=";",
        dtype=str,
        keep_default_na=False,
    )

    missing = _REQUIRED_TECNICOS - set(df.columns)
    if missing:
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason=f"Colunas faltando no CSV tecnicos: {sorted(missing)}",
        )

    if _COMPOSITE_IA_COL in df.columns:
        split = df[_COMPOSITE_IA_COL].apply(_split_composite_ia)
        df["INGREDIENTE_ATIVO"] = split.str[0]
        df["GRUPO_QUIMICI"] = split.str[1]
        df = df.drop(columns=[_COMPOSITE_IA_COL])

    drop_cols = [c for c in TECNICOS_COLS_DROP if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    rename_map = {k: v for k, v in TECNICOS_RENAME.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    df = _strip_all_str_cols(df)
    df = df.replace("", pd.NA)

    out_cols = [c for c in TECNICOS_COLS if c in df.columns]
    return df[out_cols].reset_index(drop=True)
