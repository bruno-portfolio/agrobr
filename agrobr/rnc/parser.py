from __future__ import annotations

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.utils.io import read_csv_safe

from .models import (
    _REQUIRED_PROT,
    _REQUIRED_REG,
    DATE_COLS_PROT,
    DATE_COLS_REG,
    PROTEGIDAS_COLS,
    PROTEGIDAS_RENAME,
    REGISTRADAS_COLS,
    REGISTRADAS_RENAME,
)

logger = structlog.get_logger()

PARSER_VERSION = 1


def _parse_csv(
    data: bytes,
    *,
    required: frozenset[str],
    rename: dict[str, str],
    date_cols: list[str],
    output_cols: list[str],
    label: str,
) -> pd.DataFrame:
    df = read_csv_safe(
        data,
        source="rnc",
        parser_version=PARSER_VERSION,
        label=f"CSV {label}",
        sep=",",
        dtype=str,
        keep_default_na=False,
        quotechar='"',
    )

    if df.empty:
        raise ParseError(
            source="rnc",
            parser_version=PARSER_VERSION,
            reason=f"CSV {label} vazio",
        )

    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="rnc",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatórias ausentes em {label}: {missing}",
        )

    df = df.rename(columns=rename)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    cols = [c for c in output_cols if c in df.columns]
    df = df[cols].reset_index(drop=True)

    logger.info("rnc_parse_ok", label=label, records=len(df))
    return df


def parse_registradas_csv(data: bytes) -> pd.DataFrame:
    return _parse_csv(
        data,
        required=_REQUIRED_REG,
        rename=REGISTRADAS_RENAME,
        date_cols=DATE_COLS_REG,
        output_cols=REGISTRADAS_COLS,
        label="registradas",
    )


def parse_protegidas_csv(data: bytes) -> pd.DataFrame:
    return _parse_csv(
        data,
        required=_REQUIRED_PROT,
        rename=PROTEGIDAS_RENAME,
        date_cols=DATE_COLS_PROT,
        output_cols=PROTEGIDAS_COLS,
        label="protegidas",
    )
