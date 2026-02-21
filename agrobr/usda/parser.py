from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from .models import PSD_ATTRIBUTES, PSD_COLUMNS_MAP, commodity_name

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_psd_response(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(
            columns=[
                "commodity_code",
                "commodity",
                "country_code",
                "country",
                "market_year",
                "attribute",
                "attribute_br",
                "value",
                "unit",
            ]
        )

    df = pd.DataFrame(records)

    rename = {k: v for k, v in PSD_COLUMNS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    if "commodity_code" in df.columns:
        df["commodity"] = df["commodity_code"].apply(commodity_name)

    if "attribute_id" in df.columns:
        df["attribute_br"] = df["attribute_id"].map(PSD_ATTRIBUTES).fillna("")

    drop_cols = ["calendar_year", "month", "attribute_id", "unit_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    sort_cols = [c for c in ["market_year", "country_code", "attribute"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info("usda_parse_ok", records=len(df))

    return df


def filter_attributes(
    df: pd.DataFrame,
    attributes: list[str] | None = None,
) -> pd.DataFrame:
    if not attributes or df.empty:
        return df

    attrs_lower = [a.lower() for a in attributes]
    mask = df["attribute"].str.lower().isin(attrs_lower)

    if "attribute_br" in df.columns:
        mask = mask | df["attribute_br"].str.lower().isin(attrs_lower)

    return df[mask].reset_index(drop=True)


def pivot_attributes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    index_cols = [
        c
        for c in ["commodity_code", "commodity", "country_code", "country", "market_year"]
        if c in df.columns
    ]

    col_name = "attribute_br" if "attribute_br" in df.columns else "attribute"

    try:
        result = df.pivot_table(
            index=index_cols,
            columns=col_name,
            values="value",
            aggfunc="first",
        ).reset_index()

        result.columns = [c[0] if isinstance(c, tuple) else c for c in result.columns]
        return result
    except Exception:
        logger.warning("usda_pivot_failed", exc_info=True)
        return df
