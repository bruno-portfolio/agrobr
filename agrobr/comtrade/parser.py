from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from .models import COLUNAS_MIRROR, COLUNAS_SAIDA

logger = structlog.get_logger()

PARSER_VERSION = 1

COLUNAS_MAP: dict[str, str] = {
    "period": "periodo",
    "reporterCode": "reporter_code",
    "reporterISO": "reporter_iso",
    "reporterDesc": "reporter",
    "partnerCode": "partner_code",
    "partnerISO": "partner_iso",
    "partnerDesc": "partner",
    "flowCode": "fluxo_code",
    "flowDesc": "fluxo",
    "cmdCode": "hs_code",
    "cmdDesc": "produto_desc",
    "netWgt": "peso_liquido_kg",
    "grossWgt": "peso_bruto_kg",
    "fobvalue": "valor_fob_usd",
    "cifvalue": "valor_cif_usd",
    "primaryValue": "valor_primario_usd",
    "qty": "quantidade",
    "qtyUnitAbbr": "unidade_qtd",
    "aggrLevel": "nivel_hs",
}

_NUMERIC_COLS = [
    "peso_liquido_kg",
    "peso_bruto_kg",
    "valor_fob_usd",
    "valor_cif_usd",
    "valor_primario_usd",
    "quantidade",
]


def parse_trade_data(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=COLUNAS_SAIDA)

    df = pd.DataFrame(records)

    rename = {k: v for k, v in COLUNAS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    df["periodo"] = df["periodo"].astype(str)

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "peso_liquido_kg" in df.columns:
        df["volume_ton"] = df["peso_liquido_kg"] / 1000.0
    else:
        df["volume_ton"] = float("nan")

    df["ano"] = df["periodo"].str[:4].astype(int)
    df["mes"] = pd.to_numeric(
        df["periodo"].apply(lambda p: p[4:6] if len(str(p)) >= 6 else None),
        errors="coerce",
    ).astype("Int64")

    for col in COLUNAS_SAIDA:
        if col not in df.columns:
            df[col] = None

    df = df[COLUNAS_SAIDA]

    sort_cols = [
        c for c in ["periodo", "hs_code", "reporter_iso", "partner_iso"] if c in df.columns
    ]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info("comtrade_parse_ok", records=len(df))

    return df


def parse_mirror(
    df_reporter: pd.DataFrame,
    df_partner: pd.DataFrame,
    reporter_iso: str,
    partner_iso: str,
) -> pd.DataFrame:
    if df_reporter.empty and df_partner.empty:
        return pd.DataFrame(columns=COLUNAS_MIRROR)

    merge_keys = ["periodo", "hs_code"]

    left = df_reporter[
        merge_keys + ["peso_liquido_kg", "valor_fob_usd", "volume_ton", "produto_desc"]
    ].copy()
    left = left.rename(
        columns={
            "peso_liquido_kg": "peso_liquido_kg_reporter",
            "valor_fob_usd": "valor_fob_usd_reporter",
            "volume_ton": "volume_ton_reporter",
        }
    )

    right = df_partner[
        merge_keys + ["peso_liquido_kg", "valor_fob_usd", "valor_cif_usd", "volume_ton"]
    ].copy()
    right = right.rename(
        columns={
            "peso_liquido_kg": "peso_liquido_kg_partner",
            "valor_fob_usd": "valor_fob_usd_partner",
            "valor_cif_usd": "valor_cif_usd_partner",
            "volume_ton": "volume_ton_partner",
        }
    )

    if "produto_desc" in right.columns:
        right = right.drop(columns=["produto_desc"])

    df = pd.merge(left, right, on=merge_keys, how="outer")

    df["reporter_iso"] = reporter_iso
    df["partner_iso"] = partner_iso

    df["ano"] = df["periodo"].str[:4].astype(int)
    df["mes"] = pd.to_numeric(
        df["periodo"].apply(lambda p: p[4:6] if len(str(p)) >= 6 else None),
        errors="coerce",
    ).astype("Int64")

    df["diff_peso_kg"] = df["peso_liquido_kg_reporter"] - df["peso_liquido_kg_partner"]
    df["diff_valor_fob_usd"] = df["valor_fob_usd_reporter"] - df["valor_fob_usd_partner"]

    df["ratio_valor"] = df["valor_fob_usd_reporter"] / df["valor_cif_usd_partner"].replace(
        0, float("nan")
    )
    df["ratio_peso"] = df["peso_liquido_kg_reporter"] / df["peso_liquido_kg_partner"].replace(
        0, float("nan")
    )

    for col in COLUNAS_MIRROR:
        if col not in df.columns:
            df[col] = None

    df = df[COLUNAS_MIRROR]
    df = df.sort_values(["periodo", "hs_code"]).reset_index(drop=True)

    logger.info("comtrade_mirror_parse_ok", records=len(df))

    return df
