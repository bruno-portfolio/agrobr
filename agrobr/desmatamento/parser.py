from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import COLUNAS_SAIDA_DETER, COLUNAS_SAIDA_PRODES, estado_para_uf

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_prodes_csv(data: bytes, bioma: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(io.BytesIO(data), encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(data), encoding="latin-1")
    except Exception as e:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV PRODES: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason="CSV PRODES vazio",
        )

    required = {"year", "area_km", "state"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    df["ano"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["area_km2"] = pd.to_numeric(df["area_km"], errors="coerce")
    df["uf"] = df["state"].fillna("").apply(estado_para_uf)
    df["classe"] = df.get("main_class", pd.Series(dtype=str)).fillna("desmatamento")
    df["satelite"] = df.get("satellite", pd.Series(dtype=str)).fillna("")
    df["sensor"] = df.get("sensor", pd.Series(dtype=str)).fillna("")
    df["bioma"] = bioma

    output_cols = [c for c in COLUNAS_SAIDA_PRODES if c in df.columns]
    df = df[output_cols].copy()
    df = df.reset_index(drop=True)

    logger.info("desmatamento_prodes_parse_ok", records=len(df), bioma=bioma)
    return df


def parse_deter_csv(data: bytes, bioma: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(io.BytesIO(data), encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(data), encoding="latin-1")
    except Exception as e:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV DETER: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason="CSV DETER vazio",
        )

    required = {"view_date", "areamunkm", "uf"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    df["data"] = pd.to_datetime(df["view_date"], errors="coerce").dt.date
    df["area_km2"] = pd.to_numeric(df["areamunkm"], errors="coerce")
    df["classe"] = df.get("classname", pd.Series(dtype=str)).fillna("")
    df["uf"] = df["uf"].fillna("").str.upper()
    df["municipio"] = df.get("municipality", pd.Series(dtype=str)).fillna("")
    df["municipio_id"] = pd.to_numeric(
        df.get("mun_geocod", pd.Series(dtype=str)), errors="coerce"
    ).astype("Int64")
    df["satelite"] = df.get("satellite", pd.Series(dtype=str)).fillna("")
    df["sensor"] = df.get("sensor", pd.Series(dtype=str)).fillna("")
    df["bioma"] = bioma

    output_cols = [c for c in COLUNAS_SAIDA_DETER if c in df.columns]
    df = df[output_cols].copy()
    df = df.reset_index(drop=True)

    logger.info("desmatamento_deter_parse_ok", records=len(df), bioma=bioma)
    return df
