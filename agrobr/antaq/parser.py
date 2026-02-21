from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.antaq.models import (
    COLUNAS_ATRACACAO,
    COLUNAS_CARGA,
    COLUNAS_MERCADORIA,
    PARSER_VERSION,
    RENAME_FINAL,
)

logger = structlog.get_logger()


def _read_txt(content: str, usecols: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(
        io.StringIO(content),
        sep=";",
        encoding="utf-8",
        dtype=str,
        usecols=usecols,
        low_memory=False,
    )
    df.columns = df.columns.str.strip()
    return df


def parse_atracacao(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_ATRACACAO if c in available_cols]

    df = _read_txt(content, usecols=usecols)
    logger.info("antaq_parse_atracacao", rows=len(df))
    return df


def parse_carga(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_CARGA if c in available_cols]

    df = _read_txt(content, usecols=usecols)

    if "VLPesoCargaBruta" in df.columns:
        df["VLPesoCargaBruta"] = (
            df["VLPesoCargaBruta"]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    if "QTCarga" in df.columns:
        df["QTCarga"] = pd.to_numeric(
            df["QTCarga"].str.replace(",", ".", regex=False),
            errors="coerce",
        )

    if "TEU" in df.columns:
        df["TEU"] = pd.to_numeric(df["TEU"], errors="coerce").fillna(0).astype(int)

    logger.info("antaq_parse_carga", rows=len(df))
    return df


def parse_mercadoria(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_MERCADORIA if c in available_cols]

    df = _read_txt(content, usecols=usecols)
    logger.info("antaq_parse_mercadoria", rows=len(df))
    return df


def join_movimentacao(
    df_atracacao: pd.DataFrame,
    df_carga: pd.DataFrame,
    df_mercadoria: pd.DataFrame,
) -> pd.DataFrame:
    df = df_carga.merge(
        df_atracacao[
            [
                "IDAtracacao",
                "Porto Atracação",
                "Complexo Portuário",
                "Terminal",
                "Município",
                "SGUF",
                "Região Geográfica",
                "Ano",
                "Mes",
                "Data Atracação",
            ]
        ],
        on="IDAtracacao",
        how="left",
    )

    if "CDMercadoria" in df.columns and "CDMercadoria" in df_mercadoria.columns:
        merc_cols = [
            c
            for c in ["CDMercadoria", "Grupo de Mercadoria", "Nomenclatura Simplificada Mercadoria"]
            if c in df_mercadoria.columns
        ]
        df = df.merge(
            df_mercadoria[merc_cols].drop_duplicates(subset=["CDMercadoria"]),
            on="CDMercadoria",
            how="left",
        )

    rename = {k: v for k, v in RENAME_FINAL.items() if k in df.columns}
    df = df.rename(columns=rename)

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    if "mes" in df.columns:
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")

    final_cols = [
        c
        for c in [
            "ano",
            "mes",
            "data_atracacao",
            "tipo_navegacao",
            "tipo_operacao",
            "natureza_carga",
            "sentido",
            "porto",
            "complexo_portuario",
            "terminal",
            "municipio",
            "uf",
            "regiao",
            "cd_mercadoria",
            "mercadoria",
            "grupo_mercadoria",
            "origem",
            "destino",
            "peso_bruto_ton",
            "qt_carga",
            "teu",
        ]
        if c in df.columns
    ]

    df = df[final_cols]

    df = df.sort_values([c for c in ["ano", "mes", "uf", "porto"] if c in df.columns]).reset_index(
        drop=True
    )

    logger.info(
        "antaq_join_ok",
        rows=len(df),
        columns=df.columns.tolist(),
        parser_version=PARSER_VERSION,
    )
    return df
