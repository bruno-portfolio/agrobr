from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.utils.geo import check_geopandas, parse_geojson_base
from agrobr.utils.io import concat_csv_pages

from .models import COLUNAS_IMOVEIS, COLUNAS_IMOVEIS_GEO, MAX_FEATURES_GEO, RENAME_MAP

logger = structlog.get_logger()

PARSER_VERSION = 1


def _normalize_columns(df: pd.DataFrame, output_cols: list[str]) -> pd.DataFrame:
    df = df.rename(columns=RENAME_MAP)

    df["data_criacao"] = pd.to_datetime(df["data_criacao"], errors="coerce")
    df["data_atualizacao"] = pd.to_datetime(
        df.get("data_atualizacao", pd.Series(dtype=str)), errors="coerce"
    )

    df["area_ha"] = pd.to_numeric(df["area_ha"].astype(str).str.replace(",", "."), errors="coerce")
    df["cod_municipio_ibge"] = pd.to_numeric(
        df.get("cod_municipio_ibge", pd.Series(dtype=str)), errors="coerce"
    ).astype("Int64")
    df["modulos_fiscais"] = pd.to_numeric(
        df.get("modulos_fiscais", pd.Series(dtype=str)).astype(str).str.replace(",", "."),
        errors="coerce",
    )

    df["uf"] = df["uf"].fillna("").str.strip().str.upper()
    df["status"] = df["status"].fillna("").str.strip().str.upper()
    df["tipo"] = df.get("tipo", pd.Series(dtype=str)).fillna("").str.strip().str.upper()
    df["municipio"] = df.get("municipio", pd.Series(dtype=str)).fillna("").str.strip()
    df["condicao"] = df.get("condicao", pd.Series(dtype=str)).fillna("").str.strip()
    df["cod_imovel"] = df["cod_imovel"].fillna("").astype(str).str.strip()

    cols = [c for c in output_cols if c in df.columns]
    return df[cols].copy().reset_index(drop=True)


_REQUIRED_COLS_RAW = {"cod_imovel", "status_imovel", "dat_criacao", "area", "uf"}


def parse_imoveis_csv(pages: list[bytes]) -> pd.DataFrame:
    df = concat_csv_pages(
        pages,
        source="sicar",
        parser_version=PARSER_VERSION,
        empty_columns=COLUNAS_IMOVEIS,
    )
    if df.empty:
        return df

    missing = _REQUIRED_COLS_RAW - set(df.columns)
    if missing:
        raise ParseError(
            source="sicar",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    df = _normalize_columns(df, COLUNAS_IMOVEIS)
    logger.info("sicar_parse_ok", records=len(df))
    return df


def parse_imoveis_geojson(
    pages: list[bytes], *, max_features: int | None = MAX_FEATURES_GEO
) -> Any:
    gpd = check_geopandas()

    if not pages:
        empty = gpd.GeoDataFrame(columns=COLUNAS_IMOVEIS_GEO)
        return empty.set_geometry("geometry")

    per_page_max = max_features if len(pages) == 1 else None
    gdfs = []
    for page in pages:
        gdf = parse_geojson_base(
            page,
            gpd,
            source="sicar",
            parser_version=PARSER_VERSION,
            required_cols=_REQUIRED_COLS_RAW,
            max_features=per_page_max,
            output_cols_empty=COLUNAS_IMOVEIS_GEO,
            truncation_event="sicar_geo_truncated",
        )
        if not gdf.empty:
            gdfs.append(gdf)

    if not gdfs:
        empty = gpd.GeoDataFrame(columns=COLUNAS_IMOVEIS_GEO)
        return empty.set_geometry("geometry")

    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")
    gdf = _normalize_columns(gdf, COLUNAS_IMOVEIS_GEO)
    logger.info("sicar_geojson_parse_ok", records=len(gdf))
    return gdf


def agregar_resumo(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)

    if total == 0:
        return pd.DataFrame(
            [
                {
                    "total": 0,
                    "ativos": 0,
                    "pendentes": 0,
                    "suspensos": 0,
                    "cancelados": 0,
                    "area_total_ha": 0.0,
                    "area_media_ha": 0.0,
                    "modulos_fiscais_medio": 0.0,
                    "por_tipo_IRU": 0,
                    "por_tipo_AST": 0,
                    "por_tipo_PCT": 0,
                }
            ]
        )

    status_counts = df["status"].value_counts()
    tipo_counts = df["tipo"].value_counts() if "tipo" in df.columns else pd.Series(dtype=int)

    resumo = {
        "total": total,
        "ativos": int(status_counts.get("AT", 0)),
        "pendentes": int(status_counts.get("PE", 0)),
        "suspensos": int(status_counts.get("SU", 0)),
        "cancelados": int(status_counts.get("CA", 0)),
        "area_total_ha": float(df["area_ha"].sum()) if "area_ha" in df.columns else 0.0,
        "area_media_ha": float(df["area_ha"].mean()) if "area_ha" in df.columns else 0.0,
        "modulos_fiscais_medio": (
            float(df["modulos_fiscais"].mean()) if "modulos_fiscais" in df.columns else 0.0
        ),
        "por_tipo_IRU": int(tipo_counts.get("IRU", 0)),
        "por_tipo_AST": int(tipo_counts.get("AST", 0)),
        "por_tipo_PCT": int(tipo_counts.get("PCT", 0)),
    }

    return pd.DataFrame([resumo])
