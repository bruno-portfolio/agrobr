from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize import dates
from agrobr.normalize.regions import UFS_VALIDAS, ibge_para_uf
from agrobr.utils.geo import check_geopandas, check_pyogrio

from .models import (
    ASSENTAMENTOS_COLUNAS_SAIDA,
    ASSENTAMENTOS_COLUNAS_SAIDA_GEO,
    ASSENTAMENTOS_DATE_COLS,
    ASSENTAMENTOS_NUMERIC_COLS,
    ASSENTAMENTOS_RENAME_MAP,
    ASSENTAMENTOS_REQUIRED_COLS,
    DBF_ENCODING,
    SIGEF_COLUNAS_SAIDA,
    SIGEF_COLUNAS_SAIDA_GEO,
    SIGEF_DATE_COLS,
    SIGEF_RENAME_MAP,
    SIGEF_REQUIRED_COLS,
    SNCI_COLUNAS_SAIDA,
    SNCI_COLUNAS_SAIDA_GEO,
    SNCI_DATE_COLS,
    SNCI_NUMERIC_COLS,
    SNCI_RENAME_MAP,
    SNCI_REQUIRED_COLS,
)

logger = _log.get_logger(__name__)

PARSER_VERSION = 2

BBox = tuple[float, float, float, float]


def _read_tabular(zip_path: Path, *, bbox: BBox | None = None) -> pd.DataFrame:
    pyogrio = check_pyogrio()
    df = pyogrio.read_dataframe(zip_path, encoding=DBF_ENCODING, read_geometry=False, bbox=bbox)
    return cast(pd.DataFrame, df)


def _read_geo(zip_path: Path, *, bbox: BBox | None = None) -> Any:
    gpd = check_geopandas()
    return gpd.read_file(zip_path, encoding=DBF_ENCODING, bbox=bbox)


def _validate_required(df: pd.DataFrame, required: frozenset[str], label: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="acervo_fundiario",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes em {label}: {sorted(missing)}",
        )


def _safe_ibge_to_uf(codigo: Any) -> str | None:
    try:
        return ibge_para_uf(int(codigo))
    except (ValueError, TypeError):
        return None


def _resolve_uf_from_ibge(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["uf"] = df["uf_id"].apply(_safe_ibge_to_uf)
    return df.drop(columns=["uf_id"])


def _normalize_uf_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["uf"] = df["uf"].astype("string").str.strip().str.upper()
    return df


def _coerce_dates(df: pd.DataFrame, date_cols: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()
    for col in date_cols:
        if col in df.columns:
            dates.converter_coluna(df, col, fonte="acervo_fundiario", dayfirst=True)
    return df


def _coerce_numeric(df: pd.DataFrame, numeric_cols: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _log_dirty_uf(df: pd.DataFrame, label: str) -> None:
    invalid_mask = ~df["uf"].isin(UFS_VALIDAS) & df["uf"].notna()
    n_invalid = int(invalid_mask.sum())
    if n_invalid > 0:
        counts = df.loc[invalid_mask, "uf"].value_counts().to_dict()
        logger.warning(
            "acervo_fundiario_dirty_uf_data",
            label=label,
            n_invalid=n_invalid,
            total=len(df),
            invalid_ufs=counts,
        )


def _select_output(df: pd.DataFrame, output_cols: list[str]) -> pd.DataFrame:
    cols = [c for c in output_cols if c in df.columns]
    return df[cols].reset_index(drop=True)


def _make_geometries_valid(gdf: Any) -> Any:
    invalid_mask = gdf.geometry.notna() & ~gdf.geometry.is_valid
    n_invalid = int(invalid_mask.sum())
    if n_invalid > 0:
        from shapely.validation import make_valid

        gdf = gdf.copy()
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)
        logger.warning(
            "acervo_fundiario_geom_repaired",
            invalid=n_invalid,
            total=len(gdf),
        )
    return gdf


def parse_sigef(zip_path: Path, *, bbox: BBox | None = None) -> pd.DataFrame:
    df = _read_tabular(zip_path, bbox=bbox)
    _validate_required(df, SIGEF_REQUIRED_COLS, "sigef")
    df = df.rename(columns=SIGEF_RENAME_MAP)
    df = _resolve_uf_from_ibge(df)
    df = _normalize_uf_column(df)
    df = _coerce_dates(df, SIGEF_DATE_COLS)
    df = _select_output(df, SIGEF_COLUNAS_SAIDA)
    logger.info("acervo_fundiario_sigef_parse_ok", records=len(df))
    return df


def parse_sigef_geo(zip_path: Path, *, bbox: BBox | None = None) -> Any:
    gdf = _read_geo(zip_path, bbox=bbox)
    _validate_required(gdf, SIGEF_REQUIRED_COLS, "sigef")
    gdf = gdf.rename(columns=SIGEF_RENAME_MAP)
    gdf = _resolve_uf_from_ibge(gdf)
    gdf = _normalize_uf_column(gdf)
    gdf = _coerce_dates(gdf, SIGEF_DATE_COLS)
    gdf = _make_geometries_valid(gdf)
    gdf = _select_output(gdf, SIGEF_COLUNAS_SAIDA_GEO)
    logger.info("acervo_fundiario_sigef_geo_parse_ok", records=len(gdf))
    return gdf


def parse_snci(zip_path: Path, *, bbox: BBox | None = None) -> pd.DataFrame:
    df = _read_tabular(zip_path, bbox=bbox)
    _validate_required(df, SNCI_REQUIRED_COLS, "snci")
    df = df.rename(columns=SNCI_RENAME_MAP)
    df = _normalize_uf_column(df)
    df = _coerce_dates(df, SNCI_DATE_COLS)
    df = _coerce_numeric(df, SNCI_NUMERIC_COLS)
    df = _select_output(df, SNCI_COLUNAS_SAIDA)
    logger.info("acervo_fundiario_snci_parse_ok", records=len(df))
    return df


def parse_snci_geo(zip_path: Path, *, bbox: BBox | None = None) -> Any:
    gdf = _read_geo(zip_path, bbox=bbox)
    _validate_required(gdf, SNCI_REQUIRED_COLS, "snci")
    gdf = gdf.rename(columns=SNCI_RENAME_MAP)
    gdf = _normalize_uf_column(gdf)
    gdf = _coerce_dates(gdf, SNCI_DATE_COLS)
    gdf = _coerce_numeric(gdf, SNCI_NUMERIC_COLS)
    gdf = _make_geometries_valid(gdf)
    gdf = _select_output(gdf, SNCI_COLUNAS_SAIDA_GEO)
    logger.info("acervo_fundiario_snci_geo_parse_ok", records=len(gdf))
    return gdf


def parse_assentamentos(
    zip_path: Path, *, uf: str | None = None, bbox: BBox | None = None
) -> pd.DataFrame:
    df = _read_tabular(zip_path, bbox=bbox)
    _validate_required(df, ASSENTAMENTOS_REQUIRED_COLS, "assentamentos")
    df = df.rename(columns=ASSENTAMENTOS_RENAME_MAP)
    df = _normalize_uf_column(df)
    df = _coerce_dates(df, ASSENTAMENTOS_DATE_COLS)
    df = _coerce_numeric(df, ASSENTAMENTOS_NUMERIC_COLS)
    _log_dirty_uf(df, "assentamentos")
    if uf is not None:
        df = df[df["uf"] == uf].reset_index(drop=True)
    df = _select_output(df, ASSENTAMENTOS_COLUNAS_SAIDA)
    logger.info("acervo_fundiario_assentamentos_parse_ok", records=len(df), uf=uf)
    return df


def parse_assentamentos_geo(
    zip_path: Path, *, uf: str | None = None, bbox: BBox | None = None
) -> Any:
    gdf = _read_geo(zip_path, bbox=bbox)
    _validate_required(gdf, ASSENTAMENTOS_REQUIRED_COLS, "assentamentos")
    gdf = gdf.rename(columns=ASSENTAMENTOS_RENAME_MAP)
    gdf = _normalize_uf_column(gdf)
    gdf = _coerce_dates(gdf, ASSENTAMENTOS_DATE_COLS)
    gdf = _coerce_numeric(gdf, ASSENTAMENTOS_NUMERIC_COLS)
    _log_dirty_uf(gdf, "assentamentos_geo")
    if uf is not None:
        gdf = gdf[gdf["uf"] == uf].reset_index(drop=True)
    gdf = _make_geometries_valid(gdf)
    gdf = _select_output(gdf, ASSENTAMENTOS_COLUNAS_SAIDA_GEO)
    logger.info("acervo_fundiario_assentamentos_geo_parse_ok", records=len(gdf), uf=uf)
    return gdf
