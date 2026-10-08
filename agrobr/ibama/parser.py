from __future__ import annotations

import warnings
from typing import Any

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize import dates
from agrobr.utils.geo import check_geopandas, wkt_within_limits
from agrobr.utils.io import read_csv_safe
from agrobr.utils.result import ATRIBUTO_AVISOS

from .models import (
    COLUNAS_SAIDA,
    CSV_COLUMN_MAP,
    EDICAO_COLUMN_CSV,
    GEOM_COLUMN_CSV,
)

logger = _log.get_logger(__name__)

PARSER_VERSION = 4

_FORMATO_DATA = "%Y-%m-%d %H:%M:%S"


def _read_embargos(csv_bytes: bytes, columns: list[str]) -> pd.DataFrame:
    return read_csv_safe(
        csv_bytes,
        source="ibama",
        parser_version=PARSER_VERSION,
        sep=";",
        usecols=[*columns, EDICAO_COLUMN_CSV],
        dtype=str,
    )


def _edicao(df: pd.DataFrame) -> str | None:
    valores = df[EDICAO_COLUMN_CSV]
    datas = dates.converter_datas(valores, fonte="ibama", formato=_FORMATO_DATA).datas
    return None if datas.isna().all() else str(valores.iloc[datas.argmax()])


def _normalize(
    df: pd.DataFrame,
    *,
    uf: str | None,
    bbox: tuple[float, float, float, float] | None,
    edicao: str | None,
) -> pd.DataFrame:
    df = df.rename(columns=CSV_COLUMN_MAP)

    publicacao = pd.to_datetime(edicao, errors="coerce") if edicao else None
    ate = publicacao if isinstance(publicacao, pd.Timestamp) else None
    if edicao is None and df[EDICAO_COLUMN_CSV].notna().any():
        aviso = (
            f"ibama: a edição do arquivo ({EDICAO_COLUMN_CSV}) não foi lida, nenhum valor é data "
            "válida; a data de ato posterior à edição não foi anulada."
        )
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)
    for coluna in ("data_embargo", "data_desembargo"):
        dates.converter_coluna(df, coluna, fonte="ibama", formato=_FORMATO_DATA, ate=ate)
    if df["area_embargada_ha"].str.contains(".", regex=False, na=False).any():
        raise ParseError(
            source="ibama",
            parser_version=PARSER_VERSION,
            reason="QTD_AREA_EMBARGADA com ponto: a fonte publica vírgula decimal, sem milhar",
        )
    df["area_embargada_ha"] = pd.to_numeric(
        df["area_embargada_ha"].str.replace(",", ".", regex=False), errors="coerce"
    )
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["cancelado"] = df["cancelado"].eq("S")
    df["uf"] = df["uf"].fillna("").str.strip().str.upper()
    df["municipio"] = df["municipio"].fillna("").str.strip()

    if uf is not None:
        df = df[df["uf"] == uf]
    if bbox is not None:
        min_lon, min_lat, max_lon, max_lat = bbox
        nao_informado = df["longitude"].eq(0) & df["latitude"].eq(0)
        df = df[
            df["longitude"].between(min_lon, max_lon)
            & df["latitude"].between(min_lat, max_lat)
            & ~nao_informado
        ]
    return df


def parse_embargos_csv(
    csv_bytes: bytes,
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> pd.DataFrame:
    df = _read_embargos(csv_bytes, list(CSV_COLUMN_MAP))
    edicao = _edicao(df)
    df = _normalize(df, uf=uf, bbox=bbox, edicao=edicao)
    df = df[COLUNAS_SAIDA].reset_index(drop=True)
    df.attrs[EDICAO_COLUMN_CSV] = edicao
    logger.info("ibama_embargos_parse_ok", records=len(df), edicao=edicao)
    return df


def parse_embargos_geo(
    csv_bytes: bytes,
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> Any:
    """`uf` filtra antes de carregar o WKT; `bbox` filtra pela interseção do polígono,
    o que exige ler o WKT de toda a seleção (~2 s para o Brasil inteiro)."""
    gpd = check_geopandas()
    import shapely

    if uf is None and bbox is None:
        logger.warning(
            "ibama_embargos_geo_sem_filtro",
            hint="Sem uf/bbox o parse de WKT cobre o Brasil inteiro (lento)",
        )

    df = _read_embargos(csv_bytes, [*CSV_COLUMN_MAP, GEOM_COLUMN_CSV])
    edicao = _edicao(df)
    df = _normalize(df, uf=uf, bbox=None, edicao=edicao)
    df = df[df[GEOM_COLUMN_CSV].notna()].reset_index(drop=True)

    wkts = df[GEOM_COLUMN_CSV].to_numpy(dtype=object, copy=True)
    wkts[~df[GEOM_COLUMN_CSV].map(wkt_within_limits).to_numpy(dtype=bool)] = None
    geoms = shapely.from_wkt(wkts, on_invalid="ignore")
    mask = ~shapely.is_missing(geoms)
    invalid = int((~mask).sum())
    if invalid:
        logger.warning("ibama_embargos_geo_wkt_invalido", descartados=invalid)
    if bbox is not None:
        mask &= shapely.intersects(geoms, shapely.box(*bbox))

    gdf = gpd.GeoDataFrame(
        df.loc[mask, COLUNAS_SAIDA].reset_index(drop=True),
        geometry=geoms[mask],
        crs="EPSG:4326",
    )
    gdf.attrs.update(df.attrs)
    gdf.attrs[EDICAO_COLUMN_CSV] = edicao
    logger.info("ibama_embargos_geo_parse_ok", records=len(gdf), edicao=edicao)
    return gdf
