from __future__ import annotations

import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pandas as pd

from agrobr import _log, constants
from agrobr.exceptions import ParseError
from agrobr.normalize import dates
from agrobr.normalize.regions import UFS_VALIDAS, ibge_para_uf
from agrobr.utils import io as io_utils
from agrobr.utils.geo import check_geopandas, check_pyogrio
from agrobr.utils.result import ATRIBUTO_AVISOS

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
_PARTES_DO_SHAPEFILE = frozenset({".shp", ".shx", ".dbf", ".prj", ".cpg"})
_CODIGO_DO_SHP = b"\x00\x00\x27\x0a"

BBox = tuple[float, float, float, float]


def _check_expansion(zip_path: Path) -> None:
    """O GDAL lê o membro até o fim do deflate, sem parar no tamanho declarado; o teto da expansão é o do download,
    porque o INCRA publica o ZIP sem compressão."""
    with zip_path.open("rb") as archive:
        io_utils.check_zip_expansion(
            archive,
            source="acervo_fundiario",
            limit=constants.ACERVO_MAX_DOWNLOAD_BYTES,
            label="shapefile",
        )


def _shapefile(zip_path: Path) -> str:
    """Caminho ``/vsizip/`` do único ``.shp`` do ZIP, depois do teto de expansão.

    O GDAL escolhe o driver pelo conteúdo, não pela extensão: outro formato no ZIP (um VRT, até com nome ``.shp``) lê
    arquivo local e faz pedido HTTP. Por isso só passam as partes do shapefile, sem ``..``, e o ``.shp`` precisa do
    código de arquivo do formato no cabeçalho.
    """
    _check_expansion(zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        membros = [info for info in archive.infolist() if not info.is_dir()]
        shps = [info for info in membros if PurePosixPath(info.filename).suffix.lower() == ".shp"]
        fora = sorted(
            info.filename
            for info in membros
            if PurePosixPath(info.filename).suffix.lower() not in _PARTES_DO_SHAPEFILE
            or "\\" in info.filename
            or PurePosixPath(info.filename).is_absolute()
            or ".." in PurePosixPath(info.filename).parts
        )
        if fora or len(shps) != 1:
            raise ParseError(
                source="acervo_fundiario",
                parser_version=PARSER_VERSION,
                reason=f"ZIP deve ter só as partes de 1 shapefile; .shp: {len(shps)}, fora: {fora}",
            )
        with archive.open(shps[0]) as shp:
            cabecalho = shp.read(len(_CODIGO_DO_SHP))
    if cabecalho != _CODIGO_DO_SHP:
        raise ParseError(
            source="acervo_fundiario",
            parser_version=PARSER_VERSION,
            reason=f"{shps[0].filename} não tem o cabeçalho de shapefile ({cabecalho!r})",
        )
    return f"/vsizip/{zip_path.as_posix()}/{shps[0].filename}"


def _read_tabular(zip_path: Path, *, bbox: BBox | None = None) -> pd.DataFrame:
    """Com ``bbox``, lê com a geometria e a descarta.

    No GDAL 3.8 (pyogrio < 0.10), o filtro espacial com ``read_geometry=False`` não casa nenhuma
    feição, e o recorte voltava vazio sem aviso.
    """
    if bbox is not None:
        geo = _read_geo(zip_path, bbox=bbox)
        return pd.DataFrame(geo.drop(columns=geo.geometry.name))
    caminho = _shapefile(zip_path)
    pyogrio = check_pyogrio()
    df = pyogrio.read_dataframe(caminho, encoding=DBF_ENCODING, read_geometry=False)
    return cast(pd.DataFrame, df)


def _read_geo(zip_path: Path, *, bbox: BBox | None = None) -> Any:
    caminho = _shapefile(zip_path)
    gpd = check_geopandas()
    return gpd.read_file(caminho, encoding=DBF_ENCODING, bbox=bbox)


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
    df["uf"] = df["uf"].astype(pd.Series([""]).dtype).str.strip().str.upper()
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


def _with_natureza(df: Any, natureza: str) -> Any:
    df = df.copy()
    df["natureza"] = pd.Series(natureza, index=df.index, dtype=pd.Series([""]).dtype)
    return df


def parse_sigef(zip_path: Path, *, natureza: str, bbox: BBox | None = None) -> pd.DataFrame:
    df = _read_tabular(zip_path, bbox=bbox)
    _validate_required(df, SIGEF_REQUIRED_COLS, "sigef")
    df = df.rename(columns=SIGEF_RENAME_MAP)
    df = _resolve_uf_from_ibge(df)
    df = _normalize_uf_column(df)
    df = _coerce_dates(df, SIGEF_DATE_COLS)
    df = _with_natureza(df, natureza)
    df = _select_output(df, SIGEF_COLUNAS_SAIDA)
    logger.info("acervo_fundiario_sigef_parse_ok", records=len(df), natureza=natureza)
    return df


def parse_sigef_geo(zip_path: Path, *, natureza: str, bbox: BBox | None = None) -> Any:
    gdf = _read_geo(zip_path, bbox=bbox)
    _validate_required(gdf, SIGEF_REQUIRED_COLS, "sigef")
    gdf = gdf.rename(columns=SIGEF_RENAME_MAP)
    gdf = _resolve_uf_from_ibge(gdf)
    gdf = _normalize_uf_column(gdf)
    gdf = _coerce_dates(gdf, SIGEF_DATE_COLS)
    gdf = _make_geometries_valid(gdf)
    gdf = _with_natureza(gdf, natureza)
    gdf = _select_output(gdf, SIGEF_COLUNAS_SAIDA_GEO)
    logger.info("acervo_fundiario_sigef_geo_parse_ok", records=len(gdf), natureza=natureza)
    return gdf


def join_sigef(partes: dict[str, Any]) -> Any:
    """Empilha os arquivos do SIGEF na ordem recebida (público, depois privado).

    Os avisos de cada arquivo seguem para o resultado, e parcela presente nos dois arquivos vira
    aviso, sem descartar linha: o INCRA regrava os arquivos em horários diferentes.
    """
    avisos = [
        f"sigef {natureza}: {aviso}"
        for natureza, parte in partes.items()
        for aviso in parte.attrs.get(ATRIBUTO_AVISOS, [])
    ]
    frames = list(partes.values())
    if len(frames) == 2:
        comuns = set(frames[0]["codigo_parcela"].dropna()) & set(
            frames[1]["codigo_parcela"].dropna()
        )
        if comuns:
            logger.warning("acervo_fundiario_sigef_parcela_nos_dois_arquivos", parcelas=len(comuns))
            avisos.append(
                f"acervo_fundiario: {len(comuns)} codigo_parcela aparece(m) nos arquivos público "
                "e privado do SIGEF; as linhas dos dois foram mantidas"
            )
    cheios = [frame for frame in frames if len(frame)] or frames[:1]
    df = cheios[0] if len(cheios) == 1 else pd.concat(cheios, ignore_index=True)
    df.attrs = {ATRIBUTO_AVISOS: avisos} if avisos else {}
    return df


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
