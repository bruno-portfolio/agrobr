from __future__ import annotations

from typing import Any

import pandas as pd
from lxml import etree
from pydantic import ValidationError

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.utils.geo import check_geopandas, check_pyogrio

from .models import (
    COLUNAS_SAIDA,
    COLUNAS_SAIDA_GEO,
    GEOM_COLUMN,
    NS_MS,
    NS_WFS,
    FeatureCount,
    UnidadeConservacao,
)

logger = _log.get_logger(__name__)

PARSER_VERSION = 1

DTYPES = dict.fromkeys(COLUNAS_SAIDA, pd.Series([""]).dtype) | {
    "area_ha": "float64",
    "data_criacao": "datetime64[ns]",
}

_COLECAO = f"{{{NS_WFS}}}FeatureCollection"
_MEMBRO = f"{{{NS_WFS}}}member"
_FEICAO = f"{{{NS_MS}}}ucs_selected"


def _erro(reason: str) -> ParseError:
    return ParseError(source="cnuc", parser_version=PARSER_VERSION, reason=reason)


def _colecao(data: bytes, *, huge_tree: bool = False) -> Any:
    try:
        xml_parser = etree.XMLParser(
            resolve_entities=False, load_dtd=False, no_network=True, huge_tree=huge_tree
        )
        root = etree.fromstring(data, parser=xml_parser)
    except etree.XMLSyntaxError as exc:
        raise _erro(f"GML inválido: {exc}") from exc
    if root.getroottree().docinfo.doctype:
        raise _erro("DOCTYPE não permitido")
    if root.tag != _COLECAO:
        raise _erro(f"Resposta WFS 2.0 FeatureCollection esperada, recebida {root.tag!r}")
    return root


def parse_feature_count(data: bytes) -> int:
    root = _colecao(data)
    try:
        return FeatureCount.model_validate(
            {"number_matched": root.get("numberMatched")}
        ).number_matched
    except ValidationError as exc:
        raise _erro(f"Contagem WFS inválida: {exc}") from exc


def empty_frame() -> pd.DataFrame:
    return pd.DataFrame({name: pd.Series(dtype=DTYPES[name]) for name in COLUNAS_SAIDA})


def parse_ucs(data: bytes, *, huge_tree: bool = False) -> pd.DataFrame:
    root = _colecao(data, huge_tree=huge_tree)
    membros = root.findall(_MEMBRO)
    anunciadas = root.get("numberReturned")
    if anunciadas is None or not anunciadas.isdecimal() or int(anunciadas) != len(membros):
        raise _erro(f"numberReturned={anunciadas!r} diverge de {len(membros)} feições recebidas")
    linhas = []
    for posicao, membro in enumerate(membros):
        feicoes = list(membro)
        if len(feicoes) != 1 or feicoes[0].tag != _FEICAO:
            raise _erro(f"Feição {posicao} fora da camada ms:ucs_selected")
        campos = {
            etree.QName(campo).localname: campo.text
            for campo in feicoes[0]
            if etree.QName(campo).namespace == NS_MS and etree.QName(campo).localname != GEOM_COLUMN
        }
        try:
            linhas.append(UnidadeConservacao.model_validate(campos).saida())
        except ValidationError as exc:
            raise _erro(f"UC inválida na posição {posicao}: {exc}") from exc
    if not linhas:
        return empty_frame()
    frame = pd.DataFrame(linhas, columns=COLUNAS_SAIDA).astype(DTYPES)
    logger.info("cnuc_ucs_parse_ok", records=len(frame))
    return frame


def parse_ucs_geo(data: bytes) -> Any:
    gpd = check_geopandas()
    pyogrio = check_pyogrio()
    frame = parse_ucs(data, huge_tree=True)
    if frame.empty:
        return gpd.GeoDataFrame(
            frame.assign(geometry=gpd.GeoSeries([], crs="EPSG:4326")),
            geometry="geometry",
            crs="EPSG:4326",
        )
    try:
        geometrias = pyogrio.read_dataframe(data, columns=["cd_cnuc"], DOWNLOAD_SCHEMA="NO")
    except (
        pyogrio.errors.DataSourceError,
        pyogrio.errors.DataLayerError,
        pyogrio.errors.FeatureError,
        pyogrio.errors.FieldError,
        pyogrio.errors.GeometryError,
        pyogrio.errors.CRSError,
    ) as exc:
        raise _erro(f"Geometria GML ilegível: {exc}") from exc
    if geometrias.crs is None or geometrias.crs.to_epsg() != 4326:
        raise _erro(f"CRS {geometrias.crs} diferente de EPSG:4326")
    if geometrias["cd_cnuc"].tolist() != frame["codigo"].tolist():
        raise _erro("Ordem ou códigos da geometria divergem dos atributos")
    if geometrias.geometry.isna().any():
        raise _erro("UC sem geometria no GML")
    gdf = gpd.GeoDataFrame(
        frame.assign(geometry=geometrias.geometry.values), geometry="geometry", crs="EPSG:4326"
    )
    return gdf[COLUNAS_SAIDA_GEO]
