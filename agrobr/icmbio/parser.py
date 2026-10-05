from __future__ import annotations

from typing import Any

import pandas as pd
from lxml import etree
from pydantic import ValidationError

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.utils.geo import check_geopandas, parse_geojson_base
from agrobr.utils.io import read_csv_safe

from .models import (
    COLUNAS_SAIDA,
    COLUNAS_SAIDA_GEO,
    MAX_FEATURES_GEO,
    PROPERTY_NAMES,
    RENAME_MAP,
    FeatureCount,
    UnidadeConservacao,
)

logger = _log.get_logger(__name__)

PARSER_VERSION = 3

DTYPES = dict.fromkeys(COLUNAS_SAIDA, pd.Series([""]).dtype) | {
    "area_ha": "float64",
    "ano_criacao": "Int64",
}

_REQUIRED_COLS_RAW = {"cnuc", "nomeuc", "grupouc", "areahaalb"}


def parse_ucs_csv(data: bytes) -> pd.DataFrame:
    df = read_csv_safe(
        data,
        source="icmbio",
        parser_version=PARSER_VERSION,
        label="CSV ICMBio UCs",
        dtype="string[python]",
        keep_default_na=False,
        na_filter=False,
    )
    missing = set(PROPERTY_NAMES) - set(df.columns)
    if missing:
        raise ParseError(
            source="icmbio",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    try:
        rows = [
            UnidadeConservacao.model_validate(row).model_dump()
            for row in df.to_dict(orient="records")
        ]
    except ValidationError as exc:
        raise ParseError(
            source="icmbio", parser_version=PARSER_VERSION, reason=f"UC invalida: {exc}"
        ) from exc
    df = pd.DataFrame(rows, columns=PROPERTY_NAMES, dtype=object).rename(columns=RENAME_MAP)
    df = df[COLUNAS_SAIDA].astype(DTYPES)

    logger.info("icmbio_ucs_parse_ok", records=len(df))
    return df


def parse_feature_count(data: bytes) -> int:
    try:
        xml_parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
        root = etree.fromstring(data, parser=xml_parser)
        if root.getroottree().docinfo.doctype:
            raise ValueError("DOCTYPE nao permitido")
        if root.tag != "{http://www.opengis.net/wfs}FeatureCollection":
            raise ValueError("Resposta WFS FeatureCollection esperada")
        return FeatureCount.model_validate(
            {"number_of_features": root.get("numberOfFeatures")}
        ).number_of_features
    except (etree.XMLSyntaxError, ValueError) as exc:
        raise ParseError(
            source="icmbio",
            parser_version=PARSER_VERSION,
            reason=f"Contagem WFS invalida: {exc}",
        ) from exc


def parse_ucs_geojson(data: bytes) -> Any:
    gpd = check_geopandas()
    gdf = parse_geojson_base(
        data,
        gpd,
        source="icmbio",
        parser_version=PARSER_VERSION,
        required_cols=_REQUIRED_COLS_RAW,
        max_features=MAX_FEATURES_GEO,
        output_cols_empty=COLUNAS_SAIDA_GEO,
        truncation_event="icmbio_ucs_geo_truncated",
    )
    if gdf.empty:
        return gdf.astype({name: DTYPES[name] for name in gdf.columns if name in DTYPES})

    gdf = gdf.rename(columns=RENAME_MAP)
    for coluna in ("area_ha", "ano_criacao"):
        bruto = gdf[coluna].mask(gdf[coluna].eq(""))
        valores = pd.to_numeric(bruto, errors="coerce")
        invalidos = bruto.notna() & valores.isna()
        if invalidos.any():
            raise ParseError(
                source="icmbio",
                parser_version=PARSER_VERSION,
                reason=f"UC invalida: {coluna}={bruto[invalidos].iloc[0]!r} fora do formato numérico",
            )
        gdf[coluna] = valores
    try:
        gdf["ano_criacao"] = gdf["ano_criacao"].astype("Int64")
    except TypeError as exc:
        raise ParseError(
            source="icmbio",
            parser_version=PARSER_VERSION,
            reason="UC invalida: ano_criacao não inteiro",
        ) from exc
    gdf["grupo"] = gdf["grupo"].str.upper()

    output_cols = [c for c in COLUNAS_SAIDA_GEO if c in gdf.columns]
    return gdf[output_cols].reset_index(drop=True)
