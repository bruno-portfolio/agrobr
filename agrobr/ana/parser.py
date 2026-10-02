from __future__ import annotations

import importlib
from typing import Any

import pandas as pd

from agrobr.exceptions import ParseError
from agrobr.normalize import dates, regions
from agrobr.utils.geo import check_geopandas, parse_arcgis_geojson, parse_arcgis_tabular
from agrobr.utils.result import ATRIBUTO_AVISOS

from .models import LAYERS, MASSAS_DAGUA

PARSER_VERSION = 2

_NUMERIC_COLS = frozenset(
    {
        "area_ha",
        "area_montante_km2",
        "disponibilidade_m3_s",
        "vazao_max_mensal",
        "vazao_mes_seco",
        "vazao_mes_irrigacao",
        "vazao_media_anual",
    }
)

_MASSAS_INTEIROS = ("FID", "codigo", "codigo_snisb", "codigo_trecho")
_MASSAS_MEDIDAS = frozenset({"volume_hm3", "area_km2", "area_ha", "perimetro_km"})


def _normalizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    text_dtype = pd.Series([""]).dtype
    try:
        for coluna in df.columns:
            if coluna == "geometry":
                continue
            dtype = (
                "Int64"
                if coluna in {"OBJECTID", "ID"}
                else "float64"
                if coluna in _NUMERIC_COLS
                else text_dtype
            )
            df[coluna] = pd.Series(df[coluna].array, index=df.index, dtype=dtype)
    except (TypeError, ValueError) as exc:
        raise ParseError(source="ana", parser_version=PARSER_VERSION, reason=str(exc)) from exc
    return df


def parse_layer_tabular(pages: list[bytes], *, layer_key: str) -> pd.DataFrame:
    return _normalizar_tipos(
        parse_arcgis_tabular(
            pages,
            source="ana",
            layer_config=LAYERS[layer_key],
            parser_version=PARSER_VERSION,
            numeric_cols=_NUMERIC_COLS,
            validate_required=True,
        )
    )


def parse_layer_geojson(pages: list[bytes], *, layer_key: str) -> Any:
    return _normalizar_tipos(
        parse_arcgis_geojson(
            pages,
            source="ana",
            layer_config=LAYERS[layer_key],
            parser_version=PARSER_VERSION,
            numeric_cols=_NUMERIC_COLS,
        )
    )


def _siglas(nomes: Any, desconhecidos: set[str]) -> str | None:
    if not isinstance(nomes, str) or not nomes.strip():
        return None
    siglas = set()
    for nome in nomes.split(","):
        sigla = regions.NOMES_PARA_UF.get(regions.remover_acentos(nome.strip().lower()))
        if sigla is None:
            desconhecidos.add(nome.strip())
            return None
        siglas.add(sigla)
    return "/".join(sorted(siglas))


def _normalizar_massas(df: Any) -> Any:
    text_dtype = pd.Series([""]).dtype
    df = df.copy()
    try:
        for coluna in df.columns:
            if coluna == "geometry":
                continue
            valores = df[coluna].astype(object)
            if coluna in _MASSAS_INTEIROS or coluna in _MASSAS_MEDIDAS:
                numeros = pd.to_numeric(valores, errors="raise")
                df[coluna] = numeros.astype("Int64" if coluna in _MASSAS_INTEIROS else "float64")
                continue
            vazios = valores.map(lambda valor: isinstance(valor, str) and not valor.strip())
            df[coluna] = pd.Series(
                valores.where(~vazios.astype(bool), None).array, index=df.index, dtype=text_dtype
            )
    except (TypeError, ValueError) as exc:
        raise ParseError(source="ana", parser_version=PARSER_VERSION, reason=str(exc)) from exc
    desconhecidos: set[str] = set()
    df["uf"] = pd.Series(
        [_siglas(valor, desconhecidos) for valor in df["uf"]], index=df.index, dtype=text_dtype
    )
    if desconhecidos:
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(
            f"ana: UF fora do cadastro em nmufe ({', '.join(sorted(desconhecidos))}); "
            "a coluna uf dessas feições ficou nula"
        )
    dates.converter_coluna(df, "data_construcao", fonte="ana", dayfirst=True)
    df = df.sort_values("FID", kind="stable").drop(columns="FID").reset_index(drop=True)
    return df


def parse_massas_dagua(pages: list[bytes], *, geo: bool) -> Any:
    if geo:
        check_geopandas()
        erro_shapely = importlib.import_module("shapely.errors").ShapelyError
        try:
            bruto = parse_arcgis_geojson(
                pages, source="ana", layer_config=MASSAS_DAGUA, parser_version=PARSER_VERSION
            )
        except (KeyError, TypeError, ValueError, erro_shapely) as exc:
            raise ParseError(
                source="ana",
                parser_version=PARSER_VERSION,
                reason=f"GeoJSON das massas d'água fora do layout: {exc!r}",
            ) from exc
    else:
        bruto = parse_arcgis_tabular(
            pages,
            source="ana",
            layer_config=MASSAS_DAGUA,
            parser_version=PARSER_VERSION,
            validate_required=True,
        )
    return _normalizar_massas(bruto)
