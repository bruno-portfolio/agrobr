from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr.exceptions import ParseError
from agrobr.utils.geo import parse_arcgis_geojson, parse_arcgis_tabular

from .models import LAYERS

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
