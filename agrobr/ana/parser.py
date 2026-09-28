from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr.utils.geo import parse_arcgis_geojson, parse_arcgis_tabular

from .models import LAYERS

PARSER_VERSION = 1

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


def parse_layer_tabular(pages: list[bytes], *, layer_key: str) -> pd.DataFrame:
    return parse_arcgis_tabular(
        pages,
        source="ana",
        layer_config=LAYERS[layer_key],
        parser_version=PARSER_VERSION,
        numeric_cols=_NUMERIC_COLS,
        validate_required=True,
    )


def parse_layer_geojson(pages: list[bytes], *, layer_key: str) -> Any:
    return parse_arcgis_geojson(
        pages,
        source="ana",
        layer_config=LAYERS[layer_key],
        parser_version=PARSER_VERSION,
        numeric_cols=_NUMERIC_COLS,
    )
