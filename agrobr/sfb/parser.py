from __future__ import annotations

import re
from typing import Any

import pandas as pd
import structlog

from agrobr.utils.geo import parse_arcgis_geojson, parse_arcgis_tabular

from .models import LAYERS

logger = structlog.get_logger()

PARSER_VERSION = 2

_NUMERIC_COLS = frozenset({"area_ha", "codigo_lote"})
_ANO = re.compile(r"(?<!\d)\d{4}(?!\d)")


def _com_ano_criacao(df: pd.DataFrame) -> pd.DataFrame:
    if "ano_criacao" not in df.columns:
        return df
    anos = [
        set(_ANO.findall(str(valor))) if pd.notna(valor) else set() for valor in df["ano_criacao"]
    ]
    ambiguos = [
        valor for valor, achados in zip(df["ano_criacao"], anos, strict=True) if len(achados) > 1
    ]
    if ambiguos:
        logger.warning("sfb_ano_criacao_ambiguo", linhas=len(ambiguos), exemplos=ambiguos[:3])
    df["ano_criacao"] = pd.array(
        [int(next(iter(achados))) if len(achados) == 1 else None for achados in anos], dtype="Int64"
    )
    return df


def parse_layer_tabular(pages: list[bytes], *, layer_key: str) -> pd.DataFrame:
    return _com_ano_criacao(
        parse_arcgis_tabular(
            pages,
            source="sfb",
            layer_config=LAYERS[layer_key],
            parser_version=PARSER_VERSION,
            numeric_cols=_NUMERIC_COLS,
        )
    )


def parse_layer_geojson(pages: list[bytes], *, layer_key: str) -> Any:
    return _com_ano_criacao(
        parse_arcgis_geojson(
            pages,
            source="sfb",
            layer_config=LAYERS[layer_key],
            parser_version=PARSER_VERSION,
            numeric_cols=_NUMERIC_COLS,
        )
    )
