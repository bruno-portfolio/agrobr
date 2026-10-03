from __future__ import annotations

import json
import re
import warnings
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.utils.geo import parse_arcgis_geojson, parse_arcgis_tabular
from agrobr.utils.result import ATRIBUTO_AVISOS

from . import models
from .models import LAYERS

logger = _log.get_logger(__name__)

PARSER_VERSION = 4

_NUMERIC_COLS = frozenset({"area_ha", "codigo_lote"})
_ANO = re.compile(r"(?<!\d)\d{4}(?!\d)")


def _com_ano_criacao(df: pd.DataFrame, *, texto: bool) -> pd.DataFrame:
    if "ano_criacao" not in df.columns:
        return df
    if texto:
        publicado = df["ano_criacao"].astype(object).where(df["ano_criacao"].notna(), None)
        if "ano_criacao_texto" in df.columns:
            df["ano_criacao_texto"] = publicado
        else:
            df.insert(list(df.columns).index("ano_criacao") + 1, "ano_criacao_texto", publicado)
    anos = [
        set(_ANO.findall(str(valor))) if pd.notna(valor) else set() for valor in df["ano_criacao"]
    ]
    ambiguos = [
        valor for valor, achados in zip(df["ano_criacao"], anos, strict=True) if len(achados) > 1
    ]
    if ambiguos:
        logger.warning("sfb_ano_criacao_ambiguo", linhas=len(ambiguos), exemplos=ambiguos[:3])
        aviso = (
            f"SFB: ano_criacao ambíguo em {len(ambiguos)} registros; o ano permanece nulo. "
            f"Exemplos do texto publicado: {ambiguos[:3]!r}"
        )
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=3)
    df["ano_criacao"] = pd.array(
        [int(next(iter(achados))) if len(achados) == 1 else None for achados in anos], dtype="Int64"
    )
    return df


def _normalizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    text_dtype = pd.Series([""]).dtype
    try:
        for coluna in df.columns:
            if coluna == "geometry":
                continue
            dtype = (
                "Int64"
                if coluna in {"fid", "id", "codigo_lote", "ano_criacao"}
                else "float64"
                if coluna == "area_ha"
                else text_dtype
            )
            df[coluna] = pd.Series(df[coluna].array, index=df.index, dtype=dtype)
    except (TypeError, ValueError) as exc:
        raise ParseError(source="sfb", parser_version=PARSER_VERSION, reason=str(exc)) from exc
    return df


def _mantem_texto(layer_key: str) -> bool:
    return "ano_criacao_texto" in LAYERS[layer_key]["colunas_saida"]


def parse_layer_tabular(pages: list[bytes], *, layer_key: str) -> pd.DataFrame:
    return _normalizar_tipos(
        _com_ano_criacao(
            parse_arcgis_tabular(
                pages,
                source="sfb",
                layer_config=LAYERS[layer_key],
                parser_version=PARSER_VERSION,
                numeric_cols=_NUMERIC_COLS,
            ),
            texto=_mantem_texto(layer_key),
        )
    )


def parse_layer_geojson(pages: list[bytes], *, layer_key: str) -> Any:
    return _normalizar_tipos(
        _com_ano_criacao(
            parse_arcgis_geojson(
                pages,
                source="sfb",
                layer_config=LAYERS[layer_key],
                parser_version=PARSER_VERSION,
                numeric_cols=_NUMERIC_COLS,
            ),
            texto=_mantem_texto(layer_key),
        )
    )


def ifn_atributos(page: bytes, *, lotes: bool = False) -> list[dict[str, Any]]:
    try:
        document = json.loads(page)
        if not isinstance(document, dict) or not isinstance(document.get("features"), list):
            raise ValueError("esperado objeto com lista de features")
        model = models.IfnLote if lotes else models.IfnPonto
        rows = []
        for feature in document["features"]:
            if not isinstance(feature, dict):
                raise ValueError("feição sem atributos")
            attributes = feature.get("properties", feature.get("attributes"))
            rows.append(model.model_validate(attributes).model_dump())
        return rows
    except (ValueError, TypeError, UnicodeDecodeError, ValidationError) as exc:
        raise ParseError(source="sfb", parser_version=PARSER_VERSION, reason=str(exc)) from exc


def parse_ifn(pontos: list[bytes], lotes: list[bytes], *, geometria: bool = False) -> Any:
    for page in pontos:
        ifn_atributos(page)
    parse = parse_layer_geojson if geometria else parse_layer_tabular
    df = parse(pontos, layer_key="ifn_conglomerados")
    cadastro: dict[int, str | None] = {}
    for page in lotes:
        for row in ifn_atributos(page, lotes=True):
            code = row["co_lote"]
            if code in cadastro:
                raise ParseError(
                    source="sfb",
                    parser_version=PARSER_VERSION,
                    reason=f"Código de lote duplicado no cadastro IFN: {code}",
                )
            cadastro[code] = row["no_lote"]
    codes = {int(code) for code in df["codigo_lote"].dropna()}
    if missing := codes - cadastro.keys():
        raise ParseError(
            source="sfb",
            parser_version=PARSER_VERSION,
            reason=f"Lotes IFN sem correspondente no cadastro: {sorted(missing)}",
        )
    if extra := cadastro.keys() - codes:
        raise ParseError(
            source="sfb",
            parser_version=PARSER_VERSION,
            reason=f"Lotes IFN não solicitados no cadastro: {sorted(extra)}",
        )
    if df["id"].duplicated().any():
        raise ParseError(
            source="sfb",
            parser_version=PARSER_VERSION,
            reason="IDs IFN duplicados",
        )
    df["lote"] = df["codigo_lote"].map(cadastro)
    null_codes = int(df["codigo_lote"].isna().sum())
    null_names = int((df["codigo_lote"].notna() & df["lote"].isna()).sum())
    if null_codes or null_names:
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(
            f"IFN: {null_codes} pontos com código de lote nulo e "
            f"{null_names} pontos com nome de lote nulo publicado no cadastro."
        )
    columns = models.LAYERS["ifn_conglomerados"]["colunas_saida"]
    return _normalizar_tipos(df[columns + (["geometry"] if geometria else [])])
