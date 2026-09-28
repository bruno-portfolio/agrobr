from __future__ import annotations

import re

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.utils.io import read_excel_safe

from .models import (
    ANO_INICIO,
    ANOS_FINAIS,
    COLECAO_ATUAL,
    COLUNAS_SAIDA_COBERTURA,
    COLUNAS_SAIDA_TRANSICAO,
    classe_para_nome,
    estado_para_uf,
)

logger = structlog.get_logger()

PARSER_VERSION = 1


def _validar_ids_de_classe(df: pd.DataFrame, coluna: str) -> None:
    ids = pd.to_numeric(df[coluna], errors="coerce")
    invalidos = ids.isna() | ids.mod(1).ne(0)
    if invalidos.any():
        posicao = int(invalidos.to_numpy().argmax())
        publicado = df[coluna].astype(object).iloc[posicao]
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"{coluna} sem código inteiro na linha {posicao + 2}: {publicado!r}",
        )


def parse_cobertura_xlsx(data: bytes, colecao: int = COLECAO_ATUAL) -> pd.DataFrame:
    df = read_excel_safe(
        data,
        source="mapbiomas",
        parser_version=PARSER_VERSION,
        label="XLSX cobertura",
        sheet_name=f"COVERAGE_{colecao}",
        engine="openpyxl",
    )

    if df.empty:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Sheet COVERAGE vazia",
        )

    required = {"biome", "state", "class", "class_level_0"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    year_names = {
        column: int(str(column).removeprefix("y"))
        for column in df.columns
        if re.fullmatch(r"y?\d{4}", str(column))
    }
    df = df.rename(columns=year_names)
    year_cols = list(year_names.values())
    if not year_cols:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Nenhuma coluna de ano encontrada",
        )
    if len(year_cols) != len(set(year_cols)):
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Colunas de ano duplicadas após normalização",
        )
    if colecao not in ANOS_FINAIS or any(
        not ANO_INICIO <= int(year) <= ANOS_FINAIS[colecao] for year in year_cols
    ):
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"Anos incompatíveis com a coleção {colecao}",
        )

    _validar_ids_de_classe(df, "class")
    id_vars = ["biome", "state", "class", "class_level_0"]

    melted = df.melt(
        id_vars=id_vars,
        value_vars=year_cols,
        var_name="ano",
        value_name="area_ha",
    )

    melted["bioma"] = melted["biome"]
    melted["estado"] = melted["state"].apply(estado_para_uf)
    melted["classe_id"] = pd.to_numeric(melted["class"], errors="coerce").astype("Int64")
    melted["classe"] = melted["classe_id"].apply(lambda x: classe_para_nome(int(x), colecao))
    melted["nivel_0"] = melted["class_level_0"].fillna("")
    melted["ano"] = pd.to_numeric(melted["ano"], errors="coerce").astype("Int64")
    melted["area_ha"] = pd.to_numeric(melted["area_ha"], errors="coerce")

    output_cols = [c for c in COLUNAS_SAIDA_COBERTURA if c in melted.columns]
    result = melted[output_cols].copy()
    result = result.dropna(subset=["area_ha"]).reset_index(drop=True)

    logger.info("mapbiomas_cobertura_parse_ok", records=len(result))
    return result


def parse_transicao_xlsx(data: bytes, colecao: int = COLECAO_ATUAL) -> pd.DataFrame:
    df = read_excel_safe(
        data,
        source="mapbiomas",
        parser_version=PARSER_VERSION,
        label="XLSX transicao",
        sheet_name=f"TRANSITION_{colecao}",
        engine="openpyxl",
    )

    if df.empty:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Sheet TRANSITION vazia",
        )

    required = {"biome", "state", "class_from", "class_to"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    period_cols = [c for c in df.columns if str(c).startswith("p")]
    if not period_cols:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Nenhuma coluna de periodo encontrada",
        )

    _validar_ids_de_classe(df, "class_from")
    _validar_ids_de_classe(df, "class_to")
    id_vars = ["biome", "state", "class_from", "class_to"]
    melted = df.melt(
        id_vars=id_vars,
        value_vars=period_cols,
        var_name="periodo_raw",
        value_name="area_ha",
    )

    melted["bioma"] = melted["biome"]
    melted["estado"] = melted["state"].apply(estado_para_uf)
    melted["classe_de_id"] = pd.to_numeric(melted["class_from"], errors="coerce").astype("Int64")
    melted["classe_de"] = melted["classe_de_id"].apply(lambda x: classe_para_nome(int(x), colecao))
    melted["classe_para_id"] = pd.to_numeric(melted["class_to"], errors="coerce").astype("Int64")
    melted["classe_para"] = melted["classe_para_id"].apply(
        lambda x: classe_para_nome(int(x), colecao)
    )
    melted["periodo"] = melted["periodo_raw"].astype(str).str.lstrip("p").str.replace("_", "-")
    melted["area_ha"] = pd.to_numeric(melted["area_ha"], errors="coerce")

    output_cols = [c for c in COLUNAS_SAIDA_TRANSICAO if c in melted.columns]
    result = melted[output_cols].copy()
    result = result.dropna(subset=["area_ha"]).reset_index(drop=True)

    logger.info("mapbiomas_transicao_parse_ok", records=len(result))
    return result
