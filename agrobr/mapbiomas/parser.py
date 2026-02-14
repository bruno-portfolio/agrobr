from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import (
    COLUNAS_SAIDA_COBERTURA,
    COLUNAS_SAIDA_TRANSICAO,
    SHEET_COBERTURA,
    SHEET_TRANSICAO,
    classe_para_nome,
    estado_para_uf,
)

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_cobertura_xlsx(data: bytes) -> pd.DataFrame:
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=SHEET_COBERTURA, engine="openpyxl")
    except Exception as e:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler XLSX cobertura: {e}",
        ) from e

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

    year_cols = [c for c in df.columns if isinstance(c, int)]
    if not year_cols:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason="Nenhuma coluna de ano encontrada",
        )

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
    melted["classe"] = melted["classe_id"].apply(
        lambda x: classe_para_nome(int(x)) if pd.notna(x) else ""
    )
    melted["nivel_0"] = melted["class_level_0"].fillna("")
    melted["ano"] = pd.to_numeric(melted["ano"], errors="coerce").astype("Int64")
    melted["area_ha"] = pd.to_numeric(melted["area_ha"], errors="coerce")

    output_cols = [c for c in COLUNAS_SAIDA_COBERTURA if c in melted.columns]
    result = melted[output_cols].copy()
    result = result.dropna(subset=["area_ha"]).reset_index(drop=True)

    logger.info("mapbiomas_cobertura_parse_ok", records=len(result))
    return result


def parse_transicao_xlsx(data: bytes) -> pd.DataFrame:
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=SHEET_TRANSICAO, engine="openpyxl")
    except Exception as e:
        raise ParseError(
            source="mapbiomas",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler XLSX transicao: {e}",
        ) from e

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
    melted["classe_de"] = melted["classe_de_id"].apply(
        lambda x: classe_para_nome(int(x)) if pd.notna(x) else ""
    )
    melted["classe_para_id"] = pd.to_numeric(melted["class_to"], errors="coerce").astype("Int64")
    melted["classe_para"] = melted["classe_para_id"].apply(
        lambda x: classe_para_nome(int(x)) if pd.notna(x) else ""
    )
    melted["periodo"] = melted["periodo_raw"].astype(str).str.lstrip("p").str.replace("_", "-")
    melted["area_ha"] = pd.to_numeric(melted["area_ha"], errors="coerce")

    output_cols = [c for c in COLUNAS_SAIDA_TRANSICAO if c in melted.columns]
    result = melted[output_cols].copy()
    result = result.dropna(subset=["area_ha"]).reset_index(drop=True)

    logger.info("mapbiomas_transicao_parse_ok", records=len(result))
    return result
