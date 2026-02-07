"""Parser para dados INMET — converte JSON da API em DataFrames."""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 1

COLUNAS_HORARIAS = {
    "DT_MEDICAO": "data",
    "HR_MEDICAO": "hora_utc",
    "CD_ESTACAO": "estacao",
    "UF": "uf",
    "TEM_INS": "temperatura",
    "TEM_MAX": "temperatura_max",
    "TEM_MIN": "temperatura_min",
    "UMD_INS": "umidade",
    "UMD_MAX": "umidade_max",
    "UMD_MIN": "umidade_min",
    "CHUVA": "precipitacao_mm",
    "PRE_INS": "pressao_hpa",
    "VEN_VEL": "vento_ms",
    "VEN_DIR": "vento_dir",
    "VEN_RAJ": "vento_rajada_ms",
    "RAD_GLO": "radiacao_kj_m2",
    "PTO_INS": "ponto_orvalho",
}

COLUNAS_NUMERICAS = [
    "temperatura",
    "temperatura_max",
    "temperatura_min",
    "umidade",
    "umidade_max",
    "umidade_min",
    "precipitacao_mm",
    "pressao_hpa",
    "vento_ms",
    "vento_dir",
    "vento_rajada_ms",
    "radiacao_kj_m2",
    "ponto_orvalho",
]

SENTINEL = -9999.0


def parse_observacoes(dados: list[dict[str, Any]]) -> pd.DataFrame:
    """Converte lista de observações INMET em DataFrame normalizado.

    Args:
        dados: Lista de dicts da API INMET.

    Returns:
        DataFrame com colunas renomeadas, tipos corretos e sentinelas tratados.

    Raises:
        ParseError: Se dados estiverem vazios ou malformados.
    """
    if not dados:
        raise ParseError(
            source="inmet",
            parser_version=PARSER_VERSION,
            reason="Resposta INMET vazia (nenhuma observação)",
        )

    df = pd.DataFrame(dados)

    colunas_presentes = {k: v for k, v in COLUNAS_HORARIAS.items() if k in df.columns}

    if not colunas_presentes:
        raise ParseError(
            source="inmet",
            parser_version=PARSER_VERSION,
            reason=f"Nenhuma coluna esperada encontrada. Colunas recebidas: {df.columns.tolist()}",
        )

    df = df.rename(columns=colunas_presentes)

    for col in COLUNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] == SENTINEL, col] = pd.NA

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    df = df.dropna(subset=["data"])
    df = df.sort_values(["estacao", "data", "hora_utc"]).reset_index(drop=True)

    logger.debug(
        "inmet_parse_ok",
        records=len(df),
        estacoes=df["estacao"].nunique() if "estacao" in df.columns else 0,
    )

    return df


def agregar_diario(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega observações horárias em resumo diário por estação.

    Args:
        df: DataFrame com observações horárias (output de parse_observacoes).

    Returns:
        DataFrame com colunas: data, estacao, uf, temp_media, temp_max, temp_min,
        precipitacao_mm, umidade_media, radiacao_total_kj_m2.
    """
    if df.empty:
        return df

    agg_dict: dict[str, tuple[str, str]] = {
        "temp_media": ("temperatura", "mean"),
        "temp_max": ("temperatura_max", "max"),
        "temp_min": ("temperatura_min", "min"),
        "precipitacao_mm": ("precipitacao_mm", "sum"),
        "umidade_media": ("umidade", "mean"),
        "radiacao_total_kj_m2": ("radiacao_kj_m2", "sum"),
    }

    agg_filtrado = {k: v for k, v in agg_dict.items() if v[0] in df.columns}

    if not agg_filtrado:
        return df

    group_cols = ["estacao"]
    if "uf" in df.columns:
        group_cols.append("uf")

    result = (
        df.groupby([pd.Grouper(key="data", freq="D")] + group_cols)  # type: ignore[operator]
        .agg(**{k: pd.NamedAgg(column=v[0], aggfunc=v[1]) for k, v in agg_filtrado.items()})
        .reset_index()
    )

    return result


def agregar_mensal_uf(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega observações diárias em resumo mensal por UF.

    Args:
        df: DataFrame diário (output de agregar_diario).

    Returns:
        DataFrame com colunas: mes, uf, precip_acum_mm, temp_media,
        temp_max_media, temp_min_media, num_estacoes.
    """
    if df.empty or "uf" not in df.columns:
        return df

    df = df.copy()
    df["mes"] = df["data"].dt.to_period("M")

    agg: dict[str, pd.NamedAgg] = {}

    if "precipitacao_mm" in df.columns:
        agg["precip_acum_mm"] = pd.NamedAgg(column="precipitacao_mm", aggfunc="sum")
    if "temp_media" in df.columns:
        agg["temp_media"] = pd.NamedAgg(column="temp_media", aggfunc="mean")
    if "temp_max" in df.columns:
        agg["temp_max_media"] = pd.NamedAgg(column="temp_max", aggfunc="mean")
    if "temp_min" in df.columns:
        agg["temp_min_media"] = pd.NamedAgg(column="temp_min", aggfunc="mean")
    if "estacao" in df.columns:
        agg["num_estacoes"] = pd.NamedAgg(column="estacao", aggfunc="nunique")

    if not agg:
        return df

    result = df.groupby(["mes", "uf"]).agg(**agg).reset_index()
    result["mes"] = result["mes"].dt.to_timestamp()

    return result
