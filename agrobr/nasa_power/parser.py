"""Parser para dados NASA POWER -- converte JSON da API em DataFrames."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.nasa_power.models import COLUNAS_MAP, SENTINEL

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_daily(
    data: dict[str, Any],
    lat: float,
    lon: float,
    uf: str = "",
) -> pd.DataFrame:
    """Converte resposta JSON NASA POWER em DataFrame diario.

    Args:
        data: Dict completo da API NASA POWER.
        lat: Latitude do ponto consultado.
        lon: Longitude do ponto consultado.
        uf: Sigla da UF (opcional, para enriquecer o DataFrame).

    Returns:
        DataFrame com colunas: data, lat, lon, uf, temp_media, temp_max,
        temp_min, precip_mm, umidade_rel, radiacao_mj, vento_ms.

    Raises:
        ParseError: Se dados estiverem vazios ou malformados.
    """
    if not data:
        raise ParseError(
            source="nasa_power",
            parser_version=PARSER_VERSION,
            reason="Resposta NASA POWER vazia",
        )

    parameters = data.get("properties", {}).get("parameter", {})

    if not parameters:
        raise ParseError(
            source="nasa_power",
            parser_version=PARSER_VERSION,
            reason="Nenhum parametro encontrado em properties.parameter",
        )

    # Pivot: cada parametro tem {YYYYMMDD: valor}, transformar em linhas por data.
    rows: dict[str, dict[str, Any]] = {}

    for nasa_param, daily_values in parameters.items():
        col_name = COLUNAS_MAP.get(nasa_param)
        if col_name is None:
            continue

        for date_str, value in daily_values.items():
            if date_str not in rows:
                rows[date_str] = {}
            # Tratar sentinel.
            if isinstance(value, (int, float)) and value == SENTINEL:
                rows[date_str][col_name] = None
            else:
                rows[date_str][col_name] = value

    if not rows:
        raise ParseError(
            source="nasa_power",
            parser_version=PARSER_VERSION,
            reason="Nenhuma data encontrada nos dados",
        )

    # Montar DataFrame.
    records: list[dict[str, Any]] = []
    for date_str, values in sorted(rows.items()):
        try:
            dt = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except (ValueError, IndexError):
            continue
        record = {"data": dt, "lat": lat, "lon": lon, "uf": uf}
        record.update(values)
        records.append(record)

    df = pd.DataFrame(records)

    # Converter colunas numericas.
    for col in COLUNAS_MAP.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["data"] = pd.to_datetime(df["data"])
    df = df.sort_values("data").reset_index(drop=True)

    logger.debug(
        "nasa_power_parse_ok",
        records=len(df),
        params=list(COLUNAS_MAP.values()),
    )

    return df


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados diarios em resumo mensal.

    Precipitacao: soma mensal.
    Temperatura, umidade, radiacao, vento: media mensal.

    Args:
        df: DataFrame diario (output de parse_daily).

    Returns:
        DataFrame mensal com colunas: mes, lat, lon, uf, precip_acum_mm,
        temp_media, temp_max_media, temp_min_media, umidade_media,
        radiacao_media_mj, vento_medio_ms.
    """
    if df.empty:
        return df

    df = df.copy()
    df["mes"] = df["data"].dt.to_period("M")

    agg: dict[str, pd.NamedAgg] = {}

    if "precip_mm" in df.columns:
        agg["precip_acum_mm"] = pd.NamedAgg(column="precip_mm", aggfunc="sum")
    if "temp_media" in df.columns:
        agg["temp_media"] = pd.NamedAgg(column="temp_media", aggfunc="mean")
    if "temp_max" in df.columns:
        agg["temp_max_media"] = pd.NamedAgg(column="temp_max", aggfunc="mean")
    if "temp_min" in df.columns:
        agg["temp_min_media"] = pd.NamedAgg(column="temp_min", aggfunc="mean")
    if "umidade_rel" in df.columns:
        agg["umidade_media"] = pd.NamedAgg(column="umidade_rel", aggfunc="mean")
    if "radiacao_mj" in df.columns:
        agg["radiacao_media_mj"] = pd.NamedAgg(column="radiacao_mj", aggfunc="mean")
    if "vento_ms" in df.columns:
        agg["vento_medio_ms"] = pd.NamedAgg(column="vento_ms", aggfunc="mean")

    if not agg:
        return df

    group_cols = ["uf"] if "uf" in df.columns and df["uf"].ne("").any() else []

    result = df.groupby(["mes"] + group_cols).agg(**agg).reset_index()
    result["mes"] = result["mes"].dt.to_timestamp()

    # Preservar lat/lon do primeiro registro de cada grupo.
    if "lat" in df.columns and "lon" in df.columns:
        coords = df.groupby(["mes"] + group_cols)[["lat", "lon"]].first().reset_index()
        coords["mes"] = coords["mes"].dt.to_timestamp()
        result = result.merge(coords, on=["mes"] + group_cols, how="left")

    return result
