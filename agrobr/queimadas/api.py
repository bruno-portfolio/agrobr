from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import BIOMAS_VALIDOS

logger = structlog.get_logger()


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca focos de calor detectados por satelite (INPE/BDQueimadas).

    Dados diarios ou mensais de focos de calor no Brasil, com coordenadas,
    municipio, bioma, satelite e potencia radiativa do fogo (FRP).

    Args:
        ano: Ano (ex: 2024).
        mes: Mes (1-12).
        dia: Dia especifico (1-31). Se None, busca mes completo.
        uf: Filtrar por UF (ex: "MT", "SP"). Opcional.
        bioma: Filtrar por bioma (ex: "Amazônia", "Cerrado"). Opcional.
        satelite: Filtrar por satelite (ex: "AQUA_M-T", "NOAA-20"). Opcional.
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: data, hora_gmt, lat, lon, satelite, municipio,
        municipio_id, estado, bioma, numero_dias_sem_chuva, precipitacao,
        risco_fogo, frp, uf.

    Raises:
        SourceUnavailableError: Se dados INPE indisponiveis.
        ParseError: Se nao conseguir parsear o CSV.

    Example:
        >>> df = await queimadas.focos(ano=2024, mes=9, uf="MT")
        >>> df.columns.tolist()
        ['data', 'hora_gmt', 'lat', 'lon', 'satelite', ...]
    """
    logger.info(
        "queimadas_focos",
        ano=ano,
        mes=mes,
        dia=dia,
        uf=uf,
        bioma=bioma,
        satelite=satelite,
    )

    t0 = time.monotonic()

    if dia is not None:
        data_str = f"{ano:04d}{mes:02d}{dia:02d}"
        csv_bytes, source_url = await client.fetch_focos_diario(data_str)
    else:
        csv_bytes, source_url = await client.fetch_focos_mensal(ano, mes)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_focos_csv(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if dia is None and "anual" in source_url:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df = df[df["data"].dt.month == mes].reset_index(drop=True)
        df["data"] = df["data"].dt.date

    if uf is not None:
        uf_upper = uf.strip().upper()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    if bioma is not None:
        bioma_set = {b for b in BIOMAS_VALIDOS if bioma.lower() in b.lower()}
        if bioma_set:
            df = df[df["bioma"].isin(bioma_set)].reset_index(drop=True)

    if satelite is not None:
        df = df[df["satelite"] == satelite].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="queimadas",
            source_url=source_url,
            source_method="httpx+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["queimadas"],
            selected_source="queimadas",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
