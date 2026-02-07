"""API pública do módulo INMET."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser

logger = structlog.get_logger()


async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
) -> pd.DataFrame:
    """Lista estações meteorológicas INMET.

    Args:
        tipo: "T" para automáticas, "M" para convencionais.
        uf: Filtrar por UF (ex: "MT").
        apenas_operantes: Se True, retorna apenas estações ativas.

    Returns:
        DataFrame com metadados das estações.
    """
    dados = await client.fetch_estacoes(tipo)

    if not dados:
        return pd.DataFrame()

    df = pd.DataFrame(dados)

    rename_map = {
        "CD_ESTACAO": "codigo",
        "DC_NOME": "nome",
        "SG_ESTADO": "uf",
        "CD_SITUACAO": "situacao",
        "TP_ESTACAO": "tipo",
        "VL_LATITUDE": "latitude",
        "VL_LONGITUDE": "longitude",
        "VL_ALTITUDE": "altitude",
        "DT_INICIO_OPERACAO": "inicio_operacao",
    }

    colunas_presentes = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=colunas_presentes)

    for col in ["latitude", "longitude", "altitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if apenas_operantes and "situacao" in df.columns:
        df = df[df["situacao"] == "Operante"]

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf.upper()]

    df = df.reset_index(drop=True)
    return df


async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca dados observacionais de uma estação INMET.

    Args:
        codigo: Código da estação (ex: "A001").
        inicio: Data inicial (str "YYYY-MM-DD" ou date).
        fim: Data final (str "YYYY-MM-DD" ou date).
        agregacao: "horario" (padrão) ou "diario".
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com observações meteorológicas.
    """
    if isinstance(inicio, str):
        inicio = date.fromisoformat(inicio)
    if isinstance(fim, str):
        fim = date.fromisoformat(fim)

    t0 = time.monotonic()
    dados = await client.fetch_dados_estacao(codigo, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_observacoes(dados)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if agregacao == "diario":
        df = parser.agregar_diario(df)

    if return_meta:
        meta = MetaInfo(
            source="inmet",
            source_url=f"{client.BASE_URL}/estacao/dados/{codigo}/{inicio}/{fim}",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["inmet"],
            selected_source="inmet",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


async def clima_uf(
    uf: str,
    ano: int,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Dados climáticos agregados mensalmente por UF.

    Busca dados de todas as estações automáticas operantes da UF,
    agrega por dia e depois por mês.

    Args:
        uf: Sigla da UF (ex: "MT", "SP").
        ano: Ano de referência.
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: mes, uf, precip_acum_mm, temp_media,
        temp_max_media, temp_min_media, num_estacoes.
    """
    inicio = date(ano, 1, 1)
    fim = date(ano, 12, 31)

    t0 = time.monotonic()
    dados = await client.fetch_dados_estacoes_uf(uf, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df_horario = parser.parse_observacoes(dados)
    df_diario = parser.agregar_diario(df_horario)
    df_mensal = parser.agregar_mensal_uf(df_diario)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="inmet",
            source_url=f"{client.BASE_URL}/estacoes/T",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df_mensal),
            columns=df_mensal.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["inmet"],
            selected_source="inmet",
            fetch_timestamp=datetime.now(UTC),
        )
        return df_mensal, meta

    return df_mensal
