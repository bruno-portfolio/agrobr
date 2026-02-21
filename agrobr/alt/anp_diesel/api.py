from __future__ import annotations

import time
from datetime import UTC, date, datetime

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import (
    AGREGACAO_MENSAL,
    AGREGACAO_SEMANAL,
    AGREGACOES_VALIDAS,
    NIVEIS_VALIDOS,
    NIVEL_MUNICIPIO,
    NIVEL_UF,
    PRECOS_BRASIL_URL,
    PRECOS_ESTADOS_URL,
    PRECOS_MUNICIPIOS_URLS,
    PRODUTOS_DIESEL,
    UFS_VALIDAS,
    VENDAS_M3_URL,
    _resolve_periodo_municipio,
)

logger = structlog.get_logger()


async def precos_diesel(
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = AGREGACAO_SEMANAL,
    nivel: str = NIVEL_MUNICIPIO,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if agregacao not in AGREGACOES_VALIDAS:
        raise ValueError(f"Agregacao '{agregacao}' invalida. Opcoes: {sorted(AGREGACOES_VALIDAS)}")
    if nivel not in NIVEIS_VALIDOS:
        raise ValueError(f"Nivel '{nivel}' invalido. Opcoes: {sorted(NIVEIS_VALIDOS)}")
    if produto.upper() not in {p.upper() for p in PRODUTOS_DIESEL}:
        raise ValueError(f"Produto '{produto}' invalido. Opcoes: {sorted(PRODUTOS_DIESEL)}")
    if uf and uf.upper() not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    if isinstance(inicio, str):
        inicio = date.fromisoformat(inicio)
    if isinstance(fim, str):
        fim = date.fromisoformat(fim)

    t0 = time.monotonic()

    if nivel == NIVEL_MUNICIPIO:
        df = await _fetch_and_parse_municipios(produto, uf, municipio, inicio, fim)
    elif nivel == NIVEL_UF:
        content = await client.fetch_precos_estados()
        df = parser.parse_precos(content, produto=produto, uf=uf)
    else:
        content = await client.fetch_precos_brasil()
        df = parser.parse_precos(content, produto=produto)

    fetch_parse_ms = int((time.monotonic() - t0) * 1000)

    if inicio:
        df = df[df["data"] >= pd.Timestamp(inicio)].copy()
    if fim:
        df = df[df["data"] <= pd.Timestamp(fim)].copy()

    if agregacao == AGREGACAO_MENSAL:
        df = parser.agregar_mensal(df)

    df = df.reset_index(drop=True)

    if return_meta:
        source_url = _get_source_url(nivel)
        meta = MetaInfo(
            source="anp_diesel",
            source_url=source_url,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_parse_ms,
            parse_duration_ms=0,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["anp_diesel"],
            selected_source="anp_diesel",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if uf and uf.upper() not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    if isinstance(inicio, str):
        inicio = date.fromisoformat(inicio)
    if isinstance(fim, str):
        fim = date.fromisoformat(fim)

    t0 = time.monotonic()
    content = await client.fetch_vendas_m3()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_vendas(content, uf=uf)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if inicio:
        df = df[df["data"] >= pd.Timestamp(inicio)].copy()
    if fim:
        df = df[df["data"] <= pd.Timestamp(fim)].copy()

    df = df.reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="anp_diesel",
            source_url=VENDAS_M3_URL,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["anp_diesel"],
            selected_source="anp_diesel",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


async def _fetch_and_parse_municipios(
    produto: str | None,
    uf: str | None,
    municipio: str | None,
    inicio: date | None,
    fim: date | None,
) -> pd.DataFrame:
    periodos_necessarios: list[str] = []

    if inicio and fim:
        for ano in range(inicio.year, fim.year + 1):
            p = _resolve_periodo_municipio(ano)
            if p and p not in periodos_necessarios:
                periodos_necessarios.append(p)
    elif inicio:
        for ano in range(inicio.year, datetime.now().year + 1):
            p = _resolve_periodo_municipio(ano)
            if p and p not in periodos_necessarios:
                periodos_necessarios.append(p)
    elif fim:
        for ano in range(2022, fim.year + 1):
            p = _resolve_periodo_municipio(ano)
            if p and p not in periodos_necessarios:
                periodos_necessarios.append(p)
    else:
        periodos_necessarios = list(PRECOS_MUNICIPIOS_URLS.keys())

    dfs: list[pd.DataFrame] = []
    for periodo in periodos_necessarios:
        content = await client.fetch_precos_municipios(periodo)
        df = parser.parse_precos(content, produto=produto, uf=uf, municipio=municipio)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame(
            columns=[
                "data",
                "uf",
                "municipio",
                "produto",
                "preco_venda",
                "preco_compra",
                "margem",
                "n_postos",
            ]
        )

    return pd.concat(dfs, ignore_index=True).sort_values("data").reset_index(drop=True)


def _get_source_url(nivel: str) -> str:
    if nivel == NIVEL_MUNICIPIO:
        return list(PRECOS_MUNICIPIOS_URLS.values())[0]
    if nivel == NIVEL_UF:
        return PRECOS_ESTADOS_URL
    return PRECOS_BRASIL_URL
