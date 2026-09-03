from __future__ import annotations

import time
from datetime import date, datetime

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_year_uf

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
    VENDAS_DIESEL_CSV_URL,
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
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if agregacao not in AGREGACOES_VALIDAS:
        raise InvalidParameterError(
            f"Agregacao '{agregacao}' invalida. Opcoes: {sorted(AGREGACOES_VALIDAS)}"
        )
    if nivel not in NIVEIS_VALIDOS:
        raise InvalidParameterError(f"Nivel '{nivel}' invalido. Opcoes: {sorted(NIVEIS_VALIDOS)}")
    if produto.upper() not in {p.upper() for p in PRODUTOS_DIESEL}:
        raise InvalidParameterError(
            f"Produto '{produto}' invalido. Opcoes: {sorted(PRODUTOS_DIESEL)}"
        )
    validate_year_uf(uf=uf)

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

    meta = build_source_meta(
        "anp_diesel",
        _get_source_url(nivel),
        "httpx",
        fetch_parse_ms,
        0,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf)

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

    meta = build_source_meta(
        "anp_diesel",
        VENDAS_DIESEL_CSV_URL,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _periodos_municipios(inicio: date | None, fim: date | None) -> list[str]:
    if not inicio and not fim:
        return list(PRECOS_MUNICIPIOS_URLS.keys())

    ano_inicio = inicio.year if inicio else 2022
    ano_fim = fim.year if fim else datetime.now().year

    periodos: list[str] = []
    for ano in range(ano_inicio, ano_fim + 1):
        p = _resolve_periodo_municipio(ano)
        if p and p not in periodos:
            periodos.append(p)
    return periodos


async def _fetch_and_parse_municipios(
    produto: str | None,
    uf: str | None,
    municipio: str | None,
    inicio: date | None,
    fim: date | None,
) -> pd.DataFrame:
    periodos_necessarios = _periodos_municipios(inicio, fim)

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
