from __future__ import annotations

import time
from datetime import date
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import validation
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser

logger = structlog.get_logger()


@overload
async def estacoes(
    tipo: str = ...,
    uf: str | None = ...,
    apenas_operantes: bool = ...,
    as_polars: bool = ...,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def estacoes(
    tipo: str = ...,
    uf: str | None = ...,
    apenas_operantes: bool = ...,
    as_polars: bool = ...,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    t0 = time.monotonic()
    dados = await client.fetch_estacoes(tipo)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()

    if not dados:
        df = pd.DataFrame()
    else:
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

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacoes/{tipo}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    try:
        if isinstance(inicio, str):
            inicio = date.fromisoformat(inicio)
        if isinstance(fim, str):
            fim = date.fromisoformat(fim)
    except ValueError as exc:
        raise InvalidParameterError("inicio e fim devem usar YYYY-MM-DD") from exc
    if inicio > fim:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")

    t0 = time.monotonic()
    dados = await client.fetch_dados_estacao(codigo, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_observacoes(dados)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if agregacao == "diario":
        df = parser.agregar_diario(df)

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacao/{inicio}/{fim}/{codigo}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def clima_uf(
    uf: str,
    ano: int,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validation.validate_uf(uf)
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

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacoes/T",
        "httpx",
        fetch_ms,
        parse_ms,
        df_mensal,
        parser.PARSER_VERSION,
    )
    return finalize_result(df_mensal, meta, as_polars=as_polars, return_meta=return_meta)


async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Dados horários de um ano inteiro via dadoshistoricos (sem token).

    Baixa o ZIP anual público do portal (~100 MB, cache de 1 ano por processo)
    e extrai a estação pedida — alternativa ao apitempo, que exige token para
    dados observacionais. Mesmo schema de saída de `estacao()`.
    """
    if not isinstance(ano, int) or isinstance(ano, bool) or not 2000 <= ano <= date.today().year:
        raise InvalidParameterError(f"ano deve estar entre 2000 e {date.today().year}")

    t0 = time.monotonic()
    raw, source_url = await client.fetch_historico_estacao(codigo, ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_historico_csv(raw, codigo)
    if agregacao == "diario":
        df = parser.agregar_diario(df)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "inmet",
        source_url,
        "httpx+zip+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
