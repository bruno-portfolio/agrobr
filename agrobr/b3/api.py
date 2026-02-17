from __future__ import annotations

import asyncio
import time
import warnings
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import (
    B3_CONTRATOS_AGRO,
    COLUNAS_OI_SAIDA,
    COLUNAS_SAIDA,
    TICKERS_AGRO,
    TICKERS_AGRO_OI,
)

_WARNED = False

logger = structlog.get_logger()


@overload
async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "agrobr.b3: dados da B3 (empresa privada). Ajustes diarios publicados "
            "sem autenticacao, mas termos de uso para acesso programatico nao sao "
            "claros. Classificacao: zona_cinza. Veja docs/licenses.md.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

    logger.info("b3_ajustes", data=str(data), contrato=contrato)

    data_str = data.strftime("%d/%m/%Y") if isinstance(data, date) else data

    t0 = time.monotonic()
    html, source_url = await client.fetch_ajustes(data_str)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_ajustes_html(html)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if contrato is not None:
        ticker = B3_CONTRATOS_AGRO.get(contrato, contrato.upper())
        if ticker in TICKERS_AGRO:
            df = df[df["ticker"] == ticker].reset_index(drop=True)
        else:
            df = df[df["ticker"] == contrato.upper()].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="b3",
            source_url=source_url,
            source_method="httpx+html",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["b3"],
            selected_source="b3",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("b3_historico", contrato=contrato, inicio=str(inicio), fim=str(fim))

    inicio_dt = datetime.strptime(inicio, "%Y-%m-%d").date() if isinstance(inicio, str) else inicio
    fim_dt = datetime.strptime(fim, "%Y-%m-%d").date() if isinstance(fim, str) else fim

    t0 = time.monotonic()
    frames: list[pd.DataFrame] = []
    current = inicio_dt

    while current <= fim_dt:
        if current.weekday() < 5:
            try:
                df_dia = await ajustes(data=current, contrato=contrato)
                if not df_dia.empty:
                    frames.append(df_dia)
            except Exception:
                logger.debug("b3_historico_skip", data=str(current))
            await asyncio.sleep(1.0)
        current += timedelta(days=1)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUNAS_SAIDA)

    if vencimento is not None:
        vct_upper = vencimento.strip().upper()
        df = df[df["vencimento_codigo"] == vct_upper].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="b3",
            source_url=client.BASE_URL,
            source_method="httpx+html",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=0,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["b3"],
            selected_source="b3",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


def contratos() -> list[str]:
    return sorted(B3_CONTRATOS_AGRO.keys())


@overload
async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "agrobr.b3: dados da B3 (empresa privada). Posicoes em aberto publicadas "
            "sem autenticacao, mas termos de uso para acesso programatico nao sao "
            "claros. Classificacao: zona_cinza. Veja docs/licenses.md.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

    logger.info("b3_posicoes_abertas", data=str(data), contrato=contrato, tipo=tipo)

    data_str = data.strftime("%Y-%m-%d") if isinstance(data, date) else data

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_posicoes_abertas(data_str)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_posicoes_abertas(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if contrato is not None:
        ticker = B3_CONTRATOS_AGRO.get(contrato, contrato.upper())
        if ticker in TICKERS_AGRO_OI:
            df = df[df["ticker"] == ticker].reset_index(drop=True)
        else:
            df = df[df["ticker"] == contrato.upper()].reset_index(drop=True)

    if tipo is not None:
        df = df[df["tipo"] == tipo].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="b3",
            source_url=source_url,
            source_method="httpx+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION_OI,
            schema_version="1.0",
            attempted_sources=["b3"],
            selected_source="b3",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def oi_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def oi_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def oi_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("b3_oi_historico", contrato=contrato, inicio=str(inicio), fim=str(fim))

    inicio_dt = datetime.strptime(inicio, "%Y-%m-%d").date() if isinstance(inicio, str) else inicio
    fim_dt = datetime.strptime(fim, "%Y-%m-%d").date() if isinstance(fim, str) else fim

    t0 = time.monotonic()
    frames: list[pd.DataFrame] = []
    current = inicio_dt

    while current <= fim_dt:
        if current.weekday() < 5:
            try:
                df_dia = await posicoes_abertas(data=current, contrato=contrato, tipo=tipo)
                if not df_dia.empty:
                    frames.append(df_dia)
            except Exception:
                logger.debug("b3_oi_historico_skip", data=str(current))
            await asyncio.sleep(1.0)
        current += timedelta(days=1)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUNAS_OI_SAIDA)

    if vencimento is not None:
        vct_upper = vencimento.strip().upper()
        df = df[df["vencimento_codigo"] == vct_upper].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="b3",
            source_url=client.BASE_URL_ARQUIVOS,
            source_method="httpx+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=0,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION_OI,
            schema_version="1.0",
            attempted_sources=["b3"],
            selected_source="b3",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
