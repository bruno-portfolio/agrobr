from __future__ import annotations

import hashlib
import time
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import (
    PARSER_VERSION,
    PERIODOS_RESUMO,
    PRODUTOS_HISTORICO,
    PRODUTOS_QUINZENAL,
    REGIOES_QUINZENAL,
    resolve_produto,
)

logger = structlog.get_logger()

_LICENSE_WARNING = (
    "Dados da UNICA (unicadata.com.br) sem termos de uso públicos — classificação "
    "zona cinza. Uso educacional/pesquisa; para uso comercial, consulte a UNICA. "
    "Detalhes em docs/licenses.md"
)


def _warn_license() -> None:
    warn_once("unica_license", _LICENSE_WARNING)


@overload
async def moagem_quinzenal(
    produto: str = "cana",
    *,
    regiao: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def moagem_quinzenal(
    produto: str = "cana",
    *,
    regiao: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def moagem_quinzenal(
    produto: str = "cana",
    *,
    regiao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Série quinzenal acumulada da safra corrente do Centro-Sul (relatório UNICA).

    Extraída do PDF quinzenal mais recente: moagem de cana (t) e produção de
    açúcar (t) e etanol (m³) por região, safra corrente vs anterior. Cobre
    apenas a safra do relatório vigente — histórico longo quinzenal não é
    disponibilizado pela fonte.
    """
    _warn_license()
    produto_canonico = resolve_produto(produto, PRODUTOS_QUINZENAL)
    if regiao is not None and regiao not in REGIOES_QUINZENAL:
        raise InvalidParameterError(f"Região '{regiao}' inválida. Opções: {REGIOES_QUINZENAL}")

    parsed, source_url, fetch_ms, parse_ms, pdf_bytes = await _fetch_and_parse_quinzenal()

    df = parsed.series[parsed.series["produto"] == produto_canonico]
    if regiao is not None:
        df = df[df["regiao"] == regiao]
    df = df.reset_index(drop=True)

    meta = build_source_meta(
        "unica",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["unica"],
        selected_source="unica",
        raw_content_hash=hashlib.sha256(pdf_bytes).hexdigest(),
        raw_content_size=len(pdf_bytes),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def safra_resumo(
    *,
    periodo: Literal["acumulado", "quinzena", "mensal"] = "acumulado",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def safra_resumo(
    *,
    periodo: Literal["acumulado", "quinzena", "mensal"] = "acumulado",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def safra_resumo(
    *,
    periodo: Literal["acumulado", "quinzena", "mensal"] = "acumulado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Resumo da posição da safra Centro-Sul (Tabelas 1-2 do relatório UNICA).

    Moagem, açúcar, etanol, ATR, mix açúcar/etanol e rendimentos por região,
    no acumulado da safra ou no período da edição (quinzena ou mês), sempre
    comparando com a safra anterior. Período e datas vêm do título de cada
    tabela; pedir um período que a edição corrente não publica levanta
    InvalidParameterError com os períodos disponíveis.
    """
    _warn_license()
    if periodo not in PERIODOS_RESUMO:
        raise InvalidParameterError(
            f"Período '{periodo}' inválido. Opções: {list(PERIODOS_RESUMO)}"
        )

    parsed, source_url, fetch_ms, parse_ms, pdf_bytes = await _fetch_and_parse_quinzenal()

    disponiveis = sorted(set(parsed.resumo["periodo"]))
    if periodo not in disponiveis:
        raise InvalidParameterError(
            f"A edição de {parsed.posicao:%d/%m/%Y} não publica o período '{periodo}'. "
            f"Períodos da edição: {disponiveis}"
        )
    df = parsed.resumo[parsed.resumo["periodo"] == periodo].reset_index(drop=True)

    meta = build_source_meta(
        "unica",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["unica"],
        selected_source="unica",
        raw_content_hash=hashlib.sha256(pdf_bytes).hexdigest(),
        raw_content_size=len(pdf_bytes),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def producao_historica(
    produto: str = "cana",
    *,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def producao_historica(
    produto: str = "cana",
    *,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def producao_historica(
    produto: str = "cana",
    *,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Produção histórica anual por estado (UNICA, safras 1980/1981 a 2020/2021).

    Matriz estado × safra do site clássico unicadata, com agregados
    centro_sul, norte_nordeste e brasil. O banco da fonte está congelado:
    safras a partir de 2021/2022 não foram publicadas nesse formato.
    """
    _warn_license()
    produto_canonico = resolve_produto(produto, PRODUTOS_HISTORICO)

    t0 = time.monotonic()
    content, source_url = await client.fetch_historico_xlsx(
        produto_canonico, safra_inicio, safra_fim
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_historico_xlsx(content, produto_canonico)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "unica",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["unica"],
        selected_source="unica",
        raw_content_hash=hashlib.sha256(content).hexdigest(),
        raw_content_size=len(content),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


_parsed_cache: tuple[str, parser.ParsedQuinzenal] | None = None


async def _fetch_and_parse_quinzenal() -> tuple[parser.ParsedQuinzenal, str, int, int, bytes]:
    """Cache de 1 entrada keyed pela URL do PDF: `moagem_quinzenal` e
    `safra_resumo` na mesma sessão pagam um único parse."""
    global _parsed_cache
    t0 = time.monotonic()
    pdf_bytes, source_url = await client.fetch_quinzenal_pdf()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    if _parsed_cache is not None and _parsed_cache[0] == source_url:
        return _parsed_cache[1], source_url, fetch_ms, 0, pdf_bytes

    t1 = time.monotonic()
    parsed = parser.parse_quinzenal_pdf(pdf_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    _parsed_cache = (source_url, parsed)
    logger.info(
        "unica_quinzenal_parsed",
        safra=parsed.safra,
        posicao=str(parsed.posicao.date()),
        series_rows=len(parsed.series),
    )
    return parsed, source_url, fetch_ms, parse_ms, pdf_bytes
