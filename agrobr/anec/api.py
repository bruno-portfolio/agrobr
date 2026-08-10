from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.anec import client, parser
from agrobr.anec.models import (
    TIPO_EFETIVADO,
    TIPO_PROGRAMADO,
    ANECArticle,
    resolve_produto,
)
from agrobr.anec.parser import PERIODO_CURRENT_WEEK, PERIODO_LAST_WEEK
from agrobr.exceptions import SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

logger = structlog.get_logger()


def _warn_license() -> None:
    warn_once(
        "anec_license",
        (
            "ANEC publica os dados sem termos de uso explícitos (zona_cinza). "
            "Uso comercial pode requerer autorização da associação."
        ),
        category=UserWarning,
    )


_PARSE_CACHE: dict[str, tuple[parser.ParsedReport, str, ANECArticle]] = {}


def _parse_cache_clear() -> None:
    _PARSE_CACHE.clear()


async def _fetch_and_parse(
    *,
    ano: int,
    semana: int | None,
    use_cache: bool,
) -> tuple[parser.ParsedReport, str, ANECArticle]:
    if semana is None:
        pdf_bytes, url, article = await client.fetch_latest_pdf(year=ano, use_cache=use_cache)
    else:
        articles = await client.list_articles(ano)
        match = next(
            (a for a in articles if a.week_year == (semana, ano)),
            None,
        )
        if match is None:
            raise SourceUnavailableError(
                source="anec",
                last_error=f"Semana {semana}/{ano} não disponível na ANEC",
            )
        pdf_bytes, url = await client.fetch_pdf_bytes(match, use_cache=use_cache)
        article = match

    if use_cache:
        cache_key = article.cuid
        cached = _PARSE_CACHE.get(cache_key)
        if cached is not None and cached[0].fingerprint:
            return cached

    report = parser.parse_anec_pdf(pdf_bytes)
    if use_cache:
        _PARSE_CACHE[article.cuid] = (report, url, article)
    return report, url, article


def _apply_produto_filter(df: pd.DataFrame, produto: str | None) -> pd.DataFrame:
    if produto is None:
        return df
    produto_canon = resolve_produto(produto)
    return df[df["produto"] == produto_canon]


def _tipo_to_periodo(tipo: str) -> str:
    tipo_norm = tipo.strip().lower()
    if tipo_norm == TIPO_EFETIVADO:
        return PERIODO_LAST_WEEK
    if tipo_norm == TIPO_PROGRAMADO:
        return PERIODO_CURRENT_WEEK
    raise ValueError(f"tipo inválido: {tipo!r}. Use {TIPO_EFETIVADO!r} ou {TIPO_PROGRAMADO!r}.")


def _filter_weekly(
    df: pd.DataFrame,
    *,
    porto: str | None,
    produto: str | None,
    tipo: str | None,
) -> pd.DataFrame:
    if porto is not None:
        canon_porto = parser.resolve_port(porto) or porto.strip().upper()
        df = df[df["porto"] == canon_porto]
    if produto is not None:
        df = df[df["produto"] == resolve_produto(produto)]
    if tipo is not None:
        df = df[df["periodo"] == _tipo_to_periodo(tipo)]
    return df.reset_index(drop=True)


def _build_meta(
    *,
    source_url: str,
    fetch_ms: int,
    parse_ms: int,
    df: pd.DataFrame,
    fingerprint: str,
) -> MetaInfo:
    """Constrói MetaInfo para qualquer função pública do anec.

    `fingerprint` aqui é o hash MD5 da estrutura do PDF (headers + dimensões),
    não o SHA do binário. É armazenado em `raw_content_hash` por compatibilidade
    com o campo existente do MetaInfo. O SHA do PDF binário fica no meta.json
    do cache de disco (`pdf_sha256`), separado.
    """
    return build_source_meta(
        "anec",
        source_url,
        "httpx+pdfplumber",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["anec"],
        selected_source="anec",
        schema_version="1.0",
        raw_content_hash=fingerprint,
    )


@overload
async def embarques(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: Literal["efetivado", "programado"] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def embarques(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: Literal["efetivado", "programado"] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def embarques(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: Literal["efetivado", "programado"] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _warn_license()
    logger.info("anec_embarques", ano=ano, semana=semana, porto=porto, produto=produto)

    t0 = time.monotonic()
    report, url, _article = await _fetch_and_parse(ano=ano, semana=semana, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _filter_weekly(report.weekly_shipments, porto=porto, produto=produto, tipo=tipo)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        source_url=url,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        fingerprint=report.fingerprint,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def embarques_mensais(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def embarques_mensais(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def embarques_mensais(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _warn_license()
    logger.info("anec_embarques_mensais", ano=ano, produto=produto)

    t0 = time.monotonic()
    report, url, _article = await _fetch_and_parse(ano=ano, semana=semana, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _apply_produto_filter(report.monthly_shipments, produto).reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        source_url=url,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        fingerprint=report.fingerprint,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def comparacao_anual(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def comparacao_anual(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def comparacao_anual(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _warn_license()
    logger.info("anec_comparacao_anual", ano=ano, produto=produto)

    t0 = time.monotonic()
    report, url, _article = await _fetch_and_parse(ano=ano, semana=semana, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _apply_produto_filter(report.yoy_comparison, produto).reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        source_url=url,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        fingerprint=report.fingerprint,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def destinos(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def destinos(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def destinos(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _warn_license()
    logger.info("anec_destinos", ano=ano, produto=produto)

    t0 = time.monotonic()
    report, url, _article = await _fetch_and_parse(ano=ano, semana=semana, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _apply_produto_filter(report.destinations, produto).reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        source_url=url,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        fingerprint=report.fingerprint,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def articles_disponiveis(year: int) -> list[dict[str, Any]]:
    _warn_license()
    articles = await client.list_articles(year)
    return [
        {
            "id": a.id,
            "title": a.title_en,
            "slug": a.slug_en,
            "pdf_url": a.pdf_url,
            "created_at": a.created_at.isoformat(),
            "media_updated_at": a.media_updated_at.isoformat(),
            "week": a.week_year[0],
            "year": a.week_year[1],
        }
        for a in articles
    ]
