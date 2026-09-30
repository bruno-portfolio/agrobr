from __future__ import annotations

import time
import warnings
from hashlib import sha256
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.anec import client, models, parser
from agrobr.anec.models import (
    TIPO_EFETIVADO,
    TIPO_PROGRAMADO,
    ANECArticle,
)
from agrobr.anec.parser import PERIODO_CURRENT_WEEK, PERIODO_LAST_WEEK
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)


_PARSE_CACHE: dict[tuple[str, str, str, str], tuple[parser.ParsedReport, str, ANECArticle]] = {}


def _parse_cache_clear() -> None:
    _PARSE_CACHE.clear()


async def _fetch_and_parse(
    *,
    ano: int,
    semana: int | None,
    use_cache: bool,
) -> tuple[parser.ParsedReport, client.Aquisicao, ANECArticle]:
    if semana is None:
        aquisicao, article = await client._acquire_latest(ano, use_cache=use_cache)
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
        aquisicao = await client._acquire_pdf(match, use_cache=use_cache)
        article = match

    cache_key = (
        article.cuid,
        str(article.media_updated_at),
        aquisicao.url,
        sha256(aquisicao.content).hexdigest(),
    )
    if use_cache:
        cached = _PARSE_CACHE.get(cache_key)
        if cached is not None and cached[0].fingerprint:
            return cached[0], aquisicao, article

    report = parser.parse_anec_pdf(aquisicao.content)
    if use_cache:
        _PARSE_CACHE[cache_key] = (report, aquisicao.url, article)
    return report, aquisicao, article


def _apply_produto_filter(df: pd.DataFrame, produto: str | None) -> pd.DataFrame:
    if produto is None:
        return df
    return df[df["produto"] == produto]


def _with_edition(df: pd.DataFrame, article: ANECArticle) -> pd.DataFrame:
    week, year = article.week_year
    result = df.reset_index(drop=True).copy()
    result["ano_relatorio"] = pd.Series(year, index=result.index, dtype="Int64")
    result["semana_relatorio"] = pd.Series(week, index=result.index, dtype="Int64")
    result["edicao_id"] = pd.Series(article.cuid, index=result.index, dtype="object")
    result["publicado_em"] = pd.Series(
        pd.to_datetime(article.created_at, utc=True),
        index=result.index,
        dtype="datetime64[ns, UTC]",
    )
    result["revisado_em"] = pd.Series(
        pd.to_datetime(article.media_updated_at, utc=True),
        index=result.index,
        dtype="datetime64[ns, UTC]",
    )
    return result


def _tipo_to_periodo(tipo: str) -> str:
    tipo_norm = tipo.strip().lower()
    if tipo_norm == TIPO_EFETIVADO:
        return PERIODO_LAST_WEEK
    if tipo_norm == TIPO_PROGRAMADO:
        return PERIODO_CURRENT_WEEK
    raise InvalidParameterError(
        f"tipo inválido: {tipo!r}. Use {TIPO_EFETIVADO!r} ou {TIPO_PROGRAMADO!r}."
    )


def _filter_weekly(
    df: pd.DataFrame,
    *,
    porto: str | None,
    produto: str | None,
    periodo: str | None,
) -> pd.DataFrame:
    if porto is not None:
        canon_porto = parser.resolve_port(porto) or porto.strip().upper()
        df = df[df["porto"] == canon_porto]
    if produto is not None:
        df = df[df["produto"] == produto]
    if periodo is not None:
        df = df[df["periodo"] == periodo]
    return df.reset_index(drop=True)


def _build_meta(
    *,
    aquisicao: client.Aquisicao,
    fetch_ms: int,
    parse_ms: int,
    df: pd.DataFrame,
    fingerprint: str,
    schema_version: str = "1.0",
) -> MetaInfo:
    """Constrói MetaInfo para qualquer função pública do anec.

    `raw_content_hash` e `raw_content_size` descrevem o PDF (SHA-256 e bytes).
    `fingerprint`, o hash MD5 da estrutura do PDF (headers + dimensões), vai em
    `source_details["layout_fingerprint"]`.
    """
    meta = build_source_meta(
        "anec",
        aquisicao.url,
        "httpx+pdfplumber",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["anec"],
        selected_source="anec",
        schema_version=schema_version,
        raw_content_hash=sha256(aquisicao.content).hexdigest(),
        raw_content_size=len(aquisicao.content),
        source_details={**aquisicao.source_details, "layout_fingerprint": fingerprint},
    )
    meta.from_cache = aquisicao.from_cache
    meta.fetched_at = aquisicao.fetched_at
    meta.fetch_timestamp = aquisicao.fetched_at
    return meta


async def _additional_table(
    table: Literal["monthly_shipments", "yoy_comparison", "destinations"],
    *,
    ano: int,
    semana: int | None,
    produto: str | None,
    use_cache: bool,
    as_polars: bool,
    return_meta: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    canonical = models.validate_filters(
        ano,
        semana,
        produto,
        allow_total=table == "yoy_comparison",
        produtos_publicados=models.DESTINOS_PRODUTOS_PUBLICADOS
        if table == "destinations"
        else None,
    )
    client._warn_license()
    logger.info("anec_additional_table", table=table, ano=ano, semana=semana, produto=canonical)
    t0 = time.monotonic()
    report, aquisicao, article = await _fetch_and_parse(ano=ano, semana=semana, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)
    t1 = time.monotonic()
    raw: pd.DataFrame = getattr(report, table)
    df = _with_edition(_apply_produto_filter(raw, canonical), article)
    meta = _build_meta(
        aquisicao=aquisicao,
        fetch_ms=fetch_ms,
        parse_ms=int((time.monotonic() - t1) * 1000),
        df=df,
        fingerprint=report.fingerprint,
        schema_version="1.2" if table == "yoy_comparison" else "1.1",
    )
    if table == "destinations" and raw.empty:
        message = (
            "Nenhuma participação por destino foi extraída desta edição. "
            "O layout pode conter apenas gráficos; o resultado vazio não confirma ausência de embarques."
        )
        meta.validation_warnings.append(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


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
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if kwargs:
        raise TypeError(f"Parâmetros ANEC não suportados: {sorted(kwargs)}")
    produto_canonico = models.validate_filters(ano, semana, produto)
    periodo = None if tipo is None else _tipo_to_periodo(tipo)
    client._warn_license()
    logger.info("anec_embarques", ano=ano, semana=semana, porto=porto, produto=produto)

    t0 = time.monotonic()
    report, aquisicao, _article = await _fetch_and_parse(
        ano=ano, semana=semana, use_cache=use_cache
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _filter_weekly(
        report.weekly_shipments, porto=porto, produto=produto_canonico, periodo=periodo
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        aquisicao=aquisicao,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        fingerprint=report.fingerprint,
        schema_version="1.1",
    )
    colunas = set(zip(df["produto"], df["periodo"], strict=True))
    for produto, periodo, aviso in report.avisos_da_linha_total:
        if (produto, periodo) in colunas:
            meta.validation_warnings.append(aviso)
            warn_once(f"anec_linha_total:{aviso}", aviso)
    if not df.empty and df["data_inicio"].isna().any():
        message = (
            f"Os rótulos das duas semanas do boletim {df['semana'].iloc[0]}/{df['ano'].iloc[0]} "
            "não formam semanas consecutivas de 7 dias a até 7 dias da semana da edição; "
            "data_inicio e data_fim ficam nulas."
        )
        meta.validation_warnings.append(message)
        warnings.warn(message, UserWarning, stacklevel=2)
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
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if kwargs:
        raise TypeError(f"Parâmetros ANEC não suportados: {sorted(kwargs)}")
    return await _additional_table(
        "monthly_shipments",
        ano=ano,
        semana=semana,
        produto=produto,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )


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
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if kwargs:
        raise TypeError(f"Parâmetros ANEC não suportados: {sorted(kwargs)}")
    return await _additional_table(
        "yoy_comparison",
        ano=ano,
        semana=semana,
        produto=produto,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )


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
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if kwargs:
        raise TypeError(f"Parâmetros ANEC não suportados: {sorted(kwargs)}")
    return await _additional_table(
        "destinations",
        ano=ano,
        semana=semana,
        produto=produto,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )


async def articles_disponiveis(year: int) -> list[dict[str, Any]]:
    client._warn_license()
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
