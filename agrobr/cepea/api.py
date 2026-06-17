from __future__ import annotations

import hashlib
import time
import warnings
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, overload

import duckdb
import httpx
import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.duckdb_store import get_store
from agrobr.cache.keys import build_cache_key
from agrobr.cache.policies import calculate_expiry
from agrobr.cepea import client
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.exceptions import ParseError, SourceUnavailableError, StaleDataWarning
from agrobr.models import Indicador, MetaInfo
from agrobr.utils.result import finalize_result
from agrobr.utils.time import utcnow
from agrobr.validators.sanity import validate_batch

if TYPE_CHECKING:
    import polars as pl

logger = structlog.get_logger()

SOURCE_WINDOW_DAYS = 10


def _normalize_dates(
    inicio: str | date | None,
    fim: str | date | None,
) -> tuple[date, date]:
    if isinstance(inicio, str):
        inicio = datetime.strptime(inicio, "%Y-%m-%d").date()
    if isinstance(fim, str):
        fim = datetime.strptime(fim, "%Y-%m-%d").date()
    if fim is None:
        fim = date.today()
    if inicio is None:
        inicio = fim - timedelta(days=365)
    return inicio, fim


def _needs_fetch(
    indicadores: list[Indicador],
    inicio: date,
    fim: date,
    force_refresh: bool,
    offline: bool,
) -> bool:
    if offline:
        return False
    if force_refresh:
        return True
    today = date.today()
    recent_start = today - timedelta(days=SOURCE_WINDOW_DAYS)
    if fim < recent_start:
        return False
    existing_dates = {ind.data for ind in indicadores}
    for i in range(min(SOURCE_WINDOW_DAYS, (fim - max(inicio, recent_start)).days + 1)):
        check_date = fim - timedelta(days=i)
        if check_date.weekday() < 5 and check_date not in existing_dates:
            return True
    return False


def _warn_stale(message: str, meta: MetaInfo) -> None:
    warnings.warn(message, StaleDataWarning, stacklevel=3)
    meta.validation_warnings.append("stale_data: using cache after empty fetch")


class _FetchResult(NamedTuple):
    indicadores: list[Indicador]
    source_name: str
    source_url: str
    parser_version: int
    raw_hash: str
    raw_size: int
    parse_ms: int


def _cepea_source_url(produto: str) -> str:
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    return f"{constants.URLS[constants.Fonte.CEPEA]['indicadores']}/{produto_key}.aspx"


def _noticias_agricolas_source_url(produto: str) -> str:
    produto_key = constants.NOTICIAS_AGRICOLAS_PRODUTOS.get(produto.lower(), produto.lower())
    return f"{constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]['cotacoes']}/{produto_key}"


async def _fetch_and_parse(produto: str) -> _FetchResult:
    parse_start = time.perf_counter()
    fetch_result = await client.fetch_indicador_page(produto)
    html = fetch_result.html
    source_name = fetch_result.source
    raw_size = len(html.encode("utf-8"))
    raw_hash = f"sha256:{hashlib.sha256(html.encode('utf-8')).hexdigest()[:16]}"

    if source_name == "noticias_agricolas":
        from agrobr.noticias_agricolas import parser as na_parser

        new_indicadores = na_parser.parse_indicador(html, produto)
        source_url = _noticias_agricolas_source_url(produto)
        parser_version = na_parser.PARSER_VERSION
        logger.info(
            "parse_success",
            source="noticias_agricolas",
            records_count=len(new_indicadores),
        )
    else:
        parser, new_indicadores = await get_parser_with_fallback(html, produto)
        source_url = _cepea_source_url(produto)
        parser_version = parser.version

    parse_ms = int((time.perf_counter() - parse_start) * 1000)
    return _FetchResult(
        indicadores=new_indicadores,
        source_name=source_name,
        source_url=source_url,
        parser_version=parser_version,
        raw_hash=raw_hash,
        raw_size=raw_size,
        parse_ms=parse_ms,
    )


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    _moeda: str = "BRL",
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame | pl.DataFrame: ...


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    _moeda: str = "BRL",
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame | pl.DataFrame, MetaInfo]: ...


async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    _moeda: str = "BRL",
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]:
    """Série histórica de indicadores CEPEA/ESALQ.

    Busca dados do cache DuckDB e, se necessário, faz fetch na fonte
    (CEPEA ou Notícias Agrícolas como fallback).

    Args:
        produto: Código do produto (ex: "soja", "milho", "boi_gordo").
        praca: Praça de cotação. ``None`` retorna todas.
        inicio: Data inicial (ISO string ou ``date``). Default: 365 dias atrás.
        fim: Data final. Default: hoje.
        _moeda: Reservado para conversão futura de moeda. Sem efeito atual.
        as_polars: Retorna ``polars.DataFrame`` em vez de pandas.
        validate_sanity: Aplica validação estatística (outliers, gaps).
        force_refresh: Ignora cache e força fetch na fonte.
        offline: Usa apenas cache local, sem requests HTTP.
        return_meta: Retorna tupla ``(df, MetaInfo)`` com metadados de proveniência.
    """
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="unknown",
        source_url="",
        source_method="unknown",
        fetched_at=utcnow(),
    )
    inicio, fim = _normalize_dates(inicio, fim)

    store = get_store()
    indicadores: list[Indicador] = []

    if not force_refresh:
        try:
            cached_data = store.indicadores_query(
                produto=produto,
                inicio=datetime.combine(inicio, datetime.min.time()),
                fim=datetime.combine(fim, datetime.max.time()),
                praca=praca,
            )
        except duckdb.Error as e:
            logger.warning("cache_query_failed", produto=produto, error=str(e))
            cached_data = []

        indicadores = _dicts_to_indicadores(cached_data)

        if indicadores:
            meta.from_cache = True
            meta.source = "cache"
            meta.source_method = "duckdb"
            meta.attempted_sources = ["cache"]
            meta.selected_source = "cache"

        logger.info(
            "history_query",
            produto=produto,
            inicio=inicio,
            fim=fim,
            cached_count=len(indicadores),
        )

    if _needs_fetch(indicadores, inicio, fim, force_refresh, offline):
        logger.info("fetching_from_source", produto=produto)

        try:
            result = await _fetch_and_parse(produto)

            meta.source = (
                "noticias_agricolas" if result.source_name == "noticias_agricolas" else "cepea"
            )
            meta.attempted_sources = (
                ["cepea", "noticias_agricolas"]
                if result.source_name == "noticias_agricolas"
                else ["cepea"]
            )
            meta.selected_source = meta.source
            meta.source_method = "httpx"
            meta.parse_duration_ms = result.parse_ms
            meta.source_url = result.source_url
            meta.raw_content_hash = result.raw_hash
            meta.raw_content_size = result.raw_size
            meta.parser_version = result.parser_version
            meta.from_cache = False

            if result.indicadores:
                new_dicts = _indicadores_to_dicts(result.indicadores)
                try:
                    saved_count = store.indicadores_upsert(new_dicts)
                except duckdb.Error as e:
                    logger.warning("cache_upsert_failed", produto=produto, error=str(e))
                    saved_count = 0

                logger.info(
                    "new_data_saved",
                    produto=produto,
                    fetched=len(result.indicadores),
                    saved=saved_count,
                )

                existing_dates = {ind.data for ind in indicadores}
                for ind in result.indicadores:
                    if ind.data not in existing_dates:
                        indicadores.append(ind)
            elif indicadores:
                _warn_stale(
                    f"Fresh fetch for '{produto}' returned no data. Using cached data.",
                    meta,
                )

        except (httpx.HTTPError, SourceUnavailableError, ParseError, OSError) as e:
            logger.warning(
                "source_fetch_failed",
                produto=produto,
                error=str(e),
            )
            meta.validation_warnings.append(f"source_fetch_failed: {e}")
            if not indicadores:
                cached_fallback = store.indicadores_query(
                    produto=produto,
                    inicio=datetime.combine(inicio, datetime.min.time()),
                    fim=datetime.combine(fim, datetime.max.time()),
                    praca=praca,
                )
                if cached_fallback:
                    indicadores = _dicts_to_indicadores(cached_fallback)
                    _warn_stale(
                        f"All sources failed for '{produto}'. Using stale cache ({len(indicadores)} records).",
                        meta,
                    )
                    meta.from_cache = True
                    meta.source = "cache_fallback"

    if validate_sanity and indicadores:
        indicadores, anomalies = await validate_batch(indicadores)

    indicadores = [ind for ind in indicadores if inicio <= ind.data <= fim]

    if praca:
        indicadores = [
            ind for ind in indicadores if ind.praca and ind.praca.lower() == praca.lower()
        ]

    df = _to_dataframe(indicadores)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist() if not df.empty else []
    meta.cache_key = build_cache_key(
        "cepea",
        {"produto": produto, "praca": praca or "all"},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry(constants.Fonte.CEPEA)

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _dicts_to_indicadores(dicts: list[dict[str, Any]]) -> list[Indicador]:
    indicadores = []
    for d in dicts:
        try:
            ind = Indicador(
                fonte=constants.Fonte(d["fonte"]) if d.get("fonte") else constants.Fonte.CEPEA,
                produto=d["produto"],
                praca=d.get("praca"),
                data=d["data"] if isinstance(d["data"], date) else d["data"].date(),
                valor=Decimal(str(d["valor"])),
                unidade=d.get("unidade", "BRL/unidade"),
                metodologia=d.get("metodologia"),
                parser_version=d.get("parser_version", 1),
            )
            indicadores.append(ind)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("indicador_conversion_failed", error=str(e), data=d)
    return indicadores


def _indicadores_to_dicts(indicadores: list[Indicador]) -> list[dict[str, Any]]:
    return [
        {
            "produto": ind.produto,
            "praca": ind.praca,
            "data": ind.data,
            "valor": float(ind.valor),
            "unidade": ind.unidade,
            "fonte": ind.fonte.value,
            "metodologia": ind.metodologia,
            "variacao_percentual": ind.meta.get("variacao_percentual"),
            "parser_version": ind.parser_version,
        }
        for ind in indicadores
    ]


async def produtos() -> list[str]:
    return list(constants.CEPEA_PRODUTOS.keys())


async def pracas(produto: str) -> list[str]:
    if produto.lower() not in constants.CEPEA_PRODUTOS:
        raise ValueError(
            f"Produto inválido: '{produto}'. Opções: {sorted(constants.CEPEA_PRODUTOS)}"
        )
    pracas_map = {
        "soja": ["paranagua", "parana", "rio_grande_do_sul"],
        "milho": ["campinas", "parana"],
        "cafe": ["mogiana", "sul_de_minas"],
        "cafe_robusta": ["espirito_santo"],
        "boi": ["sao_paulo"],
        "trigo": ["parana", "rio_grande_do_sul"],
        "arroz": ["rio_grande_do_sul"],
        "acucar": ["sao_paulo"],
        "frango_congelado": ["sao_paulo"],
        "frango_resfriado": ["sao_paulo"],
        "suino": ["sao_paulo"],
        "leite": ["minas_gerais", "goias", "parana", "rio_grande_do_sul", "sao_paulo"],
        "laranja_industria": ["sao_paulo"],
        "laranja_in_natura": ["sao_paulo"],
    }
    return pracas_map.get(produto.lower(), [])


async def ultimo(produto: str, praca: str | None = None, offline: bool = False) -> Indicador:
    store = get_store()
    indicadores: list[Indicador] = []

    fim = date.today()
    inicio = fim - timedelta(days=30)

    cached_data = store.indicadores_query(
        produto=produto,
        inicio=datetime.combine(inicio, datetime.min.time()),
        fim=datetime.combine(fim, datetime.max.time()),
        praca=praca,
    )

    if cached_data:
        indicadores = _dicts_to_indicadores(cached_data)

    if not offline:
        has_recent = any(ind.data >= fim - timedelta(days=3) for ind in indicadores)

        if not has_recent:
            try:
                fetch_result = await client.fetch_indicador_page(produto)
                html = fetch_result.html
                source_name = fetch_result.source

                if source_name == "noticias_agricolas":
                    from agrobr.noticias_agricolas.parser import parse_indicador as na_parse

                    new_indicadores = na_parse(html, produto)
                else:
                    parser, new_indicadores = await get_parser_with_fallback(html, produto)

                if new_indicadores:
                    new_dicts = _indicadores_to_dicts(new_indicadores)
                    store.indicadores_upsert(new_dicts)

                    existing_dates = {ind.data for ind in indicadores}
                    for ind in new_indicadores:
                        if ind.data not in existing_dates:
                            indicadores.append(ind)

            except (httpx.HTTPError, SourceUnavailableError, ParseError, OSError) as e:
                logger.warning("source_fetch_failed", produto=produto, error=str(e))

    if praca:
        indicadores = [
            ind for ind in indicadores if ind.praca and ind.praca.lower() == praca.lower()
        ]

    if not indicadores:
        raise ParseError(
            source="cepea",
            parser_version=1,
            reason=f"No indicators found for {produto}",
        )

    indicadores.sort(key=lambda x: x.data, reverse=True)
    return indicadores[0]


def _to_dataframe(indicadores: list[Indicador]) -> pd.DataFrame:
    if not indicadores:
        return pd.DataFrame()

    data = [
        {
            "data": ind.data,
            "produto": ind.produto,
            "praca": ind.praca,
            "valor": float(ind.valor),
            "unidade": ind.unidade,
            "fonte": ind.fonte.value,
            "metodologia": ind.metodologia,
            "anomalies": ind.anomalies if ind.anomalies else None,
        }
        for ind in indicadores
    ]

    df = pd.DataFrame(data)
    df["data"] = pd.to_datetime(df["data"])
    df = df.sort_values("data").reset_index(drop=True)

    return df
