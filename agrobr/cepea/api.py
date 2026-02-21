"""API pública do módulo CEPEA.

Dados CEPEA/ESALQ licenciados sob CC BY-NC 4.0.
Uso comercial requer autorização do CEPEA (cepea@usp.br).
Ref: https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx
"""

from __future__ import annotations

import hashlib
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.duckdb_store import get_store
from agrobr.cache.keys import build_cache_key
from agrobr.cache.policies import calculate_expiry
from agrobr.cepea import client
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.models import Indicador, MetaInfo
from agrobr.validators.sanity import validate_batch

if TYPE_CHECKING:
    import polars as pl

logger = structlog.get_logger()

SOURCE_WINDOW_DAYS = 10


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
    """
    Obtém série de indicadores de preço do CEPEA.

    Usa estratégia de acumulação progressiva:
    1. Busca dados no histórico local (DuckDB)
    2. Se faltar dados recentes, busca da fonte (CEPEA/Notícias Agrícolas)
    3. Salva novos dados no histórico
    4. Retorna DataFrame completo do período solicitado

    Args:
        produto: Nome do produto (soja, milho, cafe, boi, etc)
        praca: Praça específica (opcional)
        inicio: Data inicial (YYYY-MM-DD ou date)
        fim: Data final (YYYY-MM-DD ou date)
        moeda: Moeda (BRL ou USD)
        as_polars: Se True, retorna polars.DataFrame
        validate_sanity: Se True, valida dados estatisticamente
        force_refresh: Se True, ignora histórico e busca da fonte
        offline: Se True, usa apenas histórico local
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com indicadores ou tupla (DataFrame, MetaInfo) se return_meta=True
    """
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="unknown",
        source_url="",
        source_method="unknown",
        fetched_at=datetime.now(),
    )
    if isinstance(inicio, str):
        inicio = datetime.strptime(inicio, "%Y-%m-%d").date()
    if isinstance(fim, str):
        fim = datetime.strptime(fim, "%Y-%m-%d").date()

    if fim is None:
        fim = date.today()
    if inicio is None:
        inicio = fim - timedelta(days=365)

    store = get_store()
    indicadores: list[Indicador] = []

    source_url = ""
    parser_version = 1

    if not force_refresh:
        cached_data = store.indicadores_query(
            produto=produto,
            inicio=datetime.combine(inicio, datetime.min.time()),
            fim=datetime.combine(fim, datetime.max.time()),
            praca=praca,
        )

        indicadores = _dicts_to_indicadores(cached_data)

        if indicadores:
            meta.from_cache = True
            meta.source = "cache"
            meta.source_method = "duckdb"

        logger.info(
            "history_query",
            produto=produto,
            inicio=inicio,
            fim=fim,
            cached_count=len(indicadores),
        )

    needs_fetch = False
    if not offline:
        if force_refresh:
            needs_fetch = True
        else:
            today = date.today()
            recent_start = today - timedelta(days=SOURCE_WINDOW_DAYS)

            if fim >= recent_start:
                existing_dates = {ind.data for ind in indicadores}
                for i in range(min(SOURCE_WINDOW_DAYS, (fim - max(inicio, recent_start)).days + 1)):
                    check_date = fim - timedelta(days=i)
                    if check_date.weekday() < 5 and check_date not in existing_dates:
                        needs_fetch = True
                        break

    if needs_fetch:
        logger.info("fetching_from_source", produto=produto)

        try:
            parse_start = time.perf_counter()
            fetch_result = await client.fetch_indicador_page(produto)
            html = fetch_result.html
            source_name = fetch_result.source
            raw_content_size = len(html.encode("utf-8"))
            raw_content_hash = f"sha256:{hashlib.sha256(html.encode('utf-8')).hexdigest()[:16]}"

            if source_name == "noticias_agricolas":
                from agrobr.noticias_agricolas.parser import parse_indicador as na_parse

                new_indicadores = na_parse(html, produto)
                source_url = f"https://www.noticiasagricolas.com.br/cotacoes/{produto}"
                meta.source = "noticias_agricolas"
                meta.source_method = "httpx"
                logger.info(
                    "parse_success",
                    source="noticias_agricolas",
                    records_count=len(new_indicadores),
                )
            else:
                parser, new_indicadores = await get_parser_with_fallback(html, produto)
                source_url = f"https://www.cepea.esalq.usp.br/br/indicador/{produto}.aspx"
                meta.source = "cepea"
                meta.source_method = "httpx"
                parser_version = parser.version

            parse_duration_ms = int((time.perf_counter() - parse_start) * 1000)
            meta.parse_duration_ms = parse_duration_ms
            meta.source_url = source_url
            meta.raw_content_hash = raw_content_hash
            meta.raw_content_size = raw_content_size
            meta.parser_version = parser_version
            meta.from_cache = False

            if new_indicadores:
                new_dicts = _indicadores_to_dicts(new_indicadores)
                saved_count = store.indicadores_upsert(new_dicts)

                logger.info(
                    "new_data_saved",
                    produto=produto,
                    fetched=len(new_indicadores),
                    saved=saved_count,
                )

                existing_dates = {ind.data for ind in indicadores}
                for ind in new_indicadores:
                    if ind.data not in existing_dates:
                        indicadores.append(ind)
            elif indicadores:
                import warnings

                from agrobr.exceptions import StaleDataWarning

                warnings.warn(
                    f"Fresh fetch for '{produto}' returned no data. Using cached data.",
                    StaleDataWarning,
                    stacklevel=2,
                )
                meta.validation_warnings.append("stale_data: using cache after empty fetch")

        except Exception as e:
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
                    import warnings

                    from agrobr.exceptions import StaleDataWarning

                    indicadores = _dicts_to_indicadores(cached_fallback)
                    warnings.warn(
                        f"All sources failed for '{produto}'. Using stale cache ({len(indicadores)} records).",
                        StaleDataWarning,
                        stacklevel=2,
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

    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta
            return result_df
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    if return_meta:
        return df, meta
    return df


def _dicts_to_indicadores(dicts: list[dict[str, Any]]) -> list[Indicador]:
    """Converte lista de dicts do banco para objetos Indicador."""
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
        except Exception as e:
            logger.warning("indicador_conversion_failed", error=str(e), data=d)
    return indicadores


def _indicadores_to_dicts(indicadores: list[Indicador]) -> list[dict[str, Any]]:
    """Converte lista de Indicador para dicts para salvar no banco."""
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
    """Lista produtos disponíveis no CEPEA."""
    return list(constants.CEPEA_PRODUTOS.keys())


async def pracas(produto: str) -> list[str]:
    """
    Lista praças disponíveis para um produto.

    Args:
        produto: Nome do produto

    Returns:
        Lista de praças disponíveis
    """
    pracas_map = {
        "soja": ["paranagua", "parana", "rio_grande_do_sul"],
        "milho": ["campinas", "parana"],
        "cafe": ["mogiana", "sul_de_minas"],
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
    """
    Obtém último indicador disponível.

    Args:
        produto: Nome do produto
        praca: Praça específica (opcional)
        offline: Se True, usa apenas histórico local

    Returns:
        Último Indicador disponível
    """
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

            except Exception as e:
                logger.warning("source_fetch_failed", produto=produto, error=str(e))

    if praca:
        indicadores = [
            ind for ind in indicadores if ind.praca and ind.praca.lower() == praca.lower()
        ]

    if not indicadores:
        from agrobr.exceptions import ParseError

        raise ParseError(
            source="cepea",
            parser_version=1,
            reason=f"No indicators found for {produto}",
        )

    indicadores.sort(key=lambda x: x.data, reverse=True)
    return indicadores[0]


def _to_dataframe(indicadores: list[Indicador]) -> pd.DataFrame:
    """Converte lista de indicadores para DataFrame."""
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
