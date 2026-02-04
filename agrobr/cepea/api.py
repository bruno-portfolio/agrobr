"""API pública do módulo CEPEA."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.duckdb_store import get_store
from agrobr.cepea import client
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.models import Indicador
from agrobr.validators.sanity import validate_batch

if TYPE_CHECKING:
    import polars as pl

logger = structlog.get_logger()

# Janela de dados disponível na fonte (Notícias Agrícolas tem ~10 dias)
SOURCE_WINDOW_DAYS = 10


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
) -> pd.DataFrame | pl.DataFrame:
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

    Returns:
        DataFrame com indicadores
    """
    # Normaliza datas
    if isinstance(inicio, str):
        inicio = datetime.strptime(inicio, "%Y-%m-%d").date()
    if isinstance(fim, str):
        fim = datetime.strptime(fim, "%Y-%m-%d").date()

    # Default: último ano
    if fim is None:
        fim = date.today()
    if inicio is None:
        inicio = fim - timedelta(days=365)

    store = get_store()
    indicadores: list[Indicador] = []

    # 1. Busca no histórico local
    if not force_refresh:
        cached_data = store.indicadores_query(
            produto=produto,
            inicio=datetime.combine(inicio, datetime.min.time()),
            fim=datetime.combine(fim, datetime.max.time()),
            praca=praca,
        )

        indicadores = _dicts_to_indicadores(cached_data)

        logger.info(
            "history_query",
            produto=produto,
            inicio=inicio,
            fim=fim,
            cached_count=len(indicadores),
        )

    # 2. Verifica se precisa buscar dados recentes
    needs_fetch = False
    if not offline:
        if force_refresh:
            needs_fetch = True
        else:
            # Verifica se faltam dados na janela recente
            today = date.today()
            recent_start = today - timedelta(days=SOURCE_WINDOW_DAYS)

            # Se o período solicitado inclui dados recentes
            if fim >= recent_start:
                existing_dates = {ind.data for ind in indicadores}
                # Verifica se tem lacunas nos últimos dias
                for i in range(min(SOURCE_WINDOW_DAYS, (fim - max(inicio, recent_start)).days + 1)):
                    check_date = fim - timedelta(days=i)
                    # Pula fins de semana (CEPEA não publica)
                    if check_date.weekday() < 5 and check_date not in existing_dates:
                        needs_fetch = True
                        break

    # 3. Busca da fonte se necessário
    if needs_fetch:
        logger.info("fetching_from_source", produto=produto)

        try:
            # Tenta buscar - pode vir do CEPEA ou Notícias Agrícolas
            html = await client.fetch_indicador_page(produto)

            # Detecta fonte pelo conteúdo do HTML
            is_noticias_agricolas = "noticiasagricolas" in html.lower() or "cot-fisicas" in html

            if is_noticias_agricolas:
                # Usa parser de Notícias Agrícolas
                from agrobr.noticias_agricolas.parser import parse_indicador as na_parse

                new_indicadores = na_parse(html, produto)
                logger.info(
                    "parse_success",
                    source="noticias_agricolas",
                    records_count=len(new_indicadores),
                )
            else:
                # Usa parser CEPEA
                parser, new_indicadores = await get_parser_with_fallback(html, produto)

            if new_indicadores:
                # 4. Salva novos dados no histórico
                new_dicts = _indicadores_to_dicts(new_indicadores)
                saved_count = store.indicadores_upsert(new_dicts)

                logger.info(
                    "new_data_saved",
                    produto=produto,
                    fetched=len(new_indicadores),
                    saved=saved_count,
                )

                # Merge com dados existentes
                existing_dates = {ind.data for ind in indicadores}
                for ind in new_indicadores:
                    if ind.data not in existing_dates:
                        indicadores.append(ind)

        except Exception as e:
            logger.warning(
                "source_fetch_failed",
                produto=produto,
                error=str(e),
            )
            # Continua com dados do histórico

    # 5. Validação estatística
    if validate_sanity and indicadores:
        indicadores, anomalies = await validate_batch(indicadores)

    # 6. Filtra por período e praça
    indicadores = [ind for ind in indicadores if inicio <= ind.data <= fim]

    if praca:
        indicadores = [
            ind for ind in indicadores if ind.praca and ind.praca.lower() == praca.lower()
        ]

    # 7. Converte para DataFrame
    df = _to_dataframe(indicadores)

    if as_polars:
        try:
            import polars as pl

            return pl.from_pandas(df)
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    return df


def _dicts_to_indicadores(dicts: list[dict]) -> list[Indicador]:
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


def _indicadores_to_dicts(indicadores: list[Indicador]) -> list[dict]:
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

    # Busca no histórico (últimos 30 dias)
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

    # Se não tem dados recentes ou não está offline, busca da fonte
    if not offline:
        has_recent = any(ind.data >= fim - timedelta(days=3) for ind in indicadores)

        if not has_recent:
            try:
                html = await client.fetch_indicador_page(produto)

                # Detecta fonte pelo conteúdo do HTML
                is_noticias_agricolas = "noticiasagricolas" in html.lower() or "cot-fisicas" in html

                if is_noticias_agricolas:
                    from agrobr.noticias_agricolas.parser import parse_indicador as na_parse

                    new_indicadores = na_parse(html, produto)
                else:
                    parser, new_indicadores = await get_parser_with_fallback(html, produto)

                if new_indicadores:
                    # Salva no histórico
                    new_dicts = _indicadores_to_dicts(new_indicadores)
                    store.indicadores_upsert(new_dicts)

                    # Merge
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
