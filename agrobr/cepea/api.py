from __future__ import annotations

import hashlib
import json
import time
import warnings
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, NamedTuple, overload

import duckdb
import httpx
import pandas as pd

from agrobr import _log, constants
from agrobr.cache.duckdb_store import get_store
from agrobr.cache.keys import build_cache_key
from agrobr.cache.policies import calculate_expiry
from agrobr.cepea import client, serie
from agrobr.cepea.parsers import v1
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.contracts import cepea as source_contracts
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    SourceUnavailableError,
    StaleDataWarning,
)
from agrobr.models import Indicador, MetaInfo
from agrobr.normalize import regions
from agrobr.noticias_agricolas import parser as na_parser
from agrobr.utils import validation
from agrobr.utils.result import DataFrame, DataFrameResult, finalize_result
from agrobr.utils.time import hoje, utcnow
from agrobr.utils.warnings import warn_once
from agrobr.validators.sanity import validate_batch

logger = _log.get_logger(__name__)

SOURCE_WINDOW_DAYS = 25

_LICENSE_WARNING = (
    "CEPEA/ESALQ: dados sob CC BY-NC 4.0; atribuição obrigatória e uso comercial sujeito a "
    "autorização expressa do CEPEA. Veja https://www.agrobr.dev/docs/licenses/."
)


def _warn_license() -> None:
    warn_once("cepea_license", _LICENSE_WARNING)


def _today() -> date:
    return hoje()


def _normalize_dates(
    inicio: str | date | None,
    fim: str | date | None,
) -> tuple[date, date]:
    if any(isinstance(value, datetime) and pd.isna(value) for value in (inicio, fim)):
        raise InvalidParameterError("Datas inválidas: inicio e fim não aceitam NaT")
    inicio, fim = validation.parse_data(inicio, "inicio"), validation.parse_data(fim, "fim")
    if fim is None:
        fim = _today()
    if inicio is None:
        inicio = fim - timedelta(days=365)
    if inicio > fim:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim (YYYY-MM-DD)")
    return inicio, fim


def _normalize_produto(produto: object) -> str:
    if not isinstance(produto, str):
        raise InvalidParameterError("produto deve ser uma string")
    normalized = "_".join(regions.remover_acentos(produto).lower().split())
    if normalized not in constants.CEPEA_PRODUTOS:
        raise InvalidParameterError(
            f"Produto inválido: {produto!r}. Opções: {sorted(constants.CEPEA_PRODUTOS)}"
        )
    return normalized


def _normalize_praca(produto: str, praca: object | None) -> str | None:
    if praca is None:
        return None
    if not isinstance(praca, str):
        raise InvalidParameterError("praca deve ser uma string")
    requested = regions.slugificar_praca(praca.strip())
    valid = _pracas(produto)
    if requested not in valid:
        raise InvalidParameterError(f"Praça inválida para {produto!r}: {praca!r}. Opções: {valid}")
    return requested


def _vencido(ultima_coleta: datetime | None) -> bool:
    return (
        ultima_coleta is not None
        and calculate_expiry(constants.Fonte.CEPEA, desde=ultima_coleta) <= utcnow()
    )


def _periodo_fechado(fim: date) -> bool:
    return fim < _today() - timedelta(days=SOURCE_WINDOW_DAYS)


def _needs_fetch(
    indicadores: list[Indicador],
    inicio: date,
    fim: date,
    force_refresh: bool,
    offline: bool,
    ultima_coleta: datetime | None = None,
) -> bool:
    if offline:
        return False
    if force_refresh:
        return True
    if _periodo_fechado(fim):
        return False
    if ultima_coleta is not None:
        return _vencido(ultima_coleta)
    recent_start = _today() - timedelta(days=SOURCE_WINDOW_DAYS)
    existing_dates = {ind.data for ind in indicadores}
    for i in range(min(SOURCE_WINDOW_DAYS, (fim - max(inicio, recent_start)).days + 1)):
        check_date = fim - timedelta(days=i)
        if check_date.weekday() < 5 and check_date not in existing_dates:
            return True
    return False


def _avisar(meta: MetaInfo, aviso: str) -> None:
    meta.validation_warnings.append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)


def _precisa_da_serie(
    store: Any, produto: str, inicio: date, fim: date, force_refresh: bool
) -> bool:
    """A série se baixa quando o período antes da janela recente não está coberto por ela.

    A cobertura é a última data que a série publicou, registrada no download (a página sobrescreve
    as linhas recentes e não serve de medida). Dentro da validade do download (a virada das 18h do
    CEPEA), a série não se baixa de novo; a folga cobre o fim de semana e o mês do leite.
    """
    janela = _today() - timedelta(days=SOURCE_WINDOW_DAYS)
    if produto not in constants.CEPEA_SERIES or inicio >= janela:
        return False
    if force_refresh:
        return True
    cobertura = store.serie_cobertura(produto)
    if cobertura is None:
        return True
    publicada_ate, baixada_em = cobertura
    folga = timedelta(days=62 if produto == "leite" else 7)
    return _vencido(baixada_em) and publicada_ate + folga < min(fim, janela)


async def _baixar_a_serie(
    store: Any, produto: str, conhecidos: list[Indicador]
) -> tuple[list[Indicador], list[dict[str, Any]]]:
    """Baixa a série inteira e grava no cache só o dia que ele ainda não tem.

    A linha do cache prevalece: no leite, a página publica 4 casas, e a série, 2. O que volta para
    a consulta se mede contra ``conhecidos`` (o que ela já leu), não contra o cache, que outra
    consulta simultânea pode ter preenchido depois da leitura.

    Returns:
        Os dias da série fora de ``conhecidos`` e os recursos baixados, para a proveniência.
    """
    indicadores: list[Indicador] = []
    pesos: dict[date, float] = {}
    recursos: list[dict[str, Any]] = []
    for pagina, identificador, papel in constants.CEPEA_SERIES[produto]:
        baixada = await client.fetch_serie(pagina, identificador)
        rotulo = "serie_peso" if papel == "peso" else "serie"
        recursos.append(
            {
                "papel": rotulo,
                "url": baixada.url,
                "sha256": hashlib.sha256(baixada.conteudo).hexdigest(),
                "bytes": len(baixada.conteudo),
                "fetched_at": baixada.fetched_at.replace(tzinfo=UTC).isoformat(),
            }
        )
        if papel == "peso":
            pesos = serie.parse_peso(baixada.conteudo)
        else:
            indicadores.extend(serie.parse_serie(baixada.conteudo, produto, papel))
    for ind in indicadores:
        if ind.data in pesos:
            ind.meta["peso_medio_kg"] = pesos[ind.data]
    vistos = {(ind.praca, ind.data) for ind in conhecidos}
    novos = [ind for ind in indicadores if (ind.praca, ind.data) not in vistos]
    try:
        existentes = {(linha["praca"], linha["data"]) for linha in store.indicadores_query(produto)}
        store.indicadores_upsert(
            _indicadores_to_dicts(
                [ind for ind in indicadores if (ind.praca, ind.data) not in existentes]
            )
        )
        if indicadores:
            store.serie_registrar(produto, max(ind.data for ind in indicadores), baixada.fetched_at)
    except duckdb.Error as e:
        logger.warning("cache_upsert_failed", produto=produto, error=str(e))
    logger.info(
        "cepea_serie_gravada", produto=produto, recebidos=len(indicadores), novos=len(novos)
    )
    return novos, recursos


def _carimbar_serie(meta: MetaInfo, recursos: list[dict[str, Any]], pagina_baixada: bool) -> None:
    """Proveniência: com 1 corpo, o topo é o dele; com vários, fica nulo.

    A hora do topo, nos 2 campos, é a aquisição mais recente entre os corpos.
    """
    if pagina_baixada:
        recursos = [
            *recursos,
            {
                "papel": "pagina",
                "url": meta.source_url,
                "sha256": meta.raw_content_hash,
                "bytes": meta.raw_content_size,
                "fetched_at": meta.fetch_timestamp.isoformat() if meta.fetch_timestamp else None,
            },
        ]
    meta.source_details["resources"] = recursos
    unico = recursos[0] if len(recursos) == 1 else None
    meta.raw_content_hash = unico["sha256"] if unico else None
    meta.raw_content_size = unico["bytes"] if unico else 0
    meta.source_url = recursos[0]["url"]
    meta.fetched_at = meta.fetch_timestamp = max(
        datetime.fromisoformat(recurso["fetched_at"]) for recurso in recursos
    )


def _warn_stale(message: str, meta: MetaInfo) -> None:
    warnings.warn(message, StaleDataWarning, stacklevel=3)
    meta.validation_warnings.append("stale_data: using cache after empty fetch")


def _set_cache_meta(meta: MetaInfo, *, fallback: bool = False) -> None:
    meta.from_cache = True
    meta.source = "cache_fallback" if fallback else "cache"
    meta.source_method = "duckdb"
    meta.source_url = ""
    meta.selected_source = "cache"
    meta.attempted_sources = (
        list(dict.fromkeys(meta.attempted_sources + ["cache"])) if fallback else ["cache"]
    )


def _registrar_versoes(meta: MetaInfo, indicadores: list[Indicador]) -> None:
    """Do cache, ``parser_version`` é a versão gravada nos registros (a maior, se houver mais de
    uma); versão diferente da informada vai por fonte em ``source_details["parser_versions"]``."""
    versoes: dict[str, set[int]] = {}
    for ind in indicadores:
        versoes.setdefault(ind.fonte.value, set()).add(ind.parser_version)
    todas = {versao for lista in versoes.values() for versao in lista}
    if meta.from_cache and todas:
        meta.parser_version = max(todas)
    if todas - {meta.parser_version}:
        meta.source_details["parser_versions"] = {
            fonte: sorted(lista) for fonte, lista in sorted(versoes.items())
        }


class _FetchResult(NamedTuple):
    indicadores: list[Indicador]
    source_name: str
    source_url: str
    parser_version: int
    raw_hash: str
    raw_size: int
    parse_ms: int
    avisos: tuple[str, ...] = ()


def _cepea_source_url(produto: str) -> str:
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    return f"{constants.URLS[constants.Fonte.CEPEA]['indicadores']}/{produto_key}.aspx"


def _noticias_agricolas_source_url(produto: str) -> str:
    produto_key = constants.NOTICIAS_AGRICOLAS_PRODUTOS.get(produto.lower(), produto.lower())
    return f"{constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]['cotacoes']}/{produto_key}"


async def _parse_fetch_result(produto: str, fetch_result: client.FetchResult) -> _FetchResult:
    parse_start = time.perf_counter()
    html = fetch_result.html
    source_name = fetch_result.source
    raw_size = len(html.encode("utf-8"))
    raw_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
    avisos: list[str] = []

    if source_name == "noticias_agricolas":
        new_indicadores = na_parser.parse_indicador(html, produto)
        source_url = _noticias_agricolas_source_url(produto)
        parser_version = na_parser.PARSER_VERSION
        logger.info(
            "parse_success",
            source="noticias_agricolas",
            records_count=len(new_indicadores),
        )
    else:
        parser, new_indicadores = await get_parser_with_fallback(html, produto, avisos=avisos)
        source_url = _cepea_source_url(produto)
        parser_version = parser.version

    if not new_indicadores:
        raise ParseError(
            source=source_name, parser_version=parser_version, reason="Nenhum indicador extraído"
        )

    parse_ms = int((time.perf_counter() - parse_start) * 1000)
    return _FetchResult(
        indicadores=new_indicadores,
        source_name=source_name,
        source_url=source_url,
        parser_version=parser_version,
        raw_hash=raw_hash,
        raw_size=raw_size,
        parse_ms=parse_ms,
        avisos=tuple(avisos),
    )


async def _fetch_and_parse(produto: str) -> _FetchResult:
    fetched = await client.fetch_indicador_page(produto)
    try:
        return await _parse_fetch_result(produto, fetched)
    except ParseError as primary_error:
        if fetched.source == "noticias_agricolas" or not client.can_use_alternative_source(produto):
            raise
        logger.warning("cepea_content_fallback", produto=produto)
        try:
            alternative = await client.fetch_indicador_page(produto, force_alternative=True)
        except (httpx.HTTPError, SourceUnavailableError) as alternative_error:
            raise ParseError(
                source="cepea",
                parser_version=primary_error.parser_version,
                reason=f"Página CEPEA sem dados reconhecidos; fallback indisponível: {alternative_error}",
                attempted_sources=["cepea", "noticias_agricolas"],
            ) from alternative_error
        return await _parse_fetch_result(produto, alternative)


def _observation_key(ind: Indicador) -> tuple[date, str, str]:
    return ind.data, ind.produto, regions.slugificar_praca(ind.praca) if ind.praca else ""


def _source_priority(source: str) -> int:
    return {"cepea": 0, "noticias_agricolas": 1}.get(source, 2)


def _revision_order(ind: Indicador) -> tuple[datetime, int, int, str]:
    parsed_at = ind.parsed_at
    timestamp = (
        parsed_at.replace(tzinfo=UTC) if parsed_at.tzinfo is None else parsed_at.astimezone(UTC)
    )
    return (
        timestamp,
        ind.revisao,
        ind.parser_version,
        json.dumps(ind.model_dump(mode="json"), sort_keys=True, ensure_ascii=False),
    )


def _merge_indicadores(cached: list[Indicador], fetched: list[Indicador]) -> list[Indicador]:
    records = {
        (*_observation_key(ind), ind.fonte): ind for ind in sorted(cached, key=_revision_order)
    }
    records.update(
        {(*_observation_key(ind), ind.fonte): ind for ind in sorted(fetched, key=_revision_order)}
    )
    return list(records.values())


def _select_indicadores(indicadores: list[Indicador]) -> list[Indicador]:
    records: dict[tuple[date, str, str], Indicador] = {}
    for ind in sorted(
        _merge_indicadores(indicadores, []),
        key=lambda ind: (_source_priority(ind.fonte.value), ind.fonte.value),
    ):
        records.setdefault(_observation_key(ind), ind)
    return [records[key] for key in sorted(records)]


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: Literal[False] = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: Literal[False] = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Série histórica de indicadores CEPEA/ESALQ.

    Busca dados do cache DuckDB e, se necessário, faz fetch na fonte
    (CEPEA ou Notícias Agrícolas como fallback).
    A página do indicador publica uma janela recente, normalmente cerca de 15 pregões.
    Período anterior a ela vem da série histórica do CEPEA, baixada inteira e gravada
    no cache na primeira vez; laranja não tem série. Etanol é semanal. Para leite,
    ``data`` é o primeiro dia do mês de referência, não a data de publicação, e
    ``praca`` é a UF.

    Args:
        produto: Código do produto (ex: "soja", "milho", "boi_gordo").
        praca: Praça de cotação. Aceita slug de ``pracas()`` ou rótulo da fonte.
            ``None`` retorna todas.
        inicio: Data inicial (``date``, ``datetime`` ou texto ``AAAA-MM-DD`` ou ``DD/MM/AAAA``).
            A hora é descartada. Default: 365 dias antes de ``fim``.
        fim: Data final, nos mesmos formatos de ``inicio``. Default: hoje.
        as_polars: Retorna ``polars.DataFrame`` em vez de pandas.
        validate_sanity: Confere unidade, faixa e variação temporal quando houver regra.
        force_refresh: Ignora cache e força fetch na fonte.
        offline: Usa apenas cache local, sem requests HTTP.
        return_meta: Retorna tupla ``(df, MetaInfo)`` com metadados de proveniência.
    """
    produto = _normalize_produto(produto)
    praca = _normalize_praca(produto, praca)
    inicio, fim = _normalize_dates(inicio, fim)
    _warn_license()
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="unknown",
        source_url="",
        source_method="unknown",
        fetched_at=utcnow(),
        schema_version=source_contracts.CEPEA_INDICADOR_V1.version,
    )
    store = get_store()
    indicadores: list[Indicador] = []
    ultima_coleta = None

    if not force_refresh:
        try:
            cached_data = store.indicadores_query(
                produto=produto,
                inicio=datetime.combine(inicio, datetime.min.time()),
                fim=datetime.combine(fim, datetime.max.time()),
                praca=praca,
            )
            ultima_coleta = store.indicadores_ultima_coleta(produto)
        except duckdb.Error as e:
            logger.warning("cache_query_failed", produto=produto, error=str(e))
            cached_data = []

        indicadores = _dicts_to_indicadores(cached_data)

        _set_cache_meta(meta)

        logger.info(
            "history_query",
            produto=produto,
            inicio=inicio,
            fim=fim,
            cached_count=len(indicadores),
        )

    recursos: list[dict[str, Any]] = []
    serie_baixada: list[Indicador] = []
    pagina_baixada = False
    erro_da_serie: SourceUnavailableError | ParseError | None = None
    if not offline and _precisa_da_serie(store, produto, inicio, fim, force_refresh):
        try:
            novos, recursos = await _baixar_a_serie(store, produto, indicadores)
        except (SourceUnavailableError, ParseError) as e:
            erro_da_serie = e
            _avisar(
                meta,
                f"cepea: série histórica de {produto!r} indisponível ({e}); o resultado traz só o "
                "que a página e o cache tinham no período.",
            )
        else:
            serie_baixada = novos
            indicadores = _merge_indicadores(
                indicadores, [ind for ind in novos if inicio <= ind.data <= fim]
            )
            meta.source = meta.selected_source = "cepea"
            meta.source_method = "httpx+xls"
            meta.attempted_sources = ["cepea"]
            meta.from_cache = False
            meta.parser_version = constants.CEPEA_SERIE_PARSER_VERSION
    elif (
        not offline
        and produto not in constants.CEPEA_SERIES
        and inicio < _today() - timedelta(days=SOURCE_WINDOW_DAYS)
    ):
        _avisar(
            meta,
            f"cepea: o CEPEA não publica série histórica de {produto!r}; antes da janela recente, "
            "o resultado traz só o que o cache acumulou.",
        )

    if _needs_fetch(indicadores, inicio, fim, force_refresh, offline, ultima_coleta):
        logger.info("fetching_from_source", produto=produto)
        meta.attempted_sources = ["cepea"]

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
            if result.source_name == "noticias_agricolas":
                meta.validation_warnings.append(
                    "source_fetch_failed: a página do CEPEA não respondeu ou veio sem dados "
                    "reconhecidos; os dados são da Notícias Agrícolas"
                )
            meta.source_method = "httpx"
            meta.parse_duration_ms = result.parse_ms
            meta.source_url = result.source_url
            meta.raw_content_hash = result.raw_hash
            meta.raw_content_size = result.raw_size
            meta.fetch_timestamp = utcnow()
            meta.parser_version = result.parser_version
            meta.from_cache = False
            for aviso in result.avisos:
                _avisar(meta, aviso)
            pagina_baixada = True

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

                indicadores = _merge_indicadores(indicadores, result.indicadores)

        except (httpx.HTTPError, SourceUnavailableError, ParseError, OSError) as e:
            logger.warning(
                "source_fetch_failed",
                produto=produto,
                error=str(e),
            )
            meta.validation_warnings.append(f"source_fetch_failed: {e}")
            if isinstance(e, (SourceUnavailableError, ParseError)):
                meta.attempted_sources = list(
                    dict.fromkeys(meta.attempted_sources + e.attempted_sources)
                )
            if not indicadores:
                cached_fallback = store.indicadores_query(
                    produto=produto,
                    inicio=datetime.combine(inicio, datetime.min.time()),
                    fim=datetime.combine(fim, datetime.max.time()),
                    praca=praca,
                )
                if cached_fallback:
                    indicadores = _dicts_to_indicadores(cached_fallback)
            if indicadores:
                _set_cache_meta(meta, fallback=True)
                _warn_stale(
                    f"Fresh fetch failed or returned no data for '{produto}'. Using stale cache ({len(indicadores)} records).",
                    meta,
                )
            if not indicadores:
                if isinstance(e, ParseError):
                    raise
                raise SourceUnavailableError(
                    source="cepea",
                    last_error=str(e),
                    attempted_sources=list(dict.fromkeys(meta.attempted_sources + ["cache"])),
                ) from e
    elif not indicadores and not offline:
        _avisar(
            meta, f"cepea: sem dado de {produto!r} entre {inicio} e {fim} na fonte nem no cache."
        )

    indicadores = _select_indicadores(indicadores)

    if validate_sanity and indicadores:
        indicadores, anomalies = await validate_batch(indicadores)

    indicadores = [ind for ind in indicadores if inicio <= ind.data <= fim]

    if praca:
        praca_slug = regions.slugificar_praca(praca)
        indicadores = [
            ind
            for ind in indicadores
            if ind.praca and regions.slugificar_praca(ind.praca) == praca_slug
        ]

    if validate_sanity:
        marcadas = [ind for ind in indicadores if ind.anomalies]
        if marcadas:
            regras = sorted({anomalia for ind in marcadas for anomalia in ind.anomalies})
            _avisar(
                meta,
                f"cepea: a sanidade marcou {len(marcadas)} de {len(indicadores)} linhas "
                f"({'; '.join(regras)}); veja a coluna anomalies",
            )

    indicadores = _marcar_valor_mantido(store, produto, indicadores, serie_baixada)

    if erro_da_serie is not None and not indicadores:
        if isinstance(erro_da_serie, ParseError):
            raise erro_da_serie
        raise SourceUnavailableError(
            source="cepea",
            last_error=str(erro_da_serie),
            attempted_sources=list(dict.fromkeys(["cepea", *meta.attempted_sources, "cache"])),
        ) from erro_da_serie

    df = _to_dataframe(indicadores)
    meta.data_sources = sorted({ind.fonte.value for ind in indicadores})
    _registrar_versoes(meta, indicadores)
    if recursos:
        _carimbar_serie(meta, recursos, pagina_baixada)
    da_pagina = [
        ind.data
        for ind in indicadores
        if ind.parser_version != constants.CEPEA_SERIE_PARSER_VERSION
    ]
    if produto == "leite" and da_pagina and len(da_pagina) < len(indicadores):
        meta.source_details["pagina_desde"] = min(da_pagina).isoformat()

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "cepea",
        {"produto": produto, "praca": praca or "all"},
        schema_version=meta.schema_version,
    )
    if meta.from_cache and indicadores:
        meta.fetched_at = max(ind.parsed_at for ind in indicadores)
        meta.fetch_timestamp = meta.fetched_at
    meta.from_cache = meta.from_cache and bool(indicadores)
    meta.cache_expires_at = (
        calculate_expiry(constants.Fonte.CEPEA, desde=meta.fetched_at)
        if indicadores and not _periodo_fechado(fim)
        else None
    )

    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=("produto", "praca", "unidade", "fonte", "metodologia", "anomalies"),
    )


_META_COLUMNS = ("valor_usd", "peso_medio_kg")


def _marcar_valor_mantido(
    store: Any, produto: str, indicadores: list[Indicador], serie_baixada: list[Indicador]
) -> list[Indicador]:
    """Marca ``valor_mantido`` no pregão que repete o valor do pregão anterior da mesma série, até a data de
    ``constants.CEPEA_VALOR_MANTIDO``: sem negócio elegível, o CEPEA mantinha o valor do dia anterior.

    O pregão anterior vem da série inteira, e não só da janela pedida: da série baixada nesta consulta, antes do
    recorte, e do cache como complemento. Assim o 1º dia da janela também é marcado quando repete o dia anterior a
    ela, com ou sem o cache.
    """
    regra = constants.CEPEA_VALOR_MANTIDO.get(produto)
    if regra is None:
        return indicadores
    praca, corte = regra
    slug = regions.slugificar_praca(praca)

    def na_regra(ind: Indicador) -> bool:
        return (
            ind.data < corte
            and ind.praca is not None
            and regions.slugificar_praca(ind.praca) == slug
        )

    alvo = [ind for ind in indicadores if na_regra(ind)]
    if not alvo:
        return indicadores
    guardadas = _dicts_to_indicadores(
        store.indicadores_query(produto=produto, fim=datetime.combine(corte, datetime.min.time()))
    )
    serie_da_praca = {
        ind.data: ind.valor
        for ind in _select_indicadores([*guardadas, *serie_baixada])
        if na_regra(ind)
    }
    serie_da_praca.update({ind.data: ind.valor for ind in alvo})
    datas = sorted(serie_da_praca)
    mantidas = {
        dia
        for anterior, dia in zip(datas, datas[1:])
        if serie_da_praca[dia] == serie_da_praca[anterior]
    }
    return [
        ind.model_copy(update={"anomalies": [*ind.anomalies, "valor_mantido"]})
        if na_regra(ind) and ind.data in mantidas
        else ind
        for ind in indicadores
    ]


def _cached_meta(d: dict[str, Any]) -> dict[str, Any]:
    return {key: float(d[key]) for key in _META_COLUMNS if d.get(key) is not None}


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
                parsed_at=d.get("collected_at") or utcnow(),
                meta=_cached_meta(d),
                anomalies=list(d.get("anomalies") or []),
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
            "valor_usd": ind.meta.get("valor_usd"),
            "peso_medio_kg": ind.meta.get("peso_medio_kg"),
            "anomalies": ind.anomalies,
        }
        for ind in sorted(indicadores, key=_revision_order)
    ]


async def produtos() -> list[str]:
    return list(constants.CEPEA_PRODUTOS.keys())


def _pracas(produto: str) -> list[str]:
    if produto in constants.CEPEA_PRACAS_REGIONAIS:
        return [
            regions.slugificar_praca(label) for label in constants.CEPEA_PRACAS_REGIONAIS[produto]
        ]
    praca = v1.PRACAS.get(produto)
    return [regions.slugificar_praca(praca)] if praca else []


async def pracas(produto: str) -> list[str]:
    return _pracas(_normalize_produto(produto))


async def ultimo(produto: str, praca: str | None = None, offline: bool = False) -> Indicador:
    """Último indicador disponível; leite usa o mês de referência e pode ter defasagem mensal."""
    produto = _normalize_produto(produto)
    if produto == "leite" and praca is None:
        praca = "BRASIL"
    praca = _normalize_praca(produto, praca)
    _warn_license()
    store = get_store()
    indicadores: list[Indicador] = []

    fim = _today()
    inicio = fim - timedelta(days=365 if produto == "leite" else 30)

    cached_data = store.indicadores_query(
        produto=produto,
        inicio=datetime.combine(inicio, datetime.min.time()),
        fim=datetime.combine(fim, datetime.max.time()),
        praca=praca,
    )

    if cached_data:
        indicadores = _dicts_to_indicadores(cached_data)

    erro_de_rede: Exception | None = None
    busca_falhou = False
    if not offline:
        freshness = (
            (fim.replace(day=1) - timedelta(days=32)).replace(day=1)
            if produto == "leite"
            else fim - timedelta(days=3)
        )
        has_recent = any(ind.data >= freshness for ind in indicadores)

        if not has_recent or _vencido(store.indicadores_ultima_coleta(produto)):
            try:
                result = await _fetch_and_parse(produto)
                new_indicadores = result.indicadores

                if new_indicadores:
                    new_dicts = _indicadores_to_dicts(new_indicadores)
                    store.indicadores_upsert(new_dicts)

                    indicadores = _merge_indicadores(indicadores, new_indicadores)

            except (httpx.HTTPError, SourceUnavailableError, ParseError, OSError) as e:
                logger.warning("source_fetch_failed", produto=produto, error=str(e))
                busca_falhou = True
                if not isinstance(e, ParseError):
                    erro_de_rede = e

    if praca:
        praca_slug = regions.slugificar_praca(praca)
        indicadores = [
            ind
            for ind in indicadores
            if ind.praca and regions.slugificar_praca(ind.praca) == praca_slug
        ]

    if not indicadores and offline:
        raise SourceUnavailableError(
            source="cepea", last_error="offline sem dado no cache", attempted_sources=["cache"]
        )
    if not indicadores and erro_de_rede is not None:
        raise SourceUnavailableError(
            source="cepea",
            last_error=str(erro_de_rede),
            attempted_sources=getattr(erro_de_rede, "attempted_sources", ["cepea"]) + ["cache"],
        ) from erro_de_rede
    if not indicadores:
        raise ParseError(
            source="cepea",
            parser_version=constants.CEPEA_PARSER_VERSION,
            reason=f"No indicators found for {produto}",
        )

    if busca_falhou:
        warnings.warn(
            f"Fresh fetch failed or returned no data for '{produto}'. "
            f"Using stale cache ({len(indicadores)} records).",
            StaleDataWarning,
            stacklevel=2,
        )
    indicadores = _select_indicadores(indicadores)
    indicadores.sort(key=lambda x: x.data, reverse=True)
    return indicadores[0]


def _to_dataframe(indicadores: list[Indicador]) -> pd.DataFrame:
    empty = source_contracts.CEPEA_INDICADOR_V1.empty_frame()
    empty["anomalies"] = pd.Series(dtype=object)
    if not indicadores:
        return empty

    data = [
        {
            "data": ind.data,
            "produto": ind.produto,
            "praca": ind.praca,
            "valor": float(ind.valor),
            "unidade": ind.unidade,
            "fonte": ind.fonte.value,
            "metodologia": ind.metodologia,
            "anomalies": json.dumps(ind.anomalies, ensure_ascii=False) if ind.anomalies else None,
            "valor_usd": ind.meta.get("valor_usd"),
            "peso_medio_kg": ind.meta.get("peso_medio_kg"),
        }
        for ind in indicadores
    ]

    df = pd.DataFrame(data)
    df["anomalies"] = pd.Series([row["anomalies"] for row in data], dtype=object)
    df[list(_META_COLUMNS)] = df[list(_META_COLUMNS)].astype("float64")
    df["data"] = pd.to_datetime(df["data"])
    df = df.astype(empty.dtypes.to_dict())
    df = df.sort_values("data").reset_index(drop=True)

    return df
