from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import re
import time
import warnings
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.normalize.regions import remover_acentos
from agrobr.utils import tasks
from agrobr.utils import time as time_utils
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf, validate_year_uf

from . import client, parser
from .models import (
    AGREGACAO_MENSAL,
    AGREGACAO_SEMANAL,
    AGREGACOES_VALIDAS,
    NIVEIS_VALIDOS,
    NIVEL_BRASIL,
    NIVEL_MUNICIPIO,
    NIVEL_UF,
    PRECOS_BRASIL_URL,
    PRECOS_ESTADOS_URL,
    PRECOS_MUNICIPIOS_URLS,
    PRODUTOS_DIESEL,
    VENDAS_DIESEL_CSV_URL,
    _resolve_periodo_municipio,
    normalize_produto,
)

logger = _log.get_logger(__name__)


def validate_output_options(*, as_polars: bool, return_meta: bool) -> None:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError("Instale agrobr[polars] para usar as_polars=True") from None


def _normalize_price_date(value: str | date | None) -> date | None:
    if value is None:
        return value
    if type(value) is date and date(1678, 1, 1) <= value <= date(2261, 12, 31):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            parsed = date.fromisoformat(value)
            if date(1678, 1, 1) <= parsed <= date(2261, 12, 31):
                return parsed
        except ValueError:
            pass
    raise InvalidParameterError("inicio e fim devem ser datas ou strings YYYY-MM-DD")


def normalize_price_query(
    uf: str | None,
    municipio: str | None,
    produto: str,
    inicio: str | date | None,
    fim: str | date | None,
    agregacao: str,
    nivel: str,
) -> dict[str, Any]:
    if not isinstance(agregacao, str) or agregacao not in AGREGACOES_VALIDAS:
        raise InvalidParameterError(
            f"Agregacao {agregacao!r} invalida: {sorted(AGREGACOES_VALIDAS)}"
        )
    if not isinstance(nivel, str) or nivel not in NIVEIS_VALIDOS:
        raise InvalidParameterError(f"Nivel {nivel!r} invalido: {sorted(NIVEIS_VALIDOS)}")
    if not isinstance(produto, str) or normalize_produto(produto) not in PRODUTOS_DIESEL:
        raise InvalidParameterError(f"Produto {produto!r} invalido: DIESEL ou DIESEL S10")
    for name, value in (("uf", uf), ("municipio", municipio)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise InvalidParameterError(f"{name} deve ser texto não vazio ou None")
    if nivel == NIVEL_BRASIL and uf is not None:
        raise InvalidParameterError("uf exige nivel='uf' ou nivel='municipio'")
    if nivel != NIVEL_MUNICIPIO and municipio is not None:
        raise InvalidParameterError("municipio exige nivel='municipio'")
    start, end = _normalize_price_date(inicio), _normalize_price_date(fim)
    if start is not None and end is not None and start > end:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")
    return {
        "uf": validate_uf(uf),
        "municipio": " ".join(remover_acentos(municipio).upper().split()) if municipio else None,
        "produto": normalize_produto(produto),
        "inicio": start,
        "fim": end,
        "agregacao": agregacao,
        "nivel": nivel,
    }


def _normalize_range(
    inicio: str | date | None,
    fim: str | date | None,
) -> tuple[date | None, date | None]:
    try:
        start = date.fromisoformat(inicio) if isinstance(inicio, str) else inicio
        end = date.fromisoformat(fim) if isinstance(fim, str) else fim
    except ValueError as exc:
        raise InvalidParameterError("inicio e fim devem usar o formato YYYY-MM-DD") from exc
    if start is not None and not isinstance(start, date):
        raise InvalidParameterError("inicio e fim devem ser datas ou strings YYYY-MM-DD")
    if end is not None and not isinstance(end, date):
        raise InvalidParameterError("inicio e fim devem ser datas ou strings YYYY-MM-DD")
    if start is not None and end is not None and start > end:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")
    return start, end


@overload
async def precos_diesel(
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = AGREGACAO_SEMANAL,
    nivel: str = NIVEL_MUNICIPIO,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def precos_diesel(
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = AGREGACAO_SEMANAL,
    nivel: str = NIVEL_MUNICIPIO,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


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
    validate_output_options(as_polars=as_polars, return_meta=return_meta)
    df, meta = await acquire_prices(
        uf=uf,
        municipio=municipio,
        produto=produto,
        inicio=inicio,
        fim=fim,
        agregacao=agregacao,
        nivel=nivel,
    )
    if agregacao == AGREGACAO_MENSAL:
        contracts.validate_dataset(df, "anp_diesel_precos")
    meta.validation_passed = True
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def acquire_prices(
    *,
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = AGREGACAO_SEMANAL,
    nivel: str = NIVEL_MUNICIPIO,
) -> tuple[pd.DataFrame, MetaInfo]:
    """Adquire semanas validadas e deriva a tabela para validação final pelo consumidor."""
    query = normalize_price_query(uf, municipio, produto, inicio, fim, agregacao, nivel)
    t0 = time.monotonic()
    urls = await _resolve_price_urls(query)
    resources = await tasks.gather_or_cancel(*(client.fetch_precos_resource(url) for url in urls))
    fetch_ms = int((time.monotonic() - t0) * 1000)
    t1 = time.monotonic()
    frames = await asyncio.gather(
        *(
            asyncio.to_thread(
                parser.parse_precos,
                resource.content,
                produto=query["produto"],
                uf=query["uf"],
                municipio=query["municipio"],
                nivel=nivel,
            )
            for resource in resources
        )
    )
    weekly = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["data", "uf", "municipio", "produto"], kind="stable")
        .reset_index(drop=True)
    )
    contracts.validate_dataset(weekly, "anp_diesel_precos")
    selected = _select_period(weekly, query)
    df = parser.agregar_mensal(selected) if agregacao == AGREGACAO_MENSAL else selected
    parse_ms = int((time.monotonic() - t1) * 1000)
    meta = _price_meta(df, weekly, query, resources, frames, fetch_ms, parse_ms)
    for aviso in dict.fromkeys(
        aviso for parsed in frames for aviso in parsed.attrs.get("avisos", [])
    ):
        meta.validation_warnings.append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)
    meta.validation_passed = agregacao == AGREGACAO_SEMANAL
    return df, meta


@overload
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf)
    inicio, fim = _normalize_range(inicio, fim)

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


def _periodos_municipios(
    inicio: date | None,
    fim: date | None,
    catalog: dict[str, str] | None = None,
) -> list[str]:
    catalog = PRECOS_MUNICIPIOS_URLS if catalog is None else catalog
    if not inicio and not fim:
        return list(catalog)

    catalog_years = [int(year) for key in catalog for year in key.split("-")]
    if any(
        boundary is not None and _resolve_periodo_municipio(boundary.year, catalog) is None
        for boundary in (inicio, fim)
    ):
        raise SourceUnavailableError(
            source="anp_diesel",
            url=constants.URLS[constants.Fonte.ANP_DIESEL]["precos_catalogo"],
            last_error=f"Período fora do catálogo municipal disponível: {sorted(catalog)}",
        )
    ano_inicio = inicio.year if inicio else min(catalog_years)
    ano_fim = fim.year if fim else max(catalog_years)

    periodos: list[str] = []
    for ano in range(ano_inicio, ano_fim + 1):
        p = _resolve_periodo_municipio(ano, catalog)
        if p is None:
            raise SourceUnavailableError(
                source="anp_diesel",
                url=constants.URLS[constants.Fonte.ANP_DIESEL]["precos_catalogo"],
                last_error=f"Ano {ano} fora do catálogo municipal disponível: {sorted(catalog)}",
            )
        if p not in periodos:
            periodos.append(p)
    if fim is not None:
        boundary_year = (fim + timedelta(days=6)).year
        adjacent = _resolve_periodo_municipio(boundary_year, catalog)
        if adjacent is not None and adjacent not in periodos:
            periodos.append(adjacent)
    return periodos


def _price_urls(query: dict[str, Any], catalog: dict[str, str] | None = None) -> list[str]:
    catalog = PRECOS_MUNICIPIOS_URLS if catalog is None else catalog
    if query["nivel"] == NIVEL_MUNICIPIO:
        return [
            catalog[period]
            for period in _periodos_municipios(query["inicio"], query["fim"], catalog)
        ]
    return [PRECOS_ESTADOS_URL if query["nivel"] == NIVEL_UF else PRECOS_BRASIL_URL]


async def _resolve_price_urls(query: dict[str, Any]) -> list[str]:
    if query["nivel"] != NIVEL_MUNICIPIO:
        return _price_urls(query)
    current_year = time_utils.utcnow().year
    for boundary in (query["inicio"], query["fim"]):
        if boundary is not None and not 2022 <= boundary.year <= current_year:
            raise SourceUnavailableError(
                source="anp_diesel",
                url=constants.URLS[constants.Fonte.ANP_DIESEL]["precos_catalogo"],
                last_error=f"Período fora do catálogo municipal disponível: 2022 a {current_year}",
            )
    final_year = (
        min((query["fim"] + timedelta(days=6)).year, current_year) if query["fim"] else current_year
    )
    known_year = max(int(year) for key in PRECOS_MUNICIPIOS_URLS for year in key.split("-"))
    if final_year > known_year:
        return _price_urls(query, await client.fetch_precos_catalog())
    return _price_urls(query)


def _select_period(frame: pd.DataFrame, query: dict[str, Any]) -> pd.DataFrame:
    if query["inicio"] is not None:
        frame = frame[frame["data"] >= pd.Timestamp(query["inicio"])]
    if query["fim"] is not None:
        frame = frame[frame["data"] <= pd.Timestamp(query["fim"])]
    if frame.duplicated(["data", "nivel", "uf", "municipio", "produto"]).any():
        raise ParseError(
            source="anp_diesel",
            parser_version=parser.PARSER_VERSION,
            reason="Semanas duplicadas na selecao: publicacoes sobrepostas ou ambiguas",
        )
    return frame.reset_index(drop=True)


def _coverage(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "periodo_inicio": frame["periodo_inicio"].min().date().isoformat() if len(frame) else None,
        "periodo_fim": frame["periodo_fim"].max().date().isoformat() if len(frame) else None,
        "rows": len(frame),
    }


def _price_meta(
    frame: pd.DataFrame,
    weekly: pd.DataFrame,
    query: dict[str, Any],
    resources: list[client.PrecosResource],
    frames: list[pd.DataFrame],
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    receipts = [
        {
            **resource.receipt(),
            "layout_fingerprint": parsed.attrs.get("layout_fingerprint"),
            "selected_weekly_rows_before_date_filter": len(parsed),
        }
        for resource, parsed in zip(resources, frames, strict=True)
    ]
    manifest = json.dumps(receipts, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    raw_hash = (
        receipts[0]["sha256"]
        if len(receipts) == 1
        else hashlib.sha256(manifest.encode()).hexdigest()
    )
    meta = build_source_meta(
        "anp_diesel",
        resources[0].url,
        "httpx",
        fetch_ms,
        parse_ms,
        frame,
        parser.PARSER_VERSION,
        schema_version="2.0",
        raw_content_hash=raw_hash,
        source_details={
            "resources": receipts,
            "raw_hash_kind": "body_sha256" if len(receipts) == 1 else "resource_manifest_sha256",
            "resource_manifest": manifest if len(receipts) > 1 else None,
            "filters": {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in query.items()
            },
            "unit": "BRL/litro",
            "temporal_filter": "inclusive_by_week_start; exact_municipality_after_accent_and_space_normalization",
            "observed_coverage_before_date_filter": _coverage(weekly),
            "selected_coverage": _coverage(frame),
            "weekly_prices": "ANP survey means; coverage and sampled outlets may vary between weeks",
            "monthly_method": "arithmetic_mean_of_available_weekly_means; month_of_week_start; no_day_proration; no_outlet_weighting"
            if query["agregacao"] == AGREGACAO_MENSAL
            else None,
            "outlets": "weekly published count; monthly n_postos is null and n_postos_media is mean count, not unique outlets",
            "margin": "derived_resale_mean_minus_distribution_mean; not_net_profit",
            "complete_period": None,
            "coverage_guarantee": "observed_selected_weeks_only",
            "duplicate_weekly_rows_before_date_filter": int(
                weekly.duplicated(["data", "nivel", "uf", "municipio", "produto"], keep=False).sum()
            ),
            "edition": None,
            "temporal_scope": "current_workbooks_with_historical_observations; no_selectable_publication_edition",
        },
    )
    meta.fetched_at = max(resource.fetched_at for resource in resources)
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.raw_content_size = sum(len(resource.content) for resource in resources)
    return meta
