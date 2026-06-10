from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any, Literal, overload

import httpx
import pandas as pd
import structlog

from agrobr.exceptions import SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_year_uf

from . import client, parser
from .models import (
    MAX_FEATURES_WARNING,
    STATUS_VALIDOS,
    TIPO_VALIDOS,
    WFS_BASE,
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


def _build_cql_filter(
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
) -> str | None:
    parts: list[str] = []

    if cod_municipio is not None:
        parts.append(f"cod_municipio_ibge={cod_municipio}")
    elif municipio:
        escaped = municipio.replace("'", "''").replace("%", r"\%").replace("_", r"\_")
        parts.append(f"municipio ILIKE '%{escaped}%'")

    if status:
        parts.append(f"status_imovel='{status.upper()}'")

    if tipo:
        parts.append(f"tipo_imovel='{tipo.upper()}'")

    if area_min is not None:
        parts.append(f"area>={area_min}")

    if area_max is not None:
        parts.append(f"area<={area_max}")

    if criado_apos:
        if not _DATE_RE.match(criado_apos):
            raise ValueError(f"criado_apos invalido (esperado YYYY-MM-DD): {criado_apos!r}")
        parts.append(f"dat_criacao>='{criado_apos}'")

    return " AND ".join(parts) if parts else None


@overload
async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf)
    uf_upper = uf.strip().upper()

    if municipio is not None and cod_municipio is not None:
        raise ValueError("Use 'municipio' ou 'cod_municipio', nao ambos")

    if status is not None and status.upper() not in STATUS_VALIDOS:
        raise ValueError(f"Status '{status}' invalido. Opcoes: {sorted(STATUS_VALIDOS)}")

    if tipo is not None and tipo.upper() not in TIPO_VALIDOS:
        raise ValueError(f"Tipo '{tipo}' invalido. Opcoes: {sorted(TIPO_VALIDOS)}")

    logger.info(
        "sicar_imoveis",
        uf=uf_upper,
        municipio=municipio,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
    )

    cql = _build_cql_filter(
        municipio=municipio,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
    )

    if municipio is None and cod_municipio is None:
        try:
            total = await client.fetch_hits(uf_upper, cql)
            if total > MAX_FEATURES_WARNING:
                logger.warning(
                    "sicar_large_query",
                    uf=uf_upper,
                    total=total,
                    threshold=MAX_FEATURES_WARNING,
                    hint="Considere filtrar por municipio para reduzir volume",
                )
        except (httpx.HTTPError, SourceUnavailableError):
            logger.warning("sicar_hit_count_check_failed", uf=uf_upper, exc_info=True)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_imoveis(uf_upper, cql)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_imoveis_csv(pages)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if not df.empty and "cod_imovel" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["cod_imovel"], keep="first")
        df = df.sort_values("cod_imovel").reset_index(drop=True)
        if len(df) < before:
            logger.info("sicar_dedup", removed=before - len(df), remaining=len(df))

    meta = build_source_meta(
        "sicar",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["sicar_wfs"],
        selected_source="sicar_wfs",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def imoveis_geo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    max_features: int | None = 5000,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def imoveis_geo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    max_features: int | None = 5000,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def imoveis_geo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    max_features: int | None = 5000,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    validate_year_uf(uf=uf)
    uf_upper = uf.strip().upper()

    if municipio is not None and cod_municipio is not None:
        raise ValueError("Use 'municipio' ou 'cod_municipio', nao ambos")

    if status is not None and status.upper() not in STATUS_VALIDOS:
        raise ValueError(f"Status '{status}' invalido. Opcoes: {sorted(STATUS_VALIDOS)}")

    if tipo is not None and tipo.upper() not in TIPO_VALIDOS:
        raise ValueError(f"Tipo '{tipo}' invalido. Opcoes: {sorted(TIPO_VALIDOS)}")

    logger.info(
        "sicar_imoveis_geo",
        uf=uf_upper,
        municipio=municipio,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
    )

    cql = _build_cql_filter(
        municipio=municipio,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
    )

    if municipio is None and cod_municipio is None:
        try:
            async with client.make_session() as http:
                total = await client.fetch_hits(uf_upper, cql, client=http)
            effective = total if max_features is None else min(total, max_features)
            if effective > MAX_FEATURES_WARNING:
                logger.warning(
                    "sicar_geo_large_query",
                    uf=uf_upper,
                    total=total,
                    max_features=max_features,
                    threshold=MAX_FEATURES_WARNING,
                    hint="Considere definir max_features ou filtrar por municipio para reduzir volume",
                )
        except (httpx.HTTPError, SourceUnavailableError):
            logger.warning("sicar_geo_hit_count_check_failed", uf=uf_upper, exc_info=True)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_imoveis_geo(uf_upper, cql, max_features=max_features)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_imoveis_geojson(pages, max_features=max_features)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if not gdf.empty and "cod_imovel" in gdf.columns:
        before = len(gdf)
        gdf = gdf.drop_duplicates(subset=["cod_imovel"], keep="first")
        gdf = gdf.sort_values("cod_imovel").reset_index(drop=True)
        if len(gdf) < before:
            logger.info("sicar_geo_dedup", removed=before - len(gdf), remaining=len(gdf))

    if return_meta:
        meta = build_source_meta(
            "sicar",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["sicar_wfs_geo"],
            selected_source="sicar_wfs_geo",
        )
        return gdf, meta

    return gdf


@overload
async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    cod_municipio: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf)
    uf_upper = uf.strip().upper()

    if municipio is not None and cod_municipio is not None:
        raise ValueError("Use 'municipio' ou 'cod_municipio', nao ambos")

    logger.info("sicar_resumo", uf=uf_upper, municipio=municipio, cod_municipio=cod_municipio)

    t0 = time.monotonic()

    if municipio is None and cod_municipio is None:
        async with client.make_session() as http:
            total = await client.fetch_hits(uf_upper, client=http)
            ativos = await client.fetch_hits(uf_upper, "status_imovel='AT'", client=http)
            pendentes = await client.fetch_hits(uf_upper, "status_imovel='PE'", client=http)
            suspensos = await client.fetch_hits(uf_upper, "status_imovel='SU'", client=http)
            cancelados = await client.fetch_hits(uf_upper, "status_imovel='CA'", client=http)

        fetch_ms = int((time.monotonic() - t0) * 1000)

        df = pd.DataFrame(
            [
                {
                    "total": total,
                    "ativos": ativos,
                    "pendentes": pendentes,
                    "suspensos": suspensos,
                    "cancelados": cancelados,
                }
            ]
        )

        source_url = WFS_BASE
        parse_ms = 0
    else:
        cql = _build_cql_filter(municipio=municipio, cod_municipio=cod_municipio)

        pages, source_url = await client.fetch_imoveis(uf_upper, cql)
        fetch_ms = int((time.monotonic() - t0) * 1000)

        t1 = time.monotonic()
        df_raw = parser.parse_imoveis_csv(pages)
        df = parser.agregar_resumo(df_raw)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "sicar",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["sicar_wfs"],
        selected_source="sicar_wfs",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
