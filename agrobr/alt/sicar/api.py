from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import (
    MAX_FEATURES_WARNING,
    STATUS_VALIDOS,
    TIPO_VALIDOS,
    UFS_VALIDAS,
    WFS_BASE,
)

logger = structlog.get_logger()


def _build_cql_filter(
    *,
    municipio: str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
) -> str | None:
    parts: list[str] = []

    if municipio:
        escaped = municipio.replace("'", "''")
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
        parts.append(f"dat_criacao>='{criado_apos}'")

    return " AND ".join(parts) if parts else None


@overload
async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def imoveis(
    uf: str,
    *,
    municipio: str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf_upper = uf.strip().upper()
    if uf_upper not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    if status is not None and status.upper() not in STATUS_VALIDOS:
        raise ValueError(f"Status '{status}' invalido. Opcoes: {sorted(STATUS_VALIDOS)}")

    if tipo is not None and tipo.upper() not in TIPO_VALIDOS:
        raise ValueError(f"Tipo '{tipo}' invalido. Opcoes: {sorted(TIPO_VALIDOS)}")

    logger.info(
        "sicar_imoveis",
        uf=uf_upper,
        municipio=municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
    )

    cql = _build_cql_filter(
        municipio=municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
    )

    if municipio is None:
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
        except Exception:
            logger.warning("sicar_hit_count_check_failed", uf=uf_upper, exc_info=True)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_imoveis(uf_upper, cql)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_imoveis_csv(pages)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if not df.empty and "cod_imovel" in df.columns:
        df = df.sort_values("cod_imovel").reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="sicar",
            source_url=source_url,
            source_method="httpx+wfs+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["sicar_wfs"],
            selected_source="sicar_wfs",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def resumo(
    uf: str,
    *,
    municipio: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf_upper = uf.strip().upper()
    if uf_upper not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    logger.info("sicar_resumo", uf=uf_upper, municipio=municipio)

    t0 = time.monotonic()

    if municipio is None:
        total = await client.fetch_hits(uf_upper)
        ativos = await client.fetch_hits(uf_upper, "status_imovel='AT'")
        pendentes = await client.fetch_hits(uf_upper, "status_imovel='PE'")
        suspensos = await client.fetch_hits(uf_upper, "status_imovel='SU'")
        cancelados = await client.fetch_hits(uf_upper, "status_imovel='CA'")

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
        escaped = municipio.replace("'", "''")
        cql = f"municipio ILIKE '%{escaped}%'"

        pages, source_url = await client.fetch_imoveis(uf_upper, cql)
        fetch_ms = int((time.monotonic() - t0) * 1000)

        t1 = time.monotonic()
        df_raw = parser.parse_imoveis_csv(pages)
        df = parser.agregar_resumo(df_raw)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="sicar",
            source_url=source_url,
            source_method="httpx+wfs+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["sicar_wfs"],
            selected_source="sicar_wfs",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
