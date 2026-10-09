from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.geo import check_geopandas
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    check_polars,
    finalize_result,
)

from . import client, models, parser
from .models import (
    UFS_SEM_DATA_ATUALIZACAO,
    WFS_BASE,
)

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _corpo(pages: list[bytes], consulta: str) -> dict[str, Any]:
    """Proveniência das páginas WFS: com várias, o topo é o manifesto canônico ``{query, resources}``."""
    if not pages:
        return {"raw_content_hash": None, "raw_content_size": 0}
    if len(pages) == 1:
        return {
            "raw_content_hash": hashlib.sha256(pages[0]).hexdigest(),
            "raw_content_size": len(pages[0]),
        }
    recursos = [
        {"pagina": indice, "sha256": hashlib.sha256(pagina).hexdigest(), "bytes": len(pagina)}
        for indice, pagina in enumerate(pages, 1)
    ]
    manifesto = json.dumps(
        {"query": consulta, "resources": recursos},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "raw_content_hash": hashlib.sha256(manifesto).hexdigest(),
        "raw_content_size": len(manifesto),
        "source_details": {
            "query": consulta,
            "resources": recursos,
            "hash_kind": "resource_manifest_sha256",
            "manifest_encoding": "canonical_json_utf8",
            "manifest_fields": ["query", "resources"],
            "resource_bytes": sum(recurso["bytes"] for recurso in recursos),
        },
    }


def _build_cql_filter(
    *,
    cod_municipio: int | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
) -> str | None:
    models.validate_cql_filters(
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        atualizado_apos=atualizado_apos,
    )
    parts: list[str] = []

    if cod_municipio is not None:
        parts.append(f"cod_municipio_ibge={cod_municipio}")

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

    if atualizado_apos:
        parts.append(f"data_atualizacao>'{models.normalize_updated_after(atualizado_apos)}'")

    return " AND ".join(parts) if parts else None


def _check_atualizado_apos_uf(uf: str, atualizado_apos: str | None) -> None:
    if atualizado_apos and uf in UFS_SEM_DATA_ATUALIZACAO:
        raise InvalidParameterError(
            f"atualizado_apos nao suportado para UF '{uf}': campo 'data_atualizacao' "
            f"nao existe neste layer WFS (UFs sem suporte: {sorted(UFS_SEM_DATA_ATUALIZACAO)})"
        )


@overload
async def imoveis(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def imoveis(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def imoveis(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def imoveis(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf_upper, cod_municipio = models.validate_filters(
        uf, municipio=municipio, status=status, tipo=tipo
    )

    _check_atualizado_apos_uf(uf_upper, atualizado_apos)
    check_polars(as_polars)

    logger.info(
        "sicar_imoveis",
        uf=uf_upper,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
    )

    cql = _build_cql_filter(
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        atualizado_apos=atualizado_apos,
    )

    t0 = time.monotonic()
    validation_warnings: list[str] = []
    sicar_details: dict[str, Any] = {}
    pages, source_url = await client.fetch_imoveis(
        uf_upper, cql, validation_warnings=validation_warnings, source_details=sicar_details
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = await asyncio.to_thread(
        parser.parse_imoveis_json,
        pages,
        source_details=sicar_details,
        validation_warnings=validation_warnings,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    df = df.sort_values("cod_imovel").reset_index(drop=True)

    contracts.validate_dataset(df, "sicar_imoveis")
    meta = build_source_meta(
        "sicar",
        source_url,
        "httpx+wfs+geojson",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version=contracts.get_contract("sicar_imoveis").version,
        attempted_sources=["sicar_wfs"],
        selected_source="sicar_wfs",
        **_corpo(pages, source_url),
    )
    meta.validation_warnings.extend(validation_warnings)
    if sicar_details:
        meta.source_details["sicar"] = sicar_details
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def imoveis_geo(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    max_registros: int | None = 5000,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def imoveis_geo(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    max_registros: int | None = 5000,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def imoveis_geo(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
    max_registros: int | None = 5000,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    models.validate_max_registros(max_registros)
    uf_upper, cod_municipio = models.validate_filters(
        uf, municipio=municipio, status=status, tipo=tipo
    )

    _check_atualizado_apos_uf(uf_upper, atualizado_apos)
    check_geopandas()

    logger.info(
        "sicar_imoveis_geo",
        uf=uf_upper,
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
    )

    cql = _build_cql_filter(
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        atualizado_apos=atualizado_apos,
    )

    validation_warnings: list[str] = []
    t0 = time.monotonic()
    pages, source_url = await client.fetch_imoveis_geo(
        uf_upper, cql, max_features=max_registros, validation_warnings=validation_warnings
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    sicar_details: dict[str, Any] = {}
    gdf = await asyncio.to_thread(
        parser.parse_imoveis_geojson,
        pages,
        max_features=max_registros,
        source_details=sicar_details,
        validation_warnings=validation_warnings,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    gdf = gdf.sort_values("cod_imovel").reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "sicar",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            schema_version=contracts.get_contract("sicar_imoveis").version,
            attempted_sources=["sicar_wfs_geo"],
            selected_source="sicar_wfs_geo",
            **_corpo(pages, source_url),
        )
        meta.validation_warnings.extend(validation_warnings)
        meta.source_details["sicar"] = sicar_details
        return gdf, meta

    return gdf


async def imoveis_geo_stream(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
) -> AsyncGenerator[gpd.GeoDataFrame, None]:
    """Itera sobre os imoveis rurais geoespaciais de uma UF em batches de baixo consumo de memoria.

    Cada yield e um GeoDataFrame parcial de uma pagina WFS (PAGE_SIZE features). As
    ocorrencias do ultimo cod_imovel de cada pagina seguem para o lote seguinte, porque
    as paginas vem ordenadas por cod_imovel e uma versao repetida pode cair na pagina
    seguinte; a versao mantida segue a regra de imoveis(). Ideal para processar volumes
    grandes (max_registros=None implicito) sem acumular tudo em memoria antes de comecar
    a usar os dados. Async-only: sem suporte em agrobr.sync.
    """
    uf_upper, cod_municipio = models.validate_filters(
        uf, municipio=municipio, status=status, tipo=tipo
    )
    _check_atualizado_apos_uf(uf_upper, atualizado_apos)
    check_geopandas()

    cql = _build_cql_filter(
        cod_municipio=cod_municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        atualizado_apos=atualizado_apos,
    )

    seen: set[str] = set()
    retidas: Any = None
    retidas_ids: list[str] = []
    async for batch_pages, _url in client.stream_imoveis_geo(uf_upper, cql, max_features=None):
        gdf, ids, _ = parser.parse_geo_pages(batch_pages, seen=seen)
        if gdf.empty:
            continue
        if retidas is not None:
            gdf = pd.concat([retidas, gdf], ignore_index=True)
            ids = retidas_ids + ids
        ultimo = (gdf["cod_imovel"] == gdf["cod_imovel"].iloc[-1]).to_numpy()
        retidas = gdf[ultimo].reset_index(drop=True)
        retidas_ids = [feature_id for feature_id, fica in zip(ids, ultimo, strict=True) if fica]
        prontas = gdf[~ultimo].reset_index(drop=True)
        if not prontas.empty:
            prontas_ids = [
                feature_id for feature_id, fica in zip(ids, ultimo, strict=True) if not fica
            ]
            yield parser.select_versions(prontas, prontas_ids)
    if retidas is not None:
        yield parser.select_versions(retidas, retidas_ids)


@overload
async def resumo(
    uf: str,
    *,
    municipio: int | str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def resumo(
    uf: str,
    *,
    municipio: int | str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def resumo(
    uf: str,
    *,
    municipio: int | str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def resumo(
    uf: str,
    *,
    municipio: int | str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf_upper, cod_municipio = models.validate_filters(uf, municipio=municipio)

    logger.info("sicar_resumo", uf=uf_upper, cod_municipio=cod_municipio)

    t0 = time.monotonic()
    validation_warnings: list[str] = []
    sicar_details: dict[str, Any] = {}
    pages: list[bytes] = []

    if cod_municipio is None:
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
        sicar_details["unidade"] = "feicoes_publicadas"
        validation_warnings.append(
            "sicar: o resumo sem município conta feições publicadas, e versões do mesmo cod_imovel "
            "contam separado; com municipio, conta imóveis (uma versão por cod_imovel)"
        )
    else:
        cql = _build_cql_filter(cod_municipio=cod_municipio)

        pages, source_url = await client.fetch_imoveis(
            uf_upper, cql, validation_warnings=validation_warnings, source_details=sicar_details
        )
        fetch_ms = int((time.monotonic() - t0) * 1000)

        t1 = time.monotonic()
        df_raw = parser.parse_imoveis_json(
            pages, source_details=sicar_details, validation_warnings=validation_warnings
        )
        df = parser.agregar_resumo(df_raw)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "sicar",
        source_url,
        "httpx+wfs+geojson" if cod_municipio is not None else "httpx+wfs",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["sicar_wfs"],
        selected_source="sicar_wfs",
        **_corpo(pages, source_url),
    )
    meta.validation_warnings.extend(validation_warnings)
    if sicar_details:
        meta.source_details["sicar"] = sicar_details
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
