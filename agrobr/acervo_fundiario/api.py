from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.geo import check_geopandas, check_pyogrio, validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

from . import client, parser
from .models import (
    SIGEF_NATUREZAS,
    SIGEF_SCHEMA_VERSION,
    SNCI_COLUNAS_SAIDA_NATUREZA,
    SNCI_COLUNAS_SAIDA_NATUREZA_GEO,
    SNCI_NATUREZA_SCHEMA_VERSION,
)

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)

_SOURCE_METHOD = "httpx+pyogrio+shapefile_zip"


def _check_readers() -> None:
    check_pyogrio()
    check_geopandas()


def _naturezas(natureza: object) -> tuple[str, ...]:
    if natureza is None:
        return SIGEF_NATUREZAS
    if isinstance(natureza, str):
        chave = regions.remover_acentos(natureza).strip().lower()
        if chave in SIGEF_NATUREZAS:
            return (chave,)
    raise InvalidParameterError(
        f"natureza inválida: {natureza!r}. Valores válidos: {', '.join(SIGEF_NATUREZAS)}"
    )


def _tema_snci(natureza: object) -> tuple[str, str | None]:
    if natureza is None:
        return "snci", None
    (escolhida,) = _naturezas(natureza)
    return f"snci_{escolhida}", escolhida


def _com_natureza(df: Any, natureza: str | None, colunas: list[str]) -> Any:
    if natureza is None:
        return df
    return parser._with_natureza(df, natureza)[colunas]


def _build_meta(
    *,
    tema: str,
    uf: str | None,
    fetch_ms: int,
    parse_ms: int,
    df: Any,
    aquisicao: client.Aquisicao,
    schema_version: str = "1.0",
) -> MetaInfo:
    meta = build_source_meta(
        "acervo_fundiario",
        client._build_url(tema, uf),
        _SOURCE_METHOD,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version=schema_version,
        attempted_sources=[f"acervo_fundiario_{tema}"],
        selected_source=f"acervo_fundiario_{tema}",
        raw_content_hash=aquisicao.sha256,
        raw_content_size=aquisicao.size_bytes,
        source_details=aquisicao.source_details,
    )
    meta.from_cache = aquisicao.from_cache
    meta.fetched_at = aquisicao.fetched_at
    meta.fetch_timestamp = aquisicao.fetched_at
    if "topology_repaired" in df.attrs:
        meta.source_details["topology_repaired"] = df.attrs["topology_repaired"]
    return meta


async def _read_sigef(
    uf: str,
    naturezas: tuple[str, ...],
    *,
    geo: bool,
    bbox: tuple[float, float, float, float] | None,
    use_cache: bool,
) -> tuple[Any, dict[str, client.Aquisicao], int, int]:
    parse = parser.parse_sigef_geo if geo else parser.parse_sigef
    partes: dict[str, Any] = {}
    lidos: dict[str, client.Aquisicao] = {}
    fetch_ms = parse_ms = 0
    for natureza in naturezas:
        t0 = time.monotonic()
        async with client.adquirir(f"sigef_{natureza}", uf, use_cache=use_cache) as aquisicao:
            fetch_ms += int((time.monotonic() - t0) * 1000)
            t1 = time.monotonic()
            partes[natureza] = parse(aquisicao.zip_path, natureza=natureza, bbox=bbox)
            parse_ms += int((time.monotonic() - t1) * 1000)
        lidos[natureza] = aquisicao
    return parser.join_sigef(partes), lidos, fetch_ms, parse_ms


def _build_sigef_meta(
    *, uf: str, lidos: dict[str, client.Aquisicao], fetch_ms: int, parse_ms: int, df: Any
) -> MetaInfo:
    urls = {natureza: client._build_url(f"sigef_{natureza}", uf) for natureza in lidos}
    aquisicoes = list(lidos.values())
    meta = build_source_meta(
        "acervo_fundiario",
        next(iter(urls.values())),
        _SOURCE_METHOD,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version=SIGEF_SCHEMA_VERSION,
        attempted_sources=[f"acervo_fundiario_sigef_{natureza}" for natureza in lidos],
        selected_source="acervo_fundiario_sigef",
        raw_content_hash=aquisicoes[0].sha256 if len(aquisicoes) == 1 else None,
        raw_content_size=sum(aquisicao.size_bytes for aquisicao in aquisicoes),
        source_details={
            "arquivos": {
                natureza: {
                    "url": urls[natureza],
                    "from_cache": aquisicao.from_cache,
                    "fetched_at": aquisicao.fetched_at.isoformat(),
                    "sha256": aquisicao.sha256,
                    "size_bytes": aquisicao.size_bytes,
                    **aquisicao.source_details,
                }
                for natureza, aquisicao in lidos.items()
            }
        },
    )
    meta.from_cache = all(aquisicao.from_cache for aquisicao in aquisicoes)
    meta.fetched_at = min(aquisicao.fetched_at for aquisicao in aquisicoes)
    meta.fetch_timestamp = meta.fetched_at
    if "topology_repaired" in df.attrs:
        meta.source_details["topology_repaired"] = df.attrs["topology_repaired"]
    return meta


@overload
async def sigef(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sigef(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def sigef(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def sigef(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = regions.sigla_uf(uf)
    naturezas = _naturezas(natureza)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_sigef", uf=uf, natureza=natureza, bbox=bbox)

    df, lidos, fetch_ms, parse_ms = await _read_sigef(
        uf, naturezas, geo=False, bbox=bbox, use_cache=use_cache
    )
    meta = _build_sigef_meta(uf=uf, lidos=lidos, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def sigef_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def sigef_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def sigef_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    uf = regions.sigla_uf(uf)
    naturezas = _naturezas(natureza)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_sigef_geo", uf=uf, natureza=natureza, bbox=bbox)

    gdf, lidos, fetch_ms, parse_ms = await _read_sigef(
        uf, naturezas, geo=True, bbox=bbox, use_cache=use_cache
    )
    if return_meta:
        meta = _build_sigef_meta(uf=uf, lidos=lidos, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf)
        return gdf, meta
    return gdf


@overload
async def snci(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def snci(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def snci(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def snci(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = regions.sigla_uf(uf)
    tema, escolhida = _tema_snci(natureza)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_snci", uf=uf, natureza=escolhida, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir(tema, uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        df = parser.parse_snci(aquisicao.zip_path, bbox=bbox)
        df = _com_natureza(df, escolhida, SNCI_COLUNAS_SAIDA_NATUREZA)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        tema=tema,
        uf=uf,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        aquisicao=aquisicao,
        schema_version="1.0" if escolhida is None else SNCI_NATUREZA_SCHEMA_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def snci_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def snci_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def snci_geo(
    uf: str,
    *,
    natureza: Literal["publico", "privado"] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    uf = regions.sigla_uf(uf)
    tema, escolhida = _tema_snci(natureza)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_snci_geo", uf=uf, natureza=escolhida, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir(tema, uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        gdf = parser.parse_snci_geo(aquisicao.zip_path, bbox=bbox)
        gdf = _com_natureza(gdf, escolhida, SNCI_COLUNAS_SAIDA_NATUREZA_GEO)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema=tema,
            uf=uf,
            fetch_ms=fetch_ms,
            parse_ms=parse_ms,
            df=gdf,
            aquisicao=aquisicao,
            schema_version="1.0" if escolhida is None else SNCI_NATUREZA_SCHEMA_VERSION,
        )
        return gdf, meta
    return gdf


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf_norm = validate_uf(uf)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_assentamentos", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("assentamentos", use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        df = parser.parse_assentamentos(aquisicao.zip_path, uf=uf_norm, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        tema="assentamentos",
        uf=None,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        aquisicao=aquisicao,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    uf_norm = validate_uf(uf)
    bbox = validate_bbox(bbox)
    _check_readers()
    logger.info("acervo_fundiario_assentamentos_geo", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("assentamentos", use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        gdf = parser.parse_assentamentos_geo(aquisicao.zip_path, uf=uf_norm, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema="assentamentos",
            uf=None,
            fetch_ms=fetch_ms,
            parse_ms=parse_ms,
            df=gdf,
            aquisicao=aquisicao,
        )
        return gdf, meta
    return gdf
