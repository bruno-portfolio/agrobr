from __future__ import annotations

import hashlib
import time
import warnings
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import tasks
from agrobr.utils import time as time_utils
from agrobr.utils.geo import avisar_geometrias_invalidas, check_geopandas, validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import parse_data

from . import client, parser
from .models import FONTES, JANELA_PUBLICACAO_DIAS, MAX_REGISTROS_PADRAO, TIPOS_DATA

TipoData = Literal["deteccao", "publicacao"]

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _prepare_bbox(
    bbox: tuple[float, float, float, float] | None,
) -> list[float] | None:
    if not bbox:
        return None
    xmin, ymin, xmax, ymax = bbox
    return [xmin, ymin, xmax, ymax]


def _validar(
    inicio: str | date | datetime | None,
    fim: str | date | datetime | None,
    max_registros: int | None,
    tipo_data: str,
) -> tuple[str | None, str | None]:
    if not isinstance(tipo_data, str) or tipo_data not in TIPOS_DATA:
        raise InvalidParameterError(f"tipo_data={tipo_data!r} fora de {sorted(TIPOS_DATA)}")
    data_inicio = parse_data(inicio, "inicio")
    data_fim = parse_data(fim, "fim")
    if data_inicio is not None and data_fim is not None and data_inicio > data_fim:
        raise InvalidParameterError(f"inicio ({data_inicio}) posterior a fim ({data_fim})")
    if max_registros is not None and (type(max_registros) is not int or max_registros < 1):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
    return (
        data_inicio.isoformat() if data_inicio is not None else None,
        data_fim.isoformat() if data_fim is not None else None,
    )


def _deteccao_recente(tipo_data: str, fim: str | None) -> str | None:
    hoje = time_utils.hoje()
    ultimo = date.fromisoformat(fim) if fim is not None else hoje
    if tipo_data != "deteccao" or ultimo <= hoje - timedelta(days=JANELA_PUBLICACAO_DIAS):
        return None
    return (
        f"tipo_data='deteccao' até {ultimo.isoformat()}: 99% dos alertas são publicados em até "
        f"{JANELA_PUBLICACAO_DIAS} dias da detecção, então este período ainda ganha alertas enquanto "
        "a publicação chega; use tipo_data='publicacao' para o que foi publicado no período"
    )


async def _coletar(
    token: str | None,
    inicio: str | date | datetime | None,
    fim: str | date | datetime | None,
    sources: list[str] | None,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
    tipo_data: str,
    evento: str,
) -> tuple[client.Coleta, str, int]:
    bbox = validate_bbox(bbox)
    if sources is not None and (
        not isinstance(sources, (list, tuple))
        or not all(isinstance(fonte, str) for fonte in sources)
        or not set(sources) <= FONTES
    ):
        raise InvalidParameterError(
            f"sources={sources!r} fora do enum SourceTypes da API: {sorted(FONTES)}"
        )
    data_inicio, data_fim = _validar(inicio, fim, max_registros, tipo_data)
    resolved_token = client._get_token(token)

    logger.info(evento, bbox=bbox, sources=sources, max_registros=max_registros)

    t0 = time.monotonic()
    coleta, source_url = await client.fetch_alertas(
        token=resolved_token,
        start_date=data_inicio,
        end_date=data_fim,
        sources=list(sources) if sources is not None else None,
        bounding_box=_prepare_bbox(bbox),
        max_registros=max_registros,
        date_type=TIPOS_DATA[tipo_data],
    )
    recente = _deteccao_recente(tipo_data, data_fim)
    if recente is not None:
        coleta.avisos.insert(0, recente)
    for aviso in coleta.avisos:
        warnings.warn(aviso, UserWarning, stacklevel=3)
    return coleta, source_url, int((time.monotonic() - t0) * 1000)


def _meta(
    coleta: client.Coleta,
    source_url: str,
    source_method: str,
    fetch_ms: int,
    parse_ms: int,
    df: pd.DataFrame,
    selected_source: str,
    tipo_data: str,
) -> MetaInfo:
    corpos: list[dict[str, Any]] = [
        {
            "pagina": pagina,
            "url": source_url,
            "sha256": hashlib.sha256(corpo).hexdigest(),
            "bytes": len(corpo),
        }
        for pagina, corpo in enumerate(coleta.corpos, start=1)
    ]
    meta = build_source_meta(
        "mapbiomas_alerta",
        source_url,
        source_method,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[selected_source],
        selected_source=selected_source,
        raw_content_hash=corpos[0]["sha256"] if len(corpos) == 1 else None,
        raw_content_size=corpos[0]["bytes"] if len(corpos) == 1 else 0,
        source_details={
            "tipo_data": tipo_data,
            "total_anunciado": coleta.total_anunciado,
            "corpos": corpos,
        },
    )
    meta.validation_warnings.extend(coleta.avisos)
    return meta


@overload
async def alertas(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def alertas(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def alertas(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def alertas(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    coleta, source_url, fetch_ms = await _coletar(
        token, inicio, fim, sources, bbox, max_registros, tipo_data, "mapbiomas_alertas"
    )

    t1 = time.monotonic()
    df = parser.parse_alertas(coleta.registros)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _meta(
        coleta,
        source_url,
        "httpx+graphql",
        fetch_ms,
        parse_ms,
        df,
        "mapbiomas_alerta_graphql",
        tipo_data,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def alertas_geo(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def alertas_geo(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def alertas_geo(
    *,
    token: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    return_meta: bool = False,
) -> GeoDataFrameResult:
    check_geopandas()
    coleta, source_url, fetch_ms = await _coletar(
        token,
        inicio,
        fim,
        sources,
        bbox,
        max_registros,
        tipo_data,
        "mapbiomas_alertas_geo",
    )

    t1 = time.monotonic()
    gdf = parser.parse_alertas_geo(coleta.registros)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _meta(
            coleta,
            source_url,
            "httpx+graphql+wkt",
            fetch_ms,
            parse_ms,
            gdf,
            "mapbiomas_alerta_graphql_geo",
            tipo_data,
        )
        avisar_geometrias_invalidas(gdf, "MapBiomas Alerta", meta, stacklevel=2)
        return gdf, meta

    avisar_geometrias_invalidas(gdf, "MapBiomas Alerta", stacklevel=2)
    return gdf


async def alerta_info() -> dict[str, Any]:
    (date_range, _), (publication, _) = await tasks.gather_or_cancel(
        client.fetch_alert_date_range(),
        client.fetch_last_publication(),
    )
    return {"date_range": date_range, "last_publication": publication}
