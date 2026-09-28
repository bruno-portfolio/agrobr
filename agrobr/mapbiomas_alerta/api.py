from __future__ import annotations

import hashlib
import time
import warnings
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import tasks
from agrobr.utils import time as time_utils
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import FONTES, JANELA_PUBLICACAO_DIAS, MAX_REGISTROS_PADRAO, TIPOS_DATA

TipoData = Literal["deteccao", "publicacao"]

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


def _prepare_bbox(
    bbox: tuple[float, float, float, float] | None,
) -> list[float] | None:
    if not bbox:
        return None
    xmin, ymin, xmax, ymax = bbox
    return [xmin, ymin, xmax, ymax]


def _data(valor: str | None, nome: str) -> date | None:
    if valor is None:
        return None
    for formato in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(valor, formato).date()
        except ValueError:
            continue
    raise InvalidParameterError(f"{nome}={valor!r} fora dos formatos AAAA-MM-DD e DD/MM/AAAA")


def _validar(
    start_date: str | None, end_date: str | None, max_registros: int | None, tipo_data: str
) -> tuple[str | None, str | None]:
    if tipo_data not in TIPOS_DATA:
        raise InvalidParameterError(f"tipo_data={tipo_data!r} fora de {sorted(TIPOS_DATA)}")
    inicio = _data(start_date, "start_date")
    fim = _data(end_date, "end_date")
    if inicio is not None and fim is not None and inicio > fim:
        raise InvalidParameterError(f"start_date ({start_date}) posterior a end_date ({end_date})")
    if max_registros is not None and (type(max_registros) is not int or max_registros < 1):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
    return (
        inicio.isoformat() if inicio is not None else None,
        fim.isoformat() if fim is not None else None,
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
    start_date: str | None,
    end_date: str | None,
    sources: list[str] | None,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
    tipo_data: str,
    evento: str,
) -> tuple[client.Coleta, str, int]:
    bbox = validate_bbox(bbox)
    if sources is not None and not set(sources) <= FONTES:
        raise InvalidParameterError(
            f"sources={sources!r} fora do enum SourceTypes da API: {sorted(FONTES)}"
        )
    inicio, fim = _validar(start_date, end_date, max_registros, tipo_data)
    resolved_token = client._get_token(token)

    logger.info(evento, bbox=bbox, sources=sources, max_registros=max_registros)

    t0 = time.monotonic()
    coleta, source_url = await client.fetch_alertas(
        token=resolved_token,
        start_date=inicio,
        end_date=fim,
        sources=sources,
        bounding_box=_prepare_bbox(bbox),
        max_registros=max_registros,
        date_type=TIPOS_DATA[tipo_data],
    )
    recente = _deteccao_recente(tipo_data, fim)
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
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def alertas(
    *,
    token: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def alertas(
    *,
    token: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    coleta, source_url, fetch_ms = await _coletar(
        token, start_date, end_date, sources, bbox, max_registros, tipo_data, "mapbiomas_alertas"
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
    start_date: str | None = None,
    end_date: str | None = None,
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
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def alertas_geo(
    *,
    token: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = MAX_REGISTROS_PADRAO,
    tipo_data: TipoData = "deteccao",
    return_meta: bool = False,
) -> Any:
    coleta, source_url, fetch_ms = await _coletar(
        token,
        start_date,
        end_date,
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
        return gdf, meta

    return gdf


async def alerta_info() -> dict[str, Any]:
    (date_range, _), (publication, _) = await tasks.gather_or_cancel(
        client.fetch_alert_date_range(),
        client.fetch_last_publication(),
    )
    return {"date_range": date_range, "last_publication": publication}
