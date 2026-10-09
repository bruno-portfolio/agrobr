from __future__ import annotations

import time
from datetime import UTC, date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult, build_source_meta
from agrobr.utils.time import hoje

from . import client, models, output, parser, provenance
from .models import UF_COORDS


def _validate_point(lat: object, lon: object) -> tuple[float, float]:

    if not isinstance(lat, (int, float)) or isinstance(lat, bool) or not -90 <= lat <= 90:
        raise InvalidParameterError("lat deve estar entre -90 e 90")

    if not isinstance(lon, (int, float)) or isinstance(lon, bool) or not -180 <= lon <= 180:
        raise InvalidParameterError("lon deve estar entre -180 e 180")

    return float(lat), float(lon)


def _normalize_range(inicio: str | date, fim: str | date) -> tuple[date, date]:

    try:
        start = date.fromisoformat(inicio) if isinstance(inicio, str) else inicio

        end = date.fromisoformat(fim) if isinstance(fim, str) else fim

    except ValueError as exc:
        raise InvalidParameterError("inicio e fim devem usar o formato YYYY-MM-DD") from exc

    if type(start) is not date or type(end) is not date:
        raise InvalidParameterError("inicio e fim devem ser datas ou strings YYYY-MM-DD")

    if start > end:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")

    if start < date(1981, 1, 1):
        raise InvalidParameterError("inicio deve ser a partir de 1981-01-01")

    publicado = hoje()

    if start > publicado:
        raise InvalidParameterError(
            f"inicio {start.isoformat()} no futuro: a NASA POWER publica até {publicado.isoformat()}"
        )

    return start, end


def _validate_agregacao(agregacao: str) -> None:

    if not isinstance(agregacao, str) or agregacao not in {"diario", "mensal"}:
        raise InvalidParameterError("agregacao deve ser 'diario' ou 'mensal'")


@overload
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> DataFrameResult: ...


async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> DataFrameResult:

    lat, lon = _validate_point(lat, lon)

    inicio, fim = _normalize_range(inicio, fim)

    _validate_agregacao(agregacao)

    selected = models.validate_parameters(parameters)
    polars = output.preflight(as_polars, return_meta, kwargs)

    t0 = time.monotonic()

    dados = await client.fetch_daily(lat, lon, inicio, fim, parameters=selected)
    acquired_at = datetime.now(UTC)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()

    parser.validate_period(dados, inicio, fim)
    df = parser.parse_daily(dados, lat, lon, parameters=selected)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    source_url = (
        f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
        f"&parameters={','.join(selected)}&community=AG&format=JSON&time-standard=LST"
        f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
    )

    meta = build_source_meta(
        "nasa_power",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version="1.2",
        source_details=_source_details(selected, lat, lon, agregacao, dados),
    )

    provenance.apply_receipts(meta, dados, acquired_at)
    provenance.apply_sources(meta, dados)
    return output.finalize(df, meta, polars, return_meta)


@overload
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> DataFrameResult: ...


async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
    **kwargs: Any,
) -> DataFrameResult:

    _validate_agregacao(agregacao)

    if not isinstance(uf, str):
        raise InvalidParameterError(
            f"uf deve ser uma sigla brasileira. UFs disponíveis: {sorted(UF_COORDS.keys())}"
        )

    corrente = hoje().year

    if isinstance(ano, bool) or not isinstance(ano, int) or not 1981 <= ano <= corrente:
        raise InvalidParameterError(f"ano deve ser inteiro entre 1981 e {corrente}")

    uf_upper = uf.upper()

    if uf_upper not in UF_COORDS:
        raise InvalidParameterError(
            f"UF '{uf_upper}' não reconhecida. UFs disponíveis: {sorted(UF_COORDS.keys())}"
        )

    lat, lon = UF_COORDS[uf_upper]

    inicio = date(ano, 1, 1)

    fim = date(ano, 12, 31)

    selected = models.validate_parameters(parameters)
    polars = output.preflight(as_polars, return_meta, kwargs)

    t0 = time.monotonic()

    dados = await client.fetch_daily(lat, lon, inicio, fim, parameters=selected)
    acquired_at = datetime.now(UTC)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()

    parser.validate_period(dados, inicio, fim)
    df = parser.parse_daily(dados, lat, lon, uf=uf_upper, parameters=selected)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    source_url = (
        f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
        f"&parameters={','.join(selected)}&community=AG&format=JSON&time-standard=LST"
        f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
    )

    meta = build_source_meta(
        "nasa_power",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version="1.2",
        source_details=_source_details(selected, lat, lon, agregacao, dados),
    )

    provenance.apply_receipts(meta, dados, acquired_at)
    provenance.apply_sources(meta, dados)
    return output.finalize(df, meta, polars, return_meta)


def parametros() -> pd.DataFrame:
    """Lista o subconjunto de parâmetros diários AG suportado pelo agrobr."""

    return pd.DataFrame([item.model_dump() for item in models.CATALOGO])


def _source_details(
    selected: list[str], lat: float, lon: float, aggregation: str, data: dict[str, Any]
) -> dict[str, Any]:

    return {
        "parameters": selected,
        "observed_coverage": _observed_coverage(selected, data),
        "parameter_catalog": [
            item.model_dump() for item in models.CATALOGO if item.codigo in selected
        ],
        "time_basis": "LST",
        "spatial_scope": "point",
        "points": [{"lat": lat, "lon": lon}],
        "spatial_aggregation": {"all_variables": "single_requested_point"},
        "temporal_aggregation": aggregation,
        "temporal_source": "daily",
        "monthly_method": "local_aggregation_of_available_daily_values"
        if aggregation == "mensal"
        else None,
    }


def _observed_coverage(selected: list[str], data: dict[str, Any]) -> dict[str, Any]:
    response = parser.validate_response(data)
    coverage = {}
    for code in selected:
        values = response.properties.parameter[code]
        valid_dates = [
            day
            for day, value in values.items()
            if value is not None and value not in {response.header.fill_value, models.SENTINEL}
        ]
        coverage[code] = {
            "returned_days": len(values),
            "valid_days": len(valid_dates),
            "first_valid_day": min(valid_dates, default=None),
            "last_valid_day": max(valid_dates, default=None),
        }
    return coverage
