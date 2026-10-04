from __future__ import annotations

import time
import warnings
from datetime import date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils import validation
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result

from . import client, historical, models, parser

logger = _log.get_logger(__name__)


@overload
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = validation.validate_uf(uf)
    t0 = time.monotonic()
    dados = await client.fetch_estacoes(tipo)
    fetch_ms = int((time.monotonic() - t0) * 1000)
    if not dados:
        raise ParseError(
            source="inmet",
            parser_version=parser.PARSER_VERSION,
            reason=f"Catálogo de estações do INMET vazio (tipo {tipo})",
        )

    t1 = time.monotonic()

    df = pd.DataFrame(dados)

    rename_map = {
        "CD_ESTACAO": "codigo",
        "DC_NOME": "nome",
        "SG_ESTADO": "uf",
        "CD_SITUACAO": "situacao",
        "TP_ESTACAO": "tipo",
        "VL_LATITUDE": "latitude",
        "VL_LONGITUDE": "longitude",
        "VL_ALTITUDE": "altitude",
        "DT_INICIO_OPERACAO": "inicio_operacao",
    }

    colunas_presentes = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=colunas_presentes)

    for col in ["latitude", "longitude", "altitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for coluna, nome, valor in (
        ("situacao", "apenas_operantes", apenas_operantes),
        ("uf", "uf", uf),
    ):
        if valor and coluna not in df.columns:
            raise ParseError(
                source="inmet",
                parser_version=parser.PARSER_VERSION,
                reason=f"Catálogo sem a coluna {coluna}; o filtro {nome}={valor!r} não pode ser aplicado",
            )

    if apenas_operantes and "situacao" in df.columns:
        df = df[df["situacao"] == "Operante"]

    if uf is not None and "uf" in df.columns:
        df = df[df["uf"] == uf]

    df = df.reset_index(drop=True)
    parser.converter_datas_catalogo(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacoes/{tipo}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    codigo = models.validate_codigo(codigo)
    inicio, fim = models.validate_periodo(inicio, fim)
    models.validate_agregacao(agregacao)

    t0 = time.monotonic()
    dados = await client.fetch_dados_estacao(codigo, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)
    _exigir_observacoes(dados, f"da estação {codigo}", inicio, fim)

    t1 = time.monotonic()
    parser.validate_observation_scope(dados, inicio, fim, codigo=codigo)
    df = parser.parse_observacoes(dados)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if agregacao == "diario":
        df = parser.agregar_diario(df)

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacao/{inicio}/{fim}/{codigo}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        source_details={
            "access": "inmet_api",
            "time_basis": "UTC",
            "requested_period": {"inicio": inicio.isoformat(), "fim": fim.isoformat()},
            "temporal_aggregation": agregacao,
            "spatial_aggregation": {},
            "station_selection": {"mode": "codigo", "codigo": codigo},
        },
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def clima_uf(
    uf: str,
    ano: int,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def clima_uf(
    uf: str,
    ano: int,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def clima_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def clima_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = models.validate_uf(uf)
    models.validate_ano(ano)
    inicio = date(ano, 1, 1)
    fim = date(ano, 12, 31)

    t0 = time.monotonic()
    dados = await client.fetch_dados_estacoes_uf(uf, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)
    _exigir_observacoes(dados, f"das estações de {uf}", inicio, fim)

    t1 = time.monotonic()
    parser.validate_observation_scope(dados, inicio, fim, uf=uf)
    df_horario = parser.parse_observacoes(dados)
    df_diario = parser.agregar_diario(df_horario)
    df_mensal = parser.agregar_mensal_uf(df_diario)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "inmet",
        f"{client.BASE_URL}/estacoes/T",
        "httpx",
        fetch_ms,
        parse_ms,
        df_mensal,
        parser.PARSER_VERSION,
        source_details={
            "access": "inmet_api",
            "time_basis": "UTC",
            "requested_period": {"inicio": inicio.isoformat(), "fim": fim.isoformat()},
            "temporal_aggregation": "mensal",
            "spatial_aggregation": historical.spatial_aggregation(),
            "station_selection": {
                "mode": "catalogo_atual",
                "tipo": "T",
                "situacao": "Operante",
                "uf": uf,
                "returned_stations": sorted(df_horario["estacao"].unique().tolist()),
            },
        },
    )
    _avisar_chuva_parcial(df_mensal, meta.validation_warnings)
    return finalize_result(df_mensal, meta, as_polars=as_polars, return_meta=return_meta)


def _exigir_observacoes(dados: list[dict[str, Any]], alvo: str, inicio: date, fim: date) -> None:
    if not dados:
        raise SourceUnavailableError(
            source="inmet",
            url=client.BASE_URL,
            last_error=f"INMET sem observações {alvo} entre {inicio} e {fim}",
        )


def _avisar_chuva_parcial(mensal: pd.DataFrame, avisos: list[str]) -> None:
    if "estacoes_chuva" not in mensal:
        return
    sem_completa = mensal[mensal["estacoes_chuva"].eq(0) & mensal["estacoes_chuva_parciais"].gt(0)]
    if sem_completa.empty:
        return
    meses = ", ".join(f"{linha.uf} {linha.mes:%Y-%m}" for linha in sem_completa.itertuples())
    aviso = (
        f"Chuva mensal nula em {meses}: nenhuma estação com o mês completo. As estações "
        "parciais ficam fora da média e são contadas em estacoes_chuva_parciais."
    )
    avisos.append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)


@overload
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    models.validate_ano(ano)
    df, meta = await historico_periodo(
        codigo, date(ano, 1, 1), date(ano, 12, 31), agregacao, return_meta=True
    )
    if meta.source_details["coverage"]["missing_station_years"]:
        raise SourceUnavailableError(
            source="inmet",
            url=meta.source_url,
            last_error=f"Estação {codigo} sem dados no ano {ano}",
        )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _historical_meta(
    df: pd.DataFrame, details: dict[str, Any], fetch_ms: int, parse_ms: int
) -> MetaInfo:
    resources = details["resources"]
    meta = build_source_meta(
        "inmet",
        resources[0]["url"],
        "httpx+zip+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.HISTORICO_PARSER_VERSION,
        source_details=details,
        raw_content_hash=resources[0]["sha256"] if len(resources) == 1 else None,
        raw_content_size=resources[0]["bytes"] if len(resources) == 1 else 0,
    )
    meta.validation_warnings = details["warnings"]
    meta.from_cache = all(resource["from_cache"] for resource in resources)
    meta.fetched_at = max(datetime.fromisoformat(resource["fetched_at"]) for resource in resources)
    meta.fetch_timestamp = meta.fetched_at
    return meta


@overload
async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    codigo = models.validate_codigo(codigo)
    inicio, fim = models.validate_periodo(inicio, fim)
    models.validate_ano(inicio.year)
    models.validate_ano(fim.year)
    models.validate_agregacao(agregacao)
    data, details, fetch_ms, parse_ms = await historical.collect(inicio, fim, codigo=codigo)
    df = historical.aggregate(data, details, agregacao)
    meta = _historical_meta(df, details, fetch_ms, parse_ms)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def historico_uf(
    uf: str,
    ano: int,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def historico_uf(
    uf: str,
    ano: int,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def historico_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def historico_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = models.validate_uf(uf)
    models.validate_ano(ano)
    data, details, fetch_ms, parse_ms = await historical.collect(
        date(ano, 1, 1), date(ano, 12, 31), uf=uf
    )
    df = historical.aggregate(data, details, "mensal")
    _avisar_chuva_parcial(df, details["warnings"])
    meta = _historical_meta(df, details, fetch_ms, parse_ms)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
