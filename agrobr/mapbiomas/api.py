from __future__ import annotations

import asyncio
import time
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils import result
from agrobr.utils.warnings import warn_once

from . import client, municipal_parser, parser, queries, resources

logger = _log.get_logger(__name__)

_ROTULOS_DE_CLASSE = (
    ("classe", "classe_id"),
    ("classe_de", "classe_de_id"),
    ("classe_para", "classe_para_id"),
)


def _filtrar(frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    for column, value in filters.items():
        if value is not None:
            frame = frame[frame[column] == value]
    return frame.reset_index(drop=True)


def _classe_publicada(nome: str, valor: int | None, publicadas: set[int], colecao: int) -> None:
    if valor is not None and valor not in publicadas:
        raise InvalidParameterError(
            f"{nome}={valor} fora das classes publicadas na coleção {colecao}: {sorted(publicadas)}"
        )


def _classes_fora_da_legenda(frame: pd.DataFrame, colecao: int) -> str | None:
    ids = sorted(
        {
            int(codigo)
            for rotulo, coluna_id in _ROTULOS_DE_CLASSE
            if rotulo in frame
            for codigo in frame.loc[frame[rotulo].isna() & frame[coluna_id].notna(), coluna_id]
        }
    )
    if not ids:
        return None
    aviso = (
        f"MapBiomas coleção {colecao}: classes {ids} fora da legenda conhecida; "
        "o rótulo sai nulo e o classe_id é o publicado"
    )
    warn_once(f"mapbiomas_classes_{colecao}_{ids}", aviso)
    return aviso


def _build_meta(
    acquired: resources.WorkbookAcquisition,
    frame: pd.DataFrame,
    *,
    colecao: int,
    contract_name: str,
    parser_version: int,
    fetch_ms: int,
    parse_ms: int,
    details: dict[str, Any],
) -> MetaInfo:
    contract = contracts.get_contract(contract_name)
    route = "mapbiomas_dataverse" if colecao == 10 else "mapbiomas_oficial"
    meta = result.build_source_meta(
        "mapbiomas",
        acquired.source_url,
        "httpx+zip+xlsx" if acquired.member else "httpx+xlsx",
        fetch_ms,
        parse_ms,
        frame,
        parser_version,
        schema_version=contract.version,
        attempted_sources=[route],
        selected_source=route,
        raw_content_hash=acquired.resource.sha256,
        source_details={
            **details,
            "collection": colecao,
            "contract": contract.name,
            "acquisition": acquired.provenance(),
        },
    )
    meta.data_sources = [f"mapbiomas_colecao_{colecao}"]
    meta.contract_version = contract.version
    meta.raw_content_size = acquired.resource.size_bytes
    meta.fetched_at = acquired.resource.fetched_at
    meta.fetch_timestamp = acquired.resource.fetched_at
    aviso = _classes_fora_da_legenda(frame, colecao)
    if aviso:
        meta.validation_warnings.append(aviso)
    return meta


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "uf", "municipio"] = "estado",
    municipio: str | int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "uf", "municipio"] = "estado",
    municipio: str | int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "uf", "municipio"] = "estado",
    municipio: str | int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def cobertura(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "uf", "municipio"] = "estado",
    municipio: str | int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    queries.validar_opcoes(kwargs, as_polars=as_polars, return_meta=return_meta)
    colecao = queries.validar_colecao(colecao)
    uf = queries.normalizar_uf(uf)
    bioma = queries.normalizar_bioma(bioma)
    queries.validar_ano(ano, colecao)
    queries.validar_classe("classe_id", classe_id)
    nivel = "estado" if nivel == "uf" else nivel
    geocodigo = queries.geocodigo_do_municipio(nivel, municipio, uf)
    filters = {"bioma": bioma, "uf": uf, "ano": ano, "classe_id": classe_id}
    result.check_polars(as_polars)
    logger.info("mapbiomas_cobertura", **filters, nivel=nivel, geocodigo=geocodigo)
    started = time.monotonic()
    if nivel == "municipio":
        acquired = await client.fetch_biome_state_municipality_bundle(colecao=colecao)
    else:
        acquired = await client.fetch_biome_state_bundle(colecao=colecao)
    fetch_ms = int((time.monotonic() - started) * 1000)
    started = time.monotonic()
    details: dict[str, Any] = {}
    if nivel == "municipio":
        frame, details = await asyncio.to_thread(
            municipal_parser.parse_cobertura_municipal,
            acquired.content,
            colecao=colecao,
            bioma=bioma,
            uf=uf,
            ano=ano,
            classe_id=classe_id,
            geocodigo=geocodigo,
        )
        publicadas = {int(codigo) for codigo in details["classes"]}
        contract_name = "mapbiomas_cobertura_municipal"
        parser_version = municipal_parser.PARSER_VERSION
    else:
        completo = await asyncio.to_thread(
            parser.parse_cobertura_xlsx, acquired.content, colecao=colecao
        )
        publicadas = {int(codigo) for codigo in completo["classe_id"].dropna().unique()}
        frame = _filtrar(completo, filters)
        contract_name = "mapbiomas_cobertura"
        parser_version = parser.PARSER_VERSION
    _classe_publicada("classe_id", classe_id, publicadas, colecao)
    if contract_name == "mapbiomas_cobertura_municipal":
        frame = frame.assign(cod_municipio=regions.cod_municipio(frame["geocodigo"]))
    contracts.validate_dataset(frame, contract_name)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = _build_meta(
        acquired,
        frame,
        colecao=colecao,
        contract_name=contract_name,
        parser_version=parser_version,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        details=details,
    )
    return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def transicao(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def transicao(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def transicao(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def transicao(
    *,
    bioma: str | None = None,
    uf: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    queries.validar_opcoes(kwargs, as_polars=as_polars, return_meta=return_meta)
    colecao = queries.validar_colecao(colecao)
    uf = queries.normalizar_uf(uf)
    bioma = queries.normalizar_bioma(bioma)
    queries.validar_periodo(periodo, colecao)
    queries.validar_classe("classe_de_id", classe_de_id)
    queries.validar_classe("classe_para_id", classe_para_id)
    filters = {
        "bioma": bioma,
        "uf": uf,
        "periodo": periodo,
        "classe_de_id": classe_de_id,
        "classe_para_id": classe_para_id,
    }
    logger.info("mapbiomas_transicao", **filters)
    started = time.monotonic()
    acquired = await client.fetch_biome_state_bundle(colecao=colecao)
    fetch_ms = int((time.monotonic() - started) * 1000)
    started = time.monotonic()
    completo = parser.parse_transicao_xlsx(acquired.content, colecao=colecao)
    for nome, valor in (("classe_de_id", classe_de_id), ("classe_para_id", classe_para_id)):
        publicadas = {int(codigo) for codigo in completo[nome].dropna().unique()}
        _classe_publicada(nome, valor, publicadas, colecao)
    frame = _filtrar(completo, filters)
    contracts.validate_dataset(frame, "mapbiomas_transicao")
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = _build_meta(
        acquired,
        frame,
        colecao=colecao,
        contract_name="mapbiomas_transicao",
        parser_version=parser.PARSER_VERSION,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        details={},
    )
    return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)
