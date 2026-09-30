from __future__ import annotations

import importlib
import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.comexstat import client, parser, query, result
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)


def _output_guards(*, as_polars: bool, return_meta: bool) -> None:
    from agrobr.datasets.deterministic import get_snapshot

    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "Comex Stat não suporta deterministic: os arquivos são mutáveis"
        )
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None


async def _fetch_comexstat(
    selected: query.ComexQuery,
    *,
    as_polars: bool,
    return_meta: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _output_guards(as_polars=as_polars, return_meta=return_meta)
    started = time.monotonic()
    async with client.open_csv(fluxo=selected.fluxo, ano=selected.ano) as acquired:
        fetch_ms = int((time.monotonic() - started) * 1000)
        parse_started = time.monotonic()
        parsed = parser.parse_resource(acquired.file, selected)
    logger.info(
        "comexstat_parsed_resource",
        fluxo=selected.fluxo,
        ano=selected.ano,
        produto=selected.produto,
        records=len(parsed.frame),
    )
    return result.finish(
        parsed,
        acquired,
        selected.to_dict(),
        f"comexstat_{selected.fluxo}_{selected.agregacao}",
        as_polars=as_polars,
        return_meta=return_meta,
        max_memoria_bytes=selected.max_memoria_bytes,
        fetch_ms=fetch_ms,
        parse_started=parse_started,
    )


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    *,
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
    *,
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    selected = query.build_query(
        fluxo="exportacao",
        produto=produto,
        ano=ano,
        uf=uf,
        pais=pais,
        via=via,
        urf=urf,
        agregacao=agregacao,
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
    )
    return await _fetch_comexstat(selected, as_polars=as_polars, return_meta=return_meta)


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    *,
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> pd.DataFrame: ...


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
    *,
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    selected = query.build_query(
        fluxo="importacao",
        produto=produto,
        ano=ano,
        uf=uf,
        pais=pais,
        via=via,
        urf=urf,
        agregacao=agregacao,
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
    )
    return await _fetch_comexstat(selected, as_polars=as_polars, return_meta=return_meta)


@overload
async def dicionario(
    tabela: str,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def dicionario(
    tabela: str,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def dicionario(
    tabela: str,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    selected = query.dictionary_table(tabela)
    _output_guards(as_polars=as_polars, return_meta=return_meta)
    started = time.monotonic()
    async with client.open_dictionary(selected) as acquired:
        fetch_ms = int((time.monotonic() - started) * 1000)
        parse_started = time.monotonic()
        parsed = parser.parse_dictionary(acquired.file, tabela=selected)
    return result.finish(
        parsed,
        acquired,
        {"tabela": selected, "pedido": {"tabela": tabela}},
        f"comexstat_dicionario_{selected}",
        as_polars=as_polars,
        return_meta=return_meta,
        max_memoria_bytes=constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
        fetch_ms=fetch_ms,
        parse_started=parse_started,
    )
