from __future__ import annotations

import time
from typing import TYPE_CHECKING, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize.regions import UFS_VALIDAS, remover_acentos
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.validation import validate_year_uf

from . import client, parser
from .models import CULTURAS_PROGRESSO, CULTURAS_VALIDAS, ESTADO_MEDIA

if TYPE_CHECKING:
    import polars as pl

logger = _log.get_logger(__name__)

OPERACOES_VALIDAS = ("Semeadura", "Colheita")


def _chave_cultura(texto: str) -> str:
    return remover_acentos(texto.strip().lower()).replace("_", " ")


def _culturas_do_produto(produto: str) -> set[str]:
    aliases = {
        **{_chave_cultura(alias): {nome} for alias, nome in CULTURAS_PROGRESSO.items()},
        **{_chave_cultura(nome): {nome} for nome in CULTURAS_VALIDAS},
        "milho": {"Milho 1ª", "Milho 2ª"},
    }
    culturas = aliases.get(_chave_cultura(produto)) if isinstance(produto, str) else None
    if not culturas:
        raise InvalidParameterError(
            f"Produto inválido: {produto!r}. Valores válidos: "
            f"{', '.join(sorted(CULTURAS_VALIDAS))} (e milho, para a 1ª e a 2ª safra)"
        )
    return culturas


def _validar_operacao(operacao: str) -> str:
    op_title = operacao.strip().title() if isinstance(operacao, str) else ""
    if op_title not in OPERACOES_VALIDAS:
        raise InvalidParameterError(
            f"Operação inválida: {operacao!r}. Valores válidos: {', '.join(OPERACOES_VALIDAS)}"
        )
    return op_title


@overload
async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def progresso_safra(
    *,
    produto: str | None = None,
    uf: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    culturas = None if produto is None else _culturas_do_produto(produto)
    if uf is not None:
        validate_year_uf(uf=uf, ufs_validas=UFS_VALIDAS | {"BR", ESTADO_MEDIA})
    op_title = None if operacao is None else _validar_operacao(operacao)

    logger.info("conab_progresso_safra", produto=produto, uf=uf, operacao=operacao)

    t0 = time.monotonic()
    if semana_url:
        xlsx_bytes, source_url = await client.fetch_xlsx_semanal(semana_url)
    else:
        xlsx_bytes, source_url, _desc = await client.fetch_latest()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_progresso_xlsx(xlsx_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if culturas is not None:
        df = df[df["cultura"].isin(culturas)].reset_index(drop=True)

    if uf is not None:
        uf_upper = uf.strip().upper()
        selecionado = df[df["uf"].str.upper() == uf_upper]
        media = df[df["uf"] == ESTADO_MEDIA]
        if uf_upper == "BR" and selecionado.empty and not media.empty:
            coberturas = "; ".join(
                f"{linha.cultura} {linha.operacao}: {linha.n_estados} estados, "
                f"{linha.cobertura_area_pct:.1%} da área"
                for linha in media.drop_duplicates(["cultura", "operacao"]).itertuples()
            )
            raise InvalidParameterError(
                "A CONAB não publica Brasil no progresso de safra: a planilha traz a média da "
                f"própria CONAB dos estados monitorados ({coberturas}). "
                f"Use uf={ESTADO_MEDIA!r}."
            )
        df = selecionado.reset_index(drop=True)

    if op_title is not None:
        df = df[df["operacao"] == op_title].reset_index(drop=True)

    meta = build_source_meta(
        "conab_progresso",
        source_url,
        "httpx+xlsx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version="2.0",
        attempted_sources=["conab_govbr"],
        selected_source="conab_govbr",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def semanas_disponiveis(max_pages: int = 4) -> list[dict[str, str]]:
    weeks = await client.list_semanas(max_pages=max_pages)
    return [{"descricao": desc, "url": url} for desc, url in weeks]
