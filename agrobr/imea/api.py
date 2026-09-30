from __future__ import annotations

import hashlib
import time
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import resolve_cadeia_id

logger = _log.get_logger(__name__)

CHAVE = ["indicador_id", "localidade", "data_publicacao", "safra", "unidade"]


@overload
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once(
        "imea",
        "IMEA: termos de uso proíbem redistribuição de dados sem "
        "autorização escrita. Uso pessoal/educacional apenas. "
        "Ref: https://imea.com.br/imea-site/termo-de-uso.html",
    )

    cadeia_id = resolve_cadeia_id(cadeia)

    logger.info(
        "imea_cotacoes",
        cadeia=cadeia,
        cadeia_id=cadeia_id,
        safra=safra,
        unidade=unidade,
    )

    t0 = time.monotonic()
    records, corpo = await client.fetch_cotacoes(cadeia_id)
    indicadores, catalogo = await client.fetch_indicadores(cadeia_id)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_cotacoes(records, indicadores, cadeia_id)

    if safra:
        df = parser.filter_by_safra(df, safra)

    if unidade:
        df = parser.filter_by_unidade(df, unidade)

    df, colapsadas, repetidas = _colapsar_repetidos(df, cadeia_id)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "imea",
        client.cotacoes_url(cadeia_id),
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=hashlib.sha256(corpo).hexdigest(),
        raw_content_size=len(corpo),
        source_details={
            "indicadores_url": client.indicadores_url(cadeia_id),
            "indicadores_sha256": hashlib.sha256(catalogo).hexdigest(),
            "indicadores_bytes": len(catalogo),
            "duplicatas_colapsadas": colapsadas,
            "chaves_repetidas": repetidas,
        },
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _resumo(df: pd.DataFrame, linhas: pd.Series) -> dict[str, Any]:
    return {
        "linhas": int(linhas.sum()),
        "indicadores": sorted({str(v) for v in df.loc[linhas, "indicador_id"]}),
    }


def _colapsar_repetidos(
    df: pd.DataFrame, cadeia_id: int
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Colapsa o registro que a fonte publica mais de uma vez, igual em todas as colunas.

    A chave (indicador, localidade, data, safra e unidade) repetida com valores diferentes não
    se colapsa: todas as linhas saem, com aviso, porque um erro derrubaria a cadeia inteira por
    um indicador.
    """
    iguais = df.duplicated(keep="first")
    colapsadas = _resumo(df, iguais)
    if colapsadas["linhas"]:
        warn_once(
            f"imea_duplicatas_{cadeia_id}",
            f"IMEA: {colapsadas['linhas']} registro(s) publicados mais de uma vez pela fonte, "
            "iguais em todas as colunas, saem uma vez só (indicadores "
            f"{', '.join(colapsadas['indicadores'])})",
        )
        df = df[~iguais].reset_index(drop=True)
    conflitantes = df.duplicated(CHAVE, keep=False)
    repetidas = _resumo(df, conflitantes)
    if repetidas["linhas"]:
        warn_once(
            f"imea_chaves_repetidas_{cadeia_id}",
            f"IMEA: {repetidas['linhas']} linha(s) repetem a chave (indicador, localidade, data, "
            "safra e unidade) com valores diferentes e saem todas (indicadores "
            f"{', '.join(repetidas['indicadores'])})",
        )
    return df, colapsadas, repetidas
