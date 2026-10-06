from __future__ import annotations

import time
from typing import TYPE_CHECKING, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import CATEGORIAS, CEASA_UF_MAP, PRODUTOS_PROHORT

if TYPE_CHECKING:
    import polars as pl

logger = _log.get_logger(__name__)


def _chave_produto(nome: str) -> str:
    return regions.remover_acentos(nome.strip()).upper()


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    for nome, valor in (("produto", produto), ("ceasa", ceasa)):
        if valor is not None and (not isinstance(valor, str) or not valor.strip()):
            raise InvalidParameterError(f"{nome} deve ser texto não vazio, recebeu {valor!r}")
    warn_once(
        "conab_ceasa",
        "agrobr.conab.ceasa: dados CONAB/PROHORT via Pentaho CDA. "
        "Credenciais publicas embutidas no frontend, mas API nao e "
        "oficialmente documentada. Classificacao: zona_cinza. "
        "Veja https://www.agrobr.dev/docs/licenses/.",
    )

    logger.info("conab_ceasa_precos", produto=produto, ceasa=ceasa)

    t0 = time.monotonic()
    precos_json, source_url = await client.fetch_precos()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_precos(precos_json)
    parse_ms = int((time.monotonic() - t1) * 1000)

    publicados = {coluna: sorted(df[coluna].dropna().unique()) for coluna in ("produto", "ceasa")}

    if produto is not None:
        chave = _chave_produto(produto)
        if chave not in {_chave_produto(nome) for nome in publicados["produto"]}:
            raise InvalidParameterError(
                f"Produto {produto!r} fora do que a CONAB/PROHORT publica. "
                f"Válidos: {publicados['produto']}"
            )
        df = df[df["produto"].map(_chave_produto) == chave].reset_index(drop=True)

    if ceasa is not None:
        ceasa_upper = ceasa.strip().upper()
        if not any(ceasa_upper in nome.upper() for nome in publicados["ceasa"]):
            raise InvalidParameterError(
                f"CEASA {ceasa!r} fora do que a CONAB/PROHORT publica. "
                f"Válidas: {publicados['ceasa']}"
            )
        df = df[df["ceasa"].str.upper().str.contains(ceasa_upper, regex=False)].reset_index(
            drop=True
        )

    meta = build_source_meta(
        "conab_ceasa",
        source_url,
        "httpx+json",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["conab_prohort"],
        selected_source="conab_prohort",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def produtos() -> list[str]:
    return sorted(PRODUTOS_PROHORT)


def lista_ceasas() -> list[dict[str, str]]:
    result = []
    for nome, uf in sorted(CEASA_UF_MAP.items()):
        result.append({"nome": nome, "uf": uf})
    return result


def categorias() -> dict[str, list[str]]:
    return {k: list(v) for k, v in CATEGORIAS.items()}
