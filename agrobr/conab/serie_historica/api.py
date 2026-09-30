from __future__ import annotations

import time
from datetime import date
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.time import hoje
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

from . import client
from .parser import (
    PARSER_VERSION,
    avisar_soma_das_ufs,
    linhas_brasil,
    parse_serie_historica,
    records_to_dataframe,
)

logger = _log.get_logger(__name__)


def _hoje() -> date:
    return hoje()


def _safras_em_revisao(hoje: date) -> set[str]:
    inicio = hoje.year if hoje.month >= 10 else hoje.year - 1
    return {f"{ano}/{str(ano + 1)[-2:]}" for ano in (inicio - 1, inicio)} | {
        str(inicio),
        str(inicio + 1),
    }


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    client.get_xls_url(produto)
    if inicio is not None and fim is not None and inicio > fim:
        raise InvalidParameterError(f"inicio ({inicio}) posterior a fim ({fim})")
    uf = validate_uf(uf)
    t0 = time.monotonic()

    logger.info(
        "conab_serie_historica_request",
        produto=produto,
        inicio=inicio,
        fim=fim,
        uf=uf,
    )

    xls, metadata = await client.download_xls(produto)

    t1 = time.monotonic()
    records = parse_serie_historica(
        xls=xls,
        produto=produto,
        inicio=inicio,
        fim=fim,
        uf=uf,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    df = records_to_dataframe(records)
    if not df.empty:
        avisar_soma_das_ufs(produto, "série histórica", df, linhas_brasil(xls.getvalue(), produto))
    if metadata.get("categoria") == "graos" and not df.empty:
        em_revisao = sorted(set(df["safra"]) & _safras_em_revisao(_hoje()))
        if em_revisao:
            warn_once(
                f"conab_serie_historica_revisao:{produto}:{','.join(em_revisao)}",
                f"Safra {', '.join(em_revisao)} de {produto}: o levantamento mensal da CONAB "
                "ainda publica e pode revisar esses números, e a série histórica anual pode "
                "estar atrás da revisão. Para o valor vigente, use conab.safras ou "
                "datasets.estimativa_safra; para safras mais antigas, eles já leem esta série.",
            )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        "conab_serie_historica_ok",
        produto=produto,
        records=len(records),
        safras=len(df["safra"].unique()) if not df.empty else 0,
        ufs=len(df["uf"].dropna().unique()) if not df.empty else 0,
    )

    meta = build_source_meta(
        "conab_serie_historica",
        metadata.get("url", client.SERIES_HISTORICAS_URL),
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["conab_serie_historica"],
        selected_source="conab_serie_historica",
    )
    meta.dataset = "serie_historica_safra"
    meta.contract_version = "1.1"
    meta.schema_version = "1.1"
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def produtos_disponiveis() -> list[dict[str, str]]:
    return client.list_produtos()
