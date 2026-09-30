from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import CULTURAS_VALIDAS, ESTADO_MEDIA, normalizar_cultura

logger = _log.get_logger(__name__)


@overload
async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info(
        "conab_progresso_safra",
        cultura=cultura,
        estado=estado,
        operacao=operacao,
    )

    t0 = time.monotonic()
    if semana_url:
        xlsx_bytes, source_url = await client.fetch_xlsx_semanal(semana_url)
        desc = semana_url
    else:
        xlsx_bytes, source_url, desc = await client.fetch_latest()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_progresso_xlsx(xlsx_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if cultura is not None:
        cultura_norm = normalizar_cultura(cultura)
        if cultura_norm in CULTURAS_VALIDAS:
            df = df[df["cultura"] == cultura_norm].reset_index(drop=True)
        else:
            df = df[
                df["cultura"].str.lower().str.contains(cultura.lower(), na=False, regex=False)
            ].reset_index(drop=True)

    if estado is not None:
        estado_upper = estado.strip().upper()
        selecionado = df[df["estado"].str.upper() == estado_upper]
        media = df[df["estado"] == ESTADO_MEDIA]
        if estado_upper == "BR" and selecionado.empty and not media.empty:
            coberturas = "; ".join(
                f"{linha.cultura} {linha.operacao}: {linha.n_estados} estados, "
                f"{linha.cobertura_area_pct:.1%} da área"
                for linha in media.drop_duplicates(["cultura", "operacao"]).itertuples()
            )
            raise InvalidParameterError(
                "A CONAB não publica Brasil no progresso de safra: a planilha traz a média da "
                f"própria CONAB dos estados monitorados ({coberturas}). "
                f"Use estado={ESTADO_MEDIA!r}."
            )
        df = selecionado.reset_index(drop=True)

    if operacao is not None:
        op_title = operacao.strip().title()
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
