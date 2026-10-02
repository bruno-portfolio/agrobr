from __future__ import annotations

import hashlib
import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import resolve_produto, validate_selection

logger = _log.get_logger(__name__)


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    validate_selection(ano, mes, edicao)
    produto_norm = resolve_produto(produto) if produto else None
    if agregacao not in ("detalhado", "mensal"):
        raise InvalidParameterError(
            f"agregacao deve ser 'detalhado' ou 'mensal', recebido {agregacao!r}"
        )
    warn_once(
        "abiove",
        "ABIOVE: termos de uso não encontrados publicamente. "
        "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
        "Veja https://www.agrobr.dev/docs/licenses/ para detalhes.",
    )

    logger.info(
        "abiove_exportacao",
        ano=ano,
        mes=mes,
        produto=produto,
        agregacao=agregacao,
        edicao=edicao,
    )

    t0 = time.monotonic()
    excel_bytes, source_url, edicao_lida = await client.fetch_exportacao_excel(ano, edicao)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_exportacao_excel(excel_bytes, ano=ano)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if mes is not None:
        df = df[df["mes"] == mes].reset_index(drop=True)

    if produto_norm:
        df = df[df["produto"] == produto_norm].reset_index(drop=True)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    meta = build_source_meta(
        "abiove",
        source_url,
        "httpx+openpyxl",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=hashlib.sha256(excel_bytes).hexdigest(),
        raw_content_size=len(excel_bytes),
        source_details={"edicao": {"arquivo": source_url.rsplit("/", 1)[-1], "mes": edicao_lida}},
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
