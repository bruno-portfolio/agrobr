from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.antaq import client, parser
from agrobr.antaq.models import (
    MIN_ANO,
    PARSER_VERSION,
    resolve_natureza_carga,
    resolve_sentido,
    resolve_tipo_navegacao,
)
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import (
    DataFrame,
    DataFrameResult,
    build_source_meta,
    check_polars,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

logger = _log.get_logger(__name__)


def _parse_movimentacao(ano_zip: bytes, merc_zip: bytes, ano: int) -> pd.DataFrame:
    atracacao = parser.parse_atracacao(client.extract_atracacao(ano_zip, ano))
    carga = parser.parse_carga(client.extract_carga(ano_zip, ano))
    mercadoria = parser.parse_mercadoria(client.extract_mercadoria(merc_zip))
    return parser.join_movimentacao(atracacao, carga, mercadoria)


@overload
async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = ...,
    natureza_carga: str | None = ...,
    mercadoria: str | None = ...,
    porto: str | None = ...,
    uf: str | None = ...,
    sentido: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[False] = ...,
) -> DataFrame: ...


@overload
async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = ...,
    natureza_carga: str | None = ...,
    mercadoria: str | None = ...,
    porto: str | None = ...,
    uf: str | None = ...,
    sentido: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    corrente = time_utils.hoje().year
    if not isinstance(ano, int) or isinstance(ano, bool) or not MIN_ANO <= ano <= corrente:
        raise InvalidParameterError(f"Ano deve estar entre {MIN_ANO} e {corrente}, recebido: {ano}")

    uf = validate_uf(uf)
    tipo_nav_filtro = resolve_tipo_navegacao(tipo_navegacao)
    nat_carga_filtro = resolve_natureza_carga(natureza_carga)
    sentido_filtro = resolve_sentido(sentido)
    for nome, valor in (("mercadoria", mercadoria), ("porto", porto)):
        if valor is not None and (not isinstance(valor, str) or not valor.strip()):
            raise InvalidParameterError(f"{nome} deve ser texto não vazio ou None")
    check_polars(as_polars)

    logger.info(
        "antaq_movimentacao",
        ano=ano,
        tipo_navegacao=tipo_nav_filtro,
        natureza_carga=nat_carga_filtro,
        mercadoria=mercadoria,
        porto=porto,
        uf=uf,
    )

    source_url = f"https://estatistica.antaq.gov.br/ea/txt/{ano}.zip"

    t0 = time.monotonic()
    ano_zip = await client.fetch_ano_zip(ano)
    merc_zip = await client.fetch_mercadoria_zip()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = await asyncio.to_thread(_parse_movimentacao, ano_zip, merc_zip, ano)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if tipo_nav_filtro:
        df = df[df["tipo_navegacao"] == tipo_nav_filtro]

    if nat_carga_filtro:
        df = df[df["natureza_carga"] == nat_carga_filtro]

    if mercadoria:
        df = df[df["mercadoria"].str.contains(mercadoria, case=False, na=False, regex=False)]

    if porto:
        df = df[df["porto"].str.contains(porto, case=False, na=False, regex=False)]

    if uf:
        df = df[df["uf"].str.upper() == uf.strip().upper()]

    if sentido_filtro:
        df = df[df["sentido"] == sentido_filtro]

    df = df.reset_index(drop=True)

    logger.info(
        "antaq_movimentacao_ok",
        ano=ano,
        rows=len(df),
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
    )

    meta = build_source_meta(
        "antaq",
        source_url,
        "requests+zip",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["antaq_ea"],
        selected_source="antaq_ea",
        raw_content_hash=hashlib.sha256(ano_zip).hexdigest(),
        raw_content_size=len(ano_zip),
        source_details={
            "mercadoria_url": client.MERCADORIA_URL,
            "mercadoria_sha256": hashlib.sha256(merc_zip).hexdigest(),
            "mercadoria_bytes": len(merc_zip),
        },
    )
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(c for c in df.columns if c not in parser.COLUNAS_TIPADAS),
    )
