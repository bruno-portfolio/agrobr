from __future__ import annotations

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
    resolve_tipo_navegacao,
)
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf

logger = _log.get_logger(__name__)


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
) -> pd.DataFrame: ...


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
) -> tuple[pd.DataFrame, MetaInfo]: ...


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
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    corrente = time_utils.hoje().year
    if ano < MIN_ANO or ano > corrente:
        raise InvalidParameterError(f"Ano deve estar entre {MIN_ANO} e {corrente}, recebido: {ano}")

    uf = validate_uf(uf)
    tipo_nav_filtro = resolve_tipo_navegacao(tipo_navegacao)
    nat_carga_filtro = resolve_natureza_carga(natureza_carga)

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
    atracacao_txt = client.extract_atracacao(ano_zip, ano)
    carga_txt = client.extract_carga(ano_zip, ano)
    mercadoria_txt = client.extract_mercadoria(merc_zip)

    df_atracacao = parser.parse_atracacao(atracacao_txt)
    df_carga = parser.parse_carga(carga_txt)
    df_mercadoria = parser.parse_mercadoria(mercadoria_txt)

    df = parser.join_movimentacao(df_atracacao, df_carga, df_mercadoria)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if tipo_nav_filtro and "tipo_navegacao" in df.columns:
        df = df[df["tipo_navegacao"] == tipo_nav_filtro]

    if nat_carga_filtro and "natureza_carga" in df.columns:
        df = df[df["natureza_carga"] == nat_carga_filtro]

    if mercadoria and "mercadoria" in df.columns:
        df = df[df["mercadoria"].str.contains(mercadoria, case=False, na=False, regex=False)]

    if porto and "porto" in df.columns:
        df = df[df["porto"].str.contains(porto, case=False, na=False, regex=False)]

    if uf and "uf" in df.columns:
        df = df[df["uf"].str.upper() == uf.strip().upper()]

    if sentido and "sentido" in df.columns:
        sentido_map = {
            "embarque": "Embarcados",
            "desembarque": "Desembarcados",
        }
        sentido_val = sentido_map.get(sentido.lower(), sentido)
        df = df[df["sentido"] == sentido_val]

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
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
