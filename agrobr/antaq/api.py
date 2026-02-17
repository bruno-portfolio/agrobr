"""API pública do módulo ANTAQ — movimentação portuária.

Dados: Estatístico Aquaviário (ANTAQ)
URL: https://web3.antaq.gov.br/ea/sense/download.html
Licença: livre (dados públicos governo federal)
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.antaq import client, parser
from agrobr.antaq.models import (
    MAX_ANO_DEFAULT,
    MIN_ANO,
    PARSER_VERSION,
    resolve_natureza_carga,
    resolve_tipo_navegacao,
)
from agrobr.models import MetaInfo

logger = structlog.get_logger()


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
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Obtém movimentação portuária de carga de um ano.

    Args:
        ano: Ano dos dados (2010 a 2025).
        tipo_navegacao: Filtro por tipo de navegação.
            Valores: longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario.
        natureza_carga: Filtro por natureza da carga.
            Valores: granel_solido, granel_liquido, carga_geral, conteiner.
        mercadoria: Filtro por mercadoria (substring case-insensitive).
        porto: Filtro por nome do porto (substring case-insensitive).
        uf: Filtro por UF do porto (ex: SP, RJ, PR).
        sentido: Filtro por sentido (embarque ou desembarque).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com movimentação portuária, ou tupla (DataFrame, MetaInfo).

    Raises:
        ValueError: Se ano fora do range válido.
        SourceUnavailableError: Se não conseguir baixar dados.

    Example:
        >>> import agrobr
        >>> df = await agrobr.antaq.movimentacao(2024, uf="SP")
        >>> df.head()
    """
    if ano < MIN_ANO or ano > MAX_ANO_DEFAULT:
        raise ValueError(f"Ano deve estar entre {MIN_ANO} e {MAX_ANO_DEFAULT}, recebido: {ano}")

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

    source_url = f"https://web3.antaq.gov.br/ea/txt/{ano}.zip"

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
        df = df[df["mercadoria"].str.contains(mercadoria, case=False, na=False)]

    if porto and "porto" in df.columns:
        df = df[df["porto"].str.contains(porto, case=False, na=False)]

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

    if return_meta:
        meta = MetaInfo(
            source="antaq",
            source_url=source_url,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["antaq_ea"],
            selected_source="antaq_ea",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
