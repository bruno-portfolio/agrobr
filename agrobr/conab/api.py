from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.keys import build_cache_key
from agrobr.cache.policies import calculate_expiry
from agrobr.conab import client
from agrobr.conab.parsers.v1 import ConabParserV1
from agrobr.models import MetaInfo

logger = structlog.get_logger()


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="conab",
        source_url="https://www.conab.gov.br/info-agro/safras/graos",
        source_method="httpx",
        fetched_at=datetime.now(),
    )

    logger.info(
        "conab_safras_request",
        produto=produto,
        safra=safra,
        uf=uf,
        levantamento=levantamento,
    )

    parse_start = time.perf_counter()
    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra, levantamento=levantamento)

    if isinstance(xlsx, bytes):
        meta.raw_content_size = len(xlsx)
    elif hasattr(xlsx, "getbuffer"):
        meta.raw_content_size = len(xlsx.getbuffer())
    else:
        meta.raw_content_size = 0
    meta.source_url = metadata.get("url", meta.source_url)

    parser = ConabParserV1()
    safra_list = parser.parse_safra_produto(
        xlsx=xlsx,
        produto=produto,
        safra_ref=safra or metadata["safra"],
        levantamento=metadata.get("levantamento"),
    )

    meta.parse_duration_ms = int((time.perf_counter() - parse_start) * 1000)
    meta.parser_version = parser.version

    safra_list = [s for s in safra_list if s.uf is not None]

    if uf:
        safra_list = [s for s in safra_list if s.uf == uf.upper()]

    if not safra_list:
        logger.warning(
            "conab_safras_empty",
            produto=produto,
            safra=safra,
            uf=uf,
        )
        df = pd.DataFrame()
        if return_meta:
            meta.records_count = 0
            meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
            return df, meta
        return df

    df = pd.DataFrame([s.model_dump() for s in safra_list])

    for col in ("area_plantada", "produtividade", "producao"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "area_colhida" not in df.columns:
        df["area_colhida"] = df.get("area_plantada")

    contract_cols = [
        "fonte",
        "produto",
        "safra",
        "uf",
        "area_plantada",
        "area_colhida",
        "produtividade",
        "producao",
        "levantamento",
        "data_publicacao",
    ]
    df = df[[c for c in contract_cols if c in df.columns]]

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "conab:safras",
        {"produto": produto, "safra": safra or "latest"},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry(constants.Fonte.CONAB)

    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta  # type: ignore[return-value,no-any-return]
            return result_df  # type: ignore[return-value,no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_safras_success",
        produto=produto,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    logger.info(
        "conab_balanco_request",
        produto=produto,
        safra=safra,
    )

    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra)

    parser = ConabParserV1()
    suprimentos = parser.parse_suprimento(xlsx=xlsx, produto=produto)

    if not suprimentos:
        logger.warning(
            "conab_balanco_empty",
            produto=produto,
        )
        return pd.DataFrame()

    df = pd.DataFrame(suprimentos)

    if "suprimento_total" in df.columns:
        df = df.rename(columns={"suprimento_total": "suprimento"})

    for col in (
        "estoque_inicial",
        "producao",
        "importacao",
        "suprimento",
        "consumo",
        "exportacao",
        "estoque_final",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if as_polars:
        try:
            import polars as pl

            return pl.from_pandas(df)  # type: ignore[no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_balanco_success",
        produto=produto,
        records=len(df),
    )

    return df


async def brasil_total(
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    logger.info(
        "conab_brasil_total_request",
        safra=safra,
    )

    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra)

    parser = ConabParserV1()
    totais = parser.parse_brasil_total(xlsx=xlsx, safra_ref=safra)

    if not totais:
        logger.warning("conab_brasil_total_empty", safra=safra)
        return pd.DataFrame()

    df = pd.DataFrame(totais)

    if as_polars:
        try:
            import polars as pl

            return pl.from_pandas(df)  # type: ignore[no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_brasil_total_success",
        records=len(df),
    )

    return df


async def levantamentos() -> list[dict[str, Any]]:
    return await client.list_levantamentos()


async def produtos() -> list[str]:
    return list(constants.CONAB_PRODUTOS.keys())


async def ufs() -> list[str]:
    return constants.CONAB_UFS.copy()
