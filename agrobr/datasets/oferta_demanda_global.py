from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)

_PRODUCTS = [
    "acucar",
    "algodao",
    "arroz",
    "cafe",
    "farelo_soja",
    "milho",
    "oleo_soja",
    "soja",
    "trigo",
]


async def _fetch_usda_psd(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import usda

    country: str = kwargs.get("country", "BR") or "BR"
    market_year: int | None = kwargs.get("market_year")
    attributes: list[str] | None = kwargs.get("attributes")
    pivot: bool = kwargs.get("pivot", False)
    api_key: str | None = kwargs.get("api_key")

    result = await usda.psd(
        produto,
        country=country,
        market_year=market_year,
        attributes=attributes,
        pivot=pivot,
        api_key=api_key,
        return_meta=True,
    )
    return _unpack_result(result)


OFERTA_DEMANDA_GLOBAL_INFO = DatasetInfo(
    name="oferta_demanda_global",
    description="Oferta e demanda global de commodities agrícolas — USDA PSD",
    sources=[
        DatasetSource(
            name="usda",
            priority=1,
            fetch_fn=_fetch_usda_psd,
            description="USDA Production, Supply and Distribution (PSD)",
        ),
    ],
    products=_PRODUCTS,
    contract_version="1.1",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://apps.fas.usda.gov/psdonline/app/index.html",
    source_institution="USDA/FAS",
    unit="coluna unit por linha: (1000 MT), (1000 HA), (MT/HA); algodão em 1000 480 lb. Bales; café em (1000 60 KG BAGS)",
    license="livre",
)


class OfertaDemandaGlobalDataset(BaseDataset):
    info = OFERTA_DEMANDA_GLOBAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        country: str | None = "BR",
        market_year: int | None = None,
        attributes: list[str] | None = None,
        pivot: bool = False,
        api_key: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="oferta_demanda_global", produto=produto)

        snapshot = get_snapshot()
        if snapshot and market_year is None:
            market_year = int(snapshot[:4])

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            country=country,
            market_year=market_year,
            attributes=attributes,
            pivot=pivot,
            api_key=api_key,
        )

        df = self._normalize(df)
        if not pivot:
            self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_oferta_demanda_global = OfertaDemandaGlobalDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_oferta_demanda_global)


@overload
async def oferta_demanda_global(
    produto: str,
    *,
    country: str | None = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def oferta_demanda_global(
    produto: str,
    *,
    country: str | None = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def oferta_demanda_global(
    produto: str,
    *,
    country: str | None = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _oferta_demanda_global.fetch(  # type: ignore[call-arg]
        produto,
        country=country,
        market_year=market_year,
        attributes=attributes,
        pivot=pivot,
        api_key=api_key,
        return_meta=return_meta,
        as_polars=as_polars,
    )
