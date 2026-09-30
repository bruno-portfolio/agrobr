from __future__ import annotations

from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)

COLUNAS_PT = {
    "commodity_code": "codigo_produto",
    "commodity": "produto",
    "country_code": "codigo_pais",
    "country": "pais",
    "market_year": "ano_comercial",
    "attribute": "atributo",
    "attribute_br": "atributo_br",
    "value": "valor",
    "unit": "unidade",
    "attribute_id": "codigo_atributo",
    "unit_id": "codigo_unidade",
    "last_update_year": "ano_atualizacao",
    "last_update_month": "mes_atualizacao",
}

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

    pais: str | None = kwargs.get("pais", "BR")
    if pais is None:
        pais = "BR"
    ano_comercial: int | None = kwargs.get("ano_comercial")
    atributos: list[str] | None = kwargs.get("atributos")
    pivotar: bool = kwargs.get("pivotar", False)
    api_key: str | None = kwargs.get("api_key")

    result = await usda.psd(
        produto,
        country=pais,
        market_year=ano_comercial,
        attributes=atributos,
        pivot=pivotar,
        api_key=api_key,
        return_meta=True,
    )
    frame, meta = _unpack_result(result)
    return cast("pd.DataFrame", frame), meta


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
    contract_version="2.0",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://apps.fas.usda.gov/psdonline/app/index.html",
    source_institution="USDA/FAS",
    unit="coluna unidade por linha: (1000 MT), (1000 HA), (MT/HA); algodão em 1000 480 lb. Bales; café em (1000 60 KG BAGS)",
    license="livre",
)


class OfertaDemandaGlobalDataset(BaseDataset):
    info = OFERTA_DEMANDA_GLOBAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        pais: str | None = "BR",
        ano_comercial: int | None = None,
        atributos: list[str] | None = None,
        pivotar: bool = False,
        api_key: str | None = None,
        return_meta: bool = False,
    ) -> DataFrameResult:
        logger.info("dataset_fetch", dataset="oferta_demanda_global", produto=produto)

        snapshot = get_snapshot()
        if snapshot and ano_comercial is None:
            ano_comercial = int(snapshot[:4])

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            pais=pais,
            ano_comercial=ano_comercial,
            atributos=atributos,
            pivotar=pivotar,
            api_key=api_key,
        )

        df = self._normalize(df)
        if not pivotar:
            self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=COLUNAS_PT)


_oferta_demanda_global = OfertaDemandaGlobalDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_oferta_demanda_global)


@overload
async def oferta_demanda_global(
    produto: str,
    *,
    pais: str | None = "BR",
    ano_comercial: int | None = None,
    atributos: list[str] | None = None,
    pivotar: bool = False,
    api_key: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def oferta_demanda_global(
    produto: str,
    *,
    pais: str | None = "BR",
    ano_comercial: int | None = None,
    atributos: list[str] | None = None,
    pivotar: bool = False,
    api_key: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def oferta_demanda_global(
    produto: str,
    *,
    pais: str | None = "BR",
    ano_comercial: int | None = None,
    atributos: list[str] | None = None,
    pivotar: bool = False,
    api_key: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def oferta_demanda_global(
    produto: str,
    *,
    pais: str | None = "BR",
    ano_comercial: int | None = None,
    atributos: list[str] | None = None,
    pivotar: bool = False,
    api_key: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    return await _oferta_demanda_global.fetch(  # type: ignore[call-arg]
        produto,
        pais=pais,
        ano_comercial=ano_comercial,
        atributos=atributos,
        pivotar=pivotar,
        api_key=api_key,
        return_meta=return_meta,
        as_polars=as_polars,
    )
