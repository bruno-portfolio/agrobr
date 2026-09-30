from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils

logger = _log.get_logger(__name__)


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    return _unpack_result(
        await conab.balanco(
            produto=produto,
            safra=kwargs.get("safra"),
            levantamento=kwargs.get("levantamento"),
            return_meta=True,
        )
    )


BALANCO_INFO = DatasetInfo(
    name="balanco",
    description="Balanço de oferta e demanda de commodities",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Balanço de Oferta e Demanda",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao"],
    contract_version="1.1",
    update_frequency="monthly",
    typical_latency="M+0",
    source_url="https://www.gov.br/conab/",
    source_institution="CONAB",
    min_date="2010-01-01",
    unit="mil ton",
    license="livre",
)


class BalancoDataset(BaseDataset):
    info = BALANCO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        *,
        return_meta: bool = False,
        levantamento: int | None = None,
    ) -> result_utils.DataFrameResult:
        logger.info(
            "dataset_fetch",
            dataset="balanco",
            produto=produto,
            safra=safra,
            levantamento=levantamento,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, safra=safra, levantamento=levantamento
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if df.empty:
            return contracts.get_contract(self.info.name).empty_frame()

        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "conab"

        return df


_balanco = BalancoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_balanco)


@overload
async def balanco(
    produto: str,
    safra: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
) -> pd.DataFrame: ...


@overload
async def balanco(
    produto: str,
    safra: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
    levantamento: int | None = None,
) -> result_utils.DataFrame: ...


@overload
async def balanco(
    produto: str,
    safra: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def balanco(
    produto: str,
    safra: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
    levantamento: int | None = None,
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def balanco(
    produto: str,
    safra: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
    levantamento: int | None = None,
) -> result_utils.DataFrameResult:
    return await _balanco.fetch(  # type: ignore[call-arg]
        produto,
        safra=safra,
        return_meta=return_meta,
        as_polars=as_polars,
        levantamento=levantamento,
    )
