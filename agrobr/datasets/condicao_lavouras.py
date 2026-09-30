from __future__ import annotations

from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.deral import models as deral_models
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult

logger = _log.get_logger(__name__)

_PRODUCTS = sorted(deral_models.DERAL_PRODUTOS_PUBLICADOS)


async def _fetch_deral(
    produto: str,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import deral

    result = await deral.condicao_lavouras(
        produto=produto or None,
        return_meta=True,
    )
    return _unpack_result(result)


CONDICAO_LAVOURAS_INFO = DatasetInfo(
    name="condicao_lavouras",
    description="Condição das lavouras paranaenses — SEAB/DERAL",
    sources=[
        DatasetSource(
            name="deral",
            priority=1,
            fetch_fn=_fetch_deral,
            description="SEAB/DERAL — Secretaria de Agricultura do Paraná",
        ),
    ],
    products=_PRODUCTS,
    contract_version="2.0",
    update_frequency="weekly",
    typical_latency="D+3",
    source_url="https://www.agricultura.pr.gov.br/deral",
    source_institution="SEAB/DERAL",
    unit="%",
    license="livre",
)


class CondicaoLavourasDataset(BaseDataset):
    info = CONDICAO_LAVOURAS_INFO

    def _validate_produto(self, produto: str) -> None:
        if produto:
            deral_models.validate_produto(produto)

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if produto is not None:
            deral_models.validate_produto(produto)
        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="condicao_lavouras",
            produto=produto,
        )

        df, source_name, source_meta, attempted = await self._try_sources(produto or "")

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df


_condicao_lavouras = CondicaoLavourasDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_condicao_lavouras)


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _condicao_lavouras.fetch(  # type: ignore[call-arg]
        produto,
        return_meta=return_meta,
        as_polars=as_polars,
    )
