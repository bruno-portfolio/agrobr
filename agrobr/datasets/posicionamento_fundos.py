from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.cftc.models import CFTC_CONTRACTS
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()

_PRODUCTS = sorted(set(CFTC_CONTRACTS.values()))


async def _fetch_cftc_cot(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import cftc

    result = await cftc.cot(
        produto,
        start=kwargs.get("start"),
        end=kwargs.get("end"),
        combined=kwargs.get("combined", False),
        return_meta=True,
    )
    return _unpack_result(result)


POSICIONAMENTO_FUNDOS_INFO = DatasetInfo(
    name="posicionamento_fundos",
    description="Posicionamento semanal de fundos (managed money) em futuros agro — CFTC COT",
    sources=[
        DatasetSource(
            name="cftc",
            priority=1,
            fetch_fn=_fetch_cftc_cot,
            description="CFTC Commitments of Traders — Disaggregated Report",
        ),
    ],
    products=_PRODUCTS,
    contract_version="1.1",
    update_frequency="weekly",
    typical_latency="D+3",
    source_url="https://publicreporting.cftc.gov",
    source_institution="CFTC",
    min_date="2006-06-13",
    unit="contratos",
    license="livre",
)


class PosicionamentoFundosDataset(BaseDataset):
    info = POSICIONAMENTO_FUNDOS_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        start: str | None = None,
        end: str | None = None,
        combined: bool = False,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="posicionamento_fundos", produto=produto)

        snapshot = get_snapshot()
        if snapshot and end is None:
            end = snapshot

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            start=start,
            end=end,
            combined=combined,
        )

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df


_posicionamento_fundos = PosicionamentoFundosDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_posicionamento_fundos)


@overload
async def posicionamento_fundos(
    produto: str,
    *,
    start: str | None = None,
    end: str | None = None,
    combined: bool = False,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def posicionamento_fundos(
    produto: str,
    *,
    start: str | None = None,
    end: str | None = None,
    combined: bool = False,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def posicionamento_fundos(
    produto: str,
    *,
    start: str | None = None,
    end: str | None = None,
    combined: bool = False,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _posicionamento_fundos.fetch(  # type: ignore[call-arg]
        produto,
        start=start,
        end=end,
        combined=combined,
        return_meta=return_meta,
        as_polars=as_polars,
    )
