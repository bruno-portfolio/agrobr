from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_ceasa(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.conab import ceasa

    ceasa_param = kwargs.get("ceasa")
    produto_param = produto if produto else None

    result = await ceasa.precos(produto=produto_param, ceasa=ceasa_param, return_meta=True)

    return _unpack_result(result)


PRECO_ATACADO_INFO = DatasetInfo(
    name="preco_atacado",
    description="Preços de atacado em CEASAs brasileiras (CONAB/PROHORT)",
    sources=[
        DatasetSource(
            name="conab_ceasa",
            priority=1,
            fetch_fn=_fetch_ceasa,
            description="CONAB CEASA/PROHORT (preços diários de hortifrúti)",
        ),
    ],
    products=[],
    contract_version="1.1",
    update_frequency="daily",
    typical_latency="D+1",
    source_url="http://dw.ceasa.gov.br",
    source_institution="CONAB/PROHORT",
    unit="BRL/unidade",
    license="zona_cinza",
)


class PrecoAtacadoDataset(BaseDataset):
    info = PRECO_ATACADO_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        ceasa: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch",
            dataset="preco_atacado",
            produto=produto,
            ceasa=ceasa,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto or "", ceasa=ceasa
        )

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_preco_atacado = PrecoAtacadoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_preco_atacado)


@overload
async def preco_atacado(
    produto: str | None = None,
    *,
    ceasa: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def preco_atacado(
    produto: str | None = None,
    *,
    ceasa: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def preco_atacado(
    produto: str | None = None,
    *,
    ceasa: str | None = None,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def preco_atacado(
    produto: str | None = None,
    *,
    ceasa: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


async def preco_atacado(
    produto: str | None = None,
    *,
    ceasa: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _preco_atacado.fetch(  # type: ignore[call-arg]
        produto, ceasa=ceasa, return_meta=return_meta, as_polars=as_polars
    )
