from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.conab._serie_historica.client import _PRODUCT_REGISTRY, SERIES_HISTORICAS_URL
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils

logger = _log.get_logger(__name__)

_PRODUCTS = sorted(_PRODUCT_REGISTRY.keys())


async def _fetch_conab_serie(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    ano_inicio = kwargs.get("ano_inicio")
    ano_fim = kwargs.get("ano_fim")
    uf = kwargs.get("uf")

    result = await conab.serie_historica(
        produto, ano_inicio=ano_inicio, ano_fim=ano_fim, uf=uf, return_meta=True
    )

    return _unpack_result(result)


SERIE_HISTORICA_SAFRA_INFO = DatasetInfo(
    name="serie_historica_safra",
    description="Série histórica de safras por produto, safra, região e UF (CONAB)",
    sources=[
        DatasetSource(
            name="conab_serie_historica",
            priority=1,
            fetch_fn=_fetch_conab_serie,
            description="CONAB Séries Históricas de Safras",
        ),
    ],
    products=_PRODUCTS,
    contract_version="1.1",
    update_frequency="yearly",
    typical_latency="safra+6 meses",
    source_url=SERIES_HISTORICAS_URL,
    source_institution="CONAB",
    min_date="1976/77",
    unit="mil ha / mil ton / kg/ha",
    license="livre",
)


class SerieHistoricaSafraDataset(BaseDataset):
    info = SERIE_HISTORICA_SAFRA_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        ano_inicio: int | None = None,
        ano_fim: int | None = None,
        uf: str | None = None,
        return_meta: bool = False,
    ) -> result_utils.DataFrameResult:
        produto = self._produto_do_dataset(produto)
        logger.info(
            "dataset_fetch",
            dataset="serie_historica_safra",
            produto=produto,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim,
        )

        snapshot = get_snapshot()
        if snapshot and ano_inicio is None:
            ano_inicio = int(snapshot[:4]) - 5

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano_inicio=ano_inicio, ano_fim=ano_fim, uf=uf
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:  # noqa: ARG002
        return df


_serie_historica_safra = SerieHistoricaSafraDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_serie_historica_safra)


@overload
async def serie_historica_safra(
    produto: str,
    *,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def serie_historica_safra(
    produto: str,
    *,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> result_utils.DataFrame: ...


@overload
async def serie_historica_safra(
    produto: str,
    *,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def serie_historica_safra(
    produto: str,
    *,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def serie_historica_safra(
    produto: str,
    *,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> result_utils.DataFrameResult:
    return await _serie_historica_safra.fetch(  # type: ignore[call-arg]
        produto,
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        uf=uf,
        return_meta=return_meta,
        as_polars=as_polars,
    )
