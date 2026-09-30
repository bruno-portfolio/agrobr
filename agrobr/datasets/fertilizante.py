from __future__ import annotations

from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_anda(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import anda

    ano = kwargs.get("ano")

    if ano is None:
        ano = time_utils.hoje().year

    result = await anda.entregas(ano, produto=produto, return_meta=True)

    frame, meta = _unpack_result(result)
    return cast("pd.DataFrame", frame), meta


FERTILIZANTE_INFO = DatasetInfo(
    name="fertilizante",
    description="Entregas mensais de fertilizantes ao mercado brasileiro (total nacional)",
    sources=[
        DatasetSource(
            name="anda",
            priority=1,
            fetch_fn=_fetch_anda,
            description="ANDA (Associação Nacional para Difusão de Adubos)",
        ),
    ],
    products=["total"],
    contract_version="2.0",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url="https://anda.org.br",
    source_institution="ANDA",
    min_date="2009-01-01",
    unit="ton",
    license="zona_cinza",
)


class FertilizanteDataset(BaseDataset):
    info = FERTILIZANTE_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str = "total",
        ano: int | None = None,
        *,
        return_meta: bool = False,
    ) -> DataFrameResult:
        logger.info("dataset_fetch", dataset="fertilizante", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4])

        df, source_name, source_meta, attempted = await self._try_sources(produto, ano=ano)

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_fertilizante = FertilizanteDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_fertilizante)


@overload
async def fertilizante(
    produto: str = "total",
    ano: int | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def fertilizante(
    produto: str = "total",
    ano: int | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def fertilizante(
    produto: str = "total",
    ano: int | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def fertilizante(
    produto: str = "total",
    ano: int | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    return await _fertilizante.fetch(  # type: ignore[call-arg]
        produto, ano=ano, return_meta=return_meta, as_polars=as_polars
    )
