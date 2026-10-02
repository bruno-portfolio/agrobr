from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.ibge._helpers import SIDRA_BASE
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_ibge_abate(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    trimestre = kwargs.get("trimestre")
    uf = kwargs.get("uf")

    result = await ibge.abate(produto, trimestre=trimestre, uf=uf, return_meta=True)

    return _unpack_result(result)


ABATE_TRIMESTRAL_INFO = DatasetInfo(
    name="abate_trimestral",
    description="Abate de animais por espécie, trimestre e UF",
    sources=[
        DatasetSource(
            name="ibge_abate",
            priority=1,
            fetch_fn=_fetch_ibge_abate,
            description="IBGE Pesquisa Trimestral do Abate de Animais",
        ),
    ],
    products=[
        "bovino",
        "suino",
        "frango",
    ],
    contract_version="2.0",
    update_frequency="quarterly",
    typical_latency="T+2 meses",
    source_url=SIDRA_BASE,
    source_institution="IBGE",
    min_date="1997-01-01",
    unit="cabeças / kg",
    license="livre",
)


class AbateTrimestralDataset(BaseDataset):
    info = ABATE_TRIMESTRAL_INFO
    sinonimos_que_mudam_o_recorte = frozenset({"frango_congelado"})

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        trimestre: str | list[str] | None = None,
        *,
        uf: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch", dataset="abate_trimestral", produto=produto, trimestre=trimestre
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, trimestre=trimestre, uf=uf
        )

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_abate_trimestral = AbateTrimestralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_abate_trimestral)


@overload
async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _abate_trimestral.fetch(  # type: ignore[call-arg]
        produto, trimestre=trimestre, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
