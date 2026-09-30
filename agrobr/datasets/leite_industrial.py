from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge._helpers import SIDRA_BASE
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)


async def _fetch_ibge_leite_trimestral(
    produto: str,  # noqa: ARG001
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    trimestre = kwargs.get("trimestre")
    uf = kwargs.get("uf")

    result = await ibge.leite_trimestral(trimestre=trimestre, uf=uf, return_meta=True)

    return _unpack_result(result)


LEITE_INDUSTRIAL_INFO = DatasetInfo(
    name="leite_industrial",
    description="Aquisição e industrialização trimestral de leite por UF",
    sources=[
        DatasetSource(
            name="ibge_leite_trimestral",
            priority=1,
            fetch_fn=_fetch_ibge_leite_trimestral,
            description="IBGE Pesquisa Trimestral do Leite",
        ),
    ],
    products=["leite"],
    contract_version="1.0",
    update_frequency="quarterly",
    typical_latency="T+2 meses",
    source_url=SIDRA_BASE,
    source_institution="IBGE",
    min_date="1997-01-01",
    unit="mil litros / R$/litro",
    license="livre",
)


class LeiteIndustrialDataset(BaseDataset):
    info = LEITE_INDUSTRIAL_INFO

    def _validate_produto(self, produto: str) -> None:
        if produto not in ("leite", *self.info.products):
            raise InvalidParameterError(
                f"Produto '{produto}' não suportado por {self.info.name}. "
                f"Válidos: {self.info.products}"
            )

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        trimestre: str | list[str] | None = None,
        uf: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch",
            dataset="leite_industrial",
            produto=produto,
            trimestre=trimestre,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, trimestre=trimestre, uf=uf
        )

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_leite_industrial = LeiteIndustrialDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_leite_industrial)


@overload
async def leite_industrial(
    produto: str = "leite",
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def leite_industrial(
    produto: str = "leite",
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def leite_industrial(
    produto: str = "leite",
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _leite_industrial.fetch(  # type: ignore[call-arg]
        produto, trimestre=trimestre, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
