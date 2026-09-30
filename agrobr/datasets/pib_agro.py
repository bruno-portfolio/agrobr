from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)

PRODUCTS = ["agropecuaria", "industria", "servicos", "pib_total"]


async def _fetch_ibge(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.ibge import pesquisas_api

    trimestre = kwargs.get("trimestre")
    precos = kwargs.get("precos", "corrente")

    result = await pesquisas_api.pib_agro(
        trimestre=trimestre, precos=precos, setor=produto, return_meta=True
    )

    return _unpack_result(result)


PIB_AGRO_INFO = DatasetInfo(
    name="pib_agro",
    description="PIB agropecuário brasileiro por setor e trimestre",
    sources=[
        DatasetSource(
            name="ibge",
            priority=1,
            fetch_fn=_fetch_ibge,
            description="IBGE SIDRA (Contas Nacionais Trimestrais)",
        ),
    ],
    products=PRODUCTS,
    contract_version="1.0",
    update_frequency="quarterly",
    typical_latency="Q+2",
    source_url="https://sidra.ibge.gov.br",
    source_institution="IBGE",
    min_date="1996-01-01",
    unit="R$ (milhões)",
    license="livre",
)


class PibAgroDataset(BaseDataset):
    info = PIB_AGRO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str = "agropecuaria",
        trimestre: str | list[str] | None = None,
        precos: str = "corrente",
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="pib_agro", produto=produto, precos=precos)

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, trimestre=trimestre, precos=precos
        )

        df = self._normalize(df, produto, precos)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame, produto: str, precos: str) -> pd.DataFrame:
        if "setor" not in df.columns:
            df["setor"] = produto

        if "precos" not in df.columns:
            df["precos"] = precos

        return df


_pib_agro = PibAgroDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_pib_agro)


@overload
async def pib_agro(
    produto: str = "agropecuaria",
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def pib_agro(
    produto: str = "agropecuaria",
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pib_agro(
    produto: str = "agropecuaria",
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _pib_agro.fetch(  # type: ignore[call-arg]
        produto, trimestre=trimestre, precos=precos, return_meta=return_meta, as_polars=as_polars
    )
