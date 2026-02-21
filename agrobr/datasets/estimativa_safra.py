from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

if TYPE_CHECKING:
    pass

logger = structlog.get_logger()


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")

    result = await conab.safras(produto, safra=safra, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


async def _fetch_ibge_lspa(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")

    if safra:
        ano = int(safra.split("/")[0])
    else:
        from datetime import date

        ano = date.today().year

    result = await ibge.lspa(produto, ano=ano, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


ESTIMATIVA_SAFRA_INFO = DatasetInfo(
    name="estimativa_safra",
    description="Estimativas de safra corrente por UF",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Acompanhamento de Safra",
        ),
        DatasetSource(
            name="ibge_lspa",
            priority=2,
            fetch_fn=_fetch_ibge_lspa,
            description="IBGE LSPA",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao"],
    contract_version="1.0",
    update_frequency="monthly",
    typical_latency="M+0",
    source_url="https://www.gov.br/conab/",
    source_institution="CONAB",
    min_date="2005-01-01",
    unit="mil ha / mil ton / kg/ha",
    license="livre",
)


class EstimativaSafraDataset(BaseDataset):
    info = ESTIMATIVA_SAFRA_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="estimativa_safra", produto=produto, safra=safra)

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, safra=safra, uf=uf, **kwargs
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.estimativa_safra/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="estimativa_safra",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "conab"

        return df


_estimativa_safra = EstimativaSafraDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_estimativa_safra)


async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _estimativa_safra.fetch(
        produto, safra=safra, uf=uf, return_meta=return_meta, **kwargs
    )
