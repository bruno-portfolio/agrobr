from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_ibge_abate(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    trimestre = kwargs.get("trimestre")
    uf = kwargs.get("uf")

    result = await ibge.abate(produto, trimestre=trimestre, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


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
    contract_version="1.0",
    update_frequency="quarterly",
    typical_latency="T+2 meses",
    source_url="https://sidra.ibge.gov.br",
    source_institution="IBGE",
    min_date="1997-01-01",
    unit="cabeças / kg",
    license="livre",
)


class AbateTrimestralDataset(BaseDataset):
    info = ABATE_TRIMESTRAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        trimestre: str | list[str] | None = None,
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch", dataset="abate_trimestral", produto=produto, trimestre=trimestre
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, trimestre=trimestre, uf=uf, **kwargs
        )

        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.abate_trimestral/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="abate_trimestral",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df


_abate_trimestral = AbateTrimestralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_abate_trimestral)


async def abate_trimestral(
    produto: str,
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _abate_trimestral.fetch(
        produto, trimestre=trimestre, uf=uf, return_meta=return_meta, **kwargs
    )
