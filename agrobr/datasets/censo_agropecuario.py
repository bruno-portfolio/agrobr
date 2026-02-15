from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_ibge_censo_agro(tema: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    uf = kwargs.get("uf")
    nivel = kwargs.get("nivel", "uf")

    result = await ibge.censo_agro(tema, uf=uf, nivel=nivel, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


CENSO_AGROPECUARIO_INFO = DatasetInfo(
    name="censo_agropecuario",
    description="Dados do Censo Agropecuário por tema, categoria e UF",
    sources=[
        DatasetSource(
            name="ibge_censo_agro",
            priority=1,
            fetch_fn=_fetch_ibge_censo_agro,
            description="IBGE Censo Agropecuário via SIDRA",
        ),
    ],
    products=[
        "efetivo_rebanho",
        "uso_terra",
        "lavoura_temporaria",
        "lavoura_permanente",
    ],
    contract_version="1.0",
    update_frequency="decennial",
    typical_latency="Y+2 anos",
    source_url="https://sidra.ibge.gov.br",
    source_institution="IBGE",
    min_date="2006-01-01",
    unit="cabeças / hectares / toneladas",
    license="livre",
)


class CensoAgropecuarioDataset(BaseDataset):
    info = CENSO_AGROPECUARIO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        uf: str | None = None,
        nivel: str = "uf",
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch",
            dataset="censo_agropecuario",
            produto=produto,
            uf=uf,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, uf=uf, nivel=nivel, **kwargs
        )

        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.censo_agropecuario/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="censo_agropecuario",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df


_censo_agropecuario = CensoAgropecuarioDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_censo_agropecuario)


async def censo_agropecuario(
    tema: str,
    uf: str | None = None,
    nivel: str = "uf",
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _censo_agropecuario.fetch(
        tema, uf=uf, nivel=nivel, return_meta=return_meta, **kwargs
    )
