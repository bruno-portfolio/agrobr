from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_ibge_ppm(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    result = await ibge.ppm(produto, ano=ano, nivel=nivel, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


PECUARIA_MUNICIPAL_INFO = DatasetInfo(
    name="pecuaria_municipal",
    description="Efetivo de rebanhos e produção de origem animal por UF ou município",
    sources=[
        DatasetSource(
            name="ibge_ppm",
            priority=1,
            fetch_fn=_fetch_ibge_ppm,
            description="IBGE Pesquisa da Pecuária Municipal",
        ),
    ],
    products=[
        "bovino",
        "bubalino",
        "equino",
        "suino_total",
        "suino_matrizes",
        "caprino",
        "ovino",
        "galinaceos_total",
        "galinhas_poedeiras",
        "codornas",
        "leite",
        "ovos_galinha",
        "ovos_codorna",
        "mel",
        "casulos",
        "la",
    ],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url="https://sidra.ibge.gov.br",
    source_institution="IBGE",
    min_date="1974-01-01",
    unit="cabeças / mil litros / mil dúzias / kg",
    license="livre",
)


class PecuariaMunicipalDataset(BaseDataset):
    info = PECUARIA_MUNICIPAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="pecuaria_municipal", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf, **kwargs
        )

        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.pecuaria_municipal/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="pecuaria_municipal",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df


_pecuaria_municipal = PecuariaMunicipalDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_pecuaria_municipal)


async def pecuaria_municipal(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _pecuaria_municipal.fetch(
        produto, ano=ano, nivel=nivel, uf=uf, return_meta=return_meta, **kwargs
    )
