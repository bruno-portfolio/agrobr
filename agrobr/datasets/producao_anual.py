from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

if TYPE_CHECKING:
    pass

logger = structlog.get_logger()


async def _fetch_ibge_pam(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    result = await ibge.pam(produto, ano=ano, nivel=nivel, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")

    result = await conab.safras(produto, safra=safra, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        df, meta = result
        df = df.rename(
            columns={
                "area_plantada": "area_plantada",
                "producao": "producao",
                "produtividade": "rendimento",
            }
        )
        return df, meta
    return result, None


PRODUCAO_ANUAL_INFO = DatasetInfo(
    name="producao_anual",
    description="Produção agrícola anual consolidada por UF ou município",
    sources=[
        DatasetSource(
            name="ibge_pam",
            priority=1,
            fetch_fn=_fetch_ibge_pam,
            description="IBGE Produção Agrícola Municipal",
        ),
        DatasetSource(
            name="conab",
            priority=2,
            fetch_fn=_fetch_conab,
            description="CONAB Safras",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao", "cafe"],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url="https://sidra.ibge.gov.br",
    source_institution="IBGE",
    min_date="1974-01-01",
    unit="ha / ton / kg/ha",
    license="livre",
)


class ProducaoAnualDataset(BaseDataset):
    info = PRODUCAO_ANUAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="producao_anual", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf, **kwargs
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.producao_anual/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="producao_anual",
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
            df["fonte"] = "ibge_pam"

        return df


_producao_anual = ProducaoAnualDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_producao_anual)


async def producao_anual(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _producao_anual.fetch(
        produto, ano=ano, nivel=nivel, uf=uf, return_meta=return_meta, **kwargs
    )
