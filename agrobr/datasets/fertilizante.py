from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_anda(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import anda

    ano = kwargs.get("ano")
    uf = kwargs.get("uf")

    if ano is None:
        ano = datetime.now(UTC).year

    result = await anda.entregas(ano, produto=produto, uf=uf, return_meta=True)

    return _unpack_result(result)


FERTILIZANTE_INFO = DatasetInfo(
    name="fertilizante",
    description="Entregas de fertilizantes ao mercado brasileiro por UF e mês",
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
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="fertilizante", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4])

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, uf=uf, **kwargs
        )

        df = self._normalize(df)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        if "produto_fertilizante" not in df.columns:
            df["produto_fertilizante"] = "total"

        return df


_fertilizante = FertilizanteDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_fertilizante)


async def fertilizante(
    produto: str = "total",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _fertilizante.fetch(produto, ano=ano, uf=uf, return_meta=return_meta, **kwargs)
