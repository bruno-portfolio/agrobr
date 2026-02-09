"""Dataset fertilizante - Entregas de fertilizantes ao mercado brasileiro."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
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

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


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
    products=["total", "npk", "ureia", "map", "dap", "ssp", "tsp", "kcl"],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="Y+1",
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
        """Busca dados de entregas de fertilizantes.

        Fontes (em ordem de prioridade):
          1. ANDA (relatórios anuais de entregas)

        Args:
            produto: total, npk, ureia, map, dap, ssp, tsp, kcl
            ano: Ano de referência (default: ano corrente)
            uf: Filtrar por UF (ex: "MT", "PR")
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: ano, mes, uf, produto_fertilizante, volume_ton
        """
        logger.info("dataset_fetch", dataset="fertilizante", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4])

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, uf=uf, **kwargs
        )

        df = self._normalize(df, produto)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.fertilizante/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="fertilizante",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto_fertilizante" not in df.columns:
            df["produto_fertilizante"] = produto

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
    """Busca dados de entregas de fertilizantes.

    Fontes (em ordem de prioridade):
      1. ANDA (relatórios anuais de entregas)

    Args:
        produto: total, npk, ureia, map, dap, ssp, tsp, kcl
        ano: Ano de referência (default: ano corrente)
        uf: Filtrar por UF (ex: "MT", "PR")
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: ano, mes, uf, produto_fertilizante, volume_ton
    """
    return await _fertilizante.fetch(produto, ano=ano, uf=uf, return_meta=return_meta, **kwargs)
