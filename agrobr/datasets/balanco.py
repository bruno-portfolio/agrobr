"""Dataset balanco - Balanço de oferta e demanda."""

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

    df = await conab.balanco(produto=produto, safra=safra)

    meta = MetaInfo(
        source="conab",
        source_url="https://www.conab.gov.br/info-agro/safras/graos",
        source_method="httpx",
        fetched_at=datetime.now(UTC),
    )

    return df, meta


BALANCO_INFO = DatasetInfo(
    name="balanco",
    description="Balanço de oferta e demanda de commodities",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Balanço de Oferta e Demanda",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao"],
    contract_version="1.0",
    update_frequency="monthly",
    typical_latency="M+0",
)


class BalancoDataset(BaseDataset):
    info = BALANCO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        """Busca balanço de oferta e demanda.

        Fontes:
          1. CONAB Balanço de Oferta e Demanda

        Args:
            produto: soja, milho, arroz, feijao, trigo, algodao
            safra: Safra no formato "2024/25" (opcional, default: corrente)
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: safra, produto, estoque_inicial, producao,
                                   importacao, consumo, exportacao, estoque_final
        """
        logger.info("dataset_fetch", dataset="balanco", produto=produto, safra=safra)

        snapshot = get_snapshot()

        df, source_name, source_meta = await self._try_sources(produto, safra=safra, **kwargs)

        df = self._normalize(df, produto)

        if return_meta:
            meta = MetaInfo(
                source=f"datasets.balanco/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else datetime.now(UTC),
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="balanco",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
            )
            return df, meta

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if df.empty:
            return df

        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "conab"

        return df


_balanco = BalancoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_balanco)


async def balanco(
    produto: str,
    safra: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca balanço de oferta e demanda.

    Fontes:
      1. CONAB Balanço de Oferta e Demanda

    Args:
        produto: soja, milho, arroz, feijao, trigo, algodao
        safra: Safra no formato "2024/25" (opcional, default: corrente)
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: safra, produto, estoque_inicial, producao,
                               importacao, consumo, exportacao, estoque_final
    """
    return await _balanco.fetch(produto, safra=safra, return_meta=return_meta, **kwargs)
