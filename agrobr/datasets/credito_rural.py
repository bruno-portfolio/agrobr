"""Dataset credito_rural - Crédito rural SICOR por UF/município."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_bcb_odata(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    safra = kwargs.get("safra")
    finalidade = kwargs.get("finalidade", "custeio")
    uf = kwargs.get("uf")
    agregacao = kwargs.get("agregacao", "municipio")

    result = await bcb.credito_rural(
        produto,
        safra=safra,
        finalidade=finalidade,
        uf=uf,
        agregacao=agregacao,
        return_meta=True,
    )

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


CREDITO_RURAL_INFO = DatasetInfo(
    name="credito_rural",
    description="Crédito rural SICOR/BCB por UF ou município, com fallback BigQuery",
    sources=[
        DatasetSource(
            name="bcb",
            priority=1,
            fetch_fn=_fetch_bcb_odata,
            description="BCB API Olinda (OData) com fallback BigQuery",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao", "cafe", "cana", "sorgo"],
    contract_version="1.0",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://olinda.bcb.gov.br",
    source_institution="BCB/SICOR",
    min_date="2013-01-01",
    unit="BRL",
    license="livre",
)


class CreditoRuralDataset(BaseDataset):
    info = CREDITO_RURAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        finalidade: str = "custeio",
        uf: str | None = None,
        agregacao: Literal["municipio", "uf"] = "municipio",
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        """Busca dados de crédito rural do SICOR/BCB.

        Fontes (em ordem de prioridade):
          1. BCB API Olinda (OData) com fallback BigQuery

        Args:
            produto: soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo
            safra: Safra (ex: "2024/25"). Se None, mais recente disponível.
            finalidade: custeio, investimento, comercializacao
            uf: Filtrar por UF (ex: "MT", "PR")
            agregacao: "municipio" (padrão) ou "uf"
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: safra, uf, produto, valor,
            area_financiada, qtd_contratos
        """
        logger.info(
            "dataset_fetch",
            dataset="credito_rural",
            produto=produto,
            safra=safra,
            finalidade=finalidade,
        )

        snapshot = get_snapshot()
        if snapshot and safra is None:
            ano_snap = int(snapshot[:4])
            safra = f"{ano_snap - 1}/{ano_snap}"

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, safra=safra, finalidade=finalidade, uf=uf, agregacao=agregacao, **kwargs
        )

        df = self._normalize(df, produto, finalidade)
        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.credito_rural/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="credito_rural",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df

    def _normalize(self, df: pd.DataFrame, produto: str, finalidade: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = produto

        if "finalidade" not in df.columns:
            df["finalidade"] = finalidade

        return df


_credito_rural = CreditoRuralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_credito_rural)


async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["municipio", "uf"] = "municipio",
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca dados de crédito rural do SICOR/BCB.

    Fontes (em ordem de prioridade):
      1. BCB API Olinda (OData) com fallback BigQuery

    Args:
        produto: soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo
        safra: Safra (ex: "2024/25"). Se None, mais recente disponível.
        finalidade: custeio, investimento, comercializacao
        uf: Filtrar por UF (ex: "MT", "PR")
        agregacao: "municipio" (padrão) ou "uf"
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: safra, uf, produto, valor,
        area_financiada, qtd_contratos
    """
    return await _credito_rural.fetch(
        produto,
        safra=safra,
        finalidade=finalidade,
        uf=uf,
        agregacao=agregacao,
        return_meta=return_meta,
        **kwargs,
    )
