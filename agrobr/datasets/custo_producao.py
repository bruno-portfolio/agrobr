"""Dataset custo_producao - Custo de produção agrícola por cultura e UF."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.conab.custo_producao.api import custo_producao as _custo_producao

    uf: str | None = kwargs.get("uf")
    safra: str | None = kwargs.get("safra")
    tecnologia: str = kwargs.get("tecnologia", "alta")

    result = await _custo_producao(
        produto, uf=uf, safra=safra, tecnologia=tecnologia, return_meta=True
    )

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


CUSTO_PRODUCAO_INFO = DatasetInfo(
    name="custo_producao",
    description="Custo de produção agrícola detalhado por cultura, UF e safra",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Custo de Produção (planilhas oficiais)",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao", "cafe"],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="Y+0",
)


class CustoProducaoDataset(BaseDataset):
    info = CUSTO_PRODUCAO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        uf: str | None = None,
        safra: str | None = None,
        tecnologia: str = "alta",
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        """Busca custos de produção agrícola.

        Fontes (em ordem de prioridade):
          1. CONAB Custo de Produção

        Args:
            produto: soja, milho, arroz, feijao, trigo, algodao, cafe
            uf: Filtrar por UF (ex: "MT", "PR")
            safra: Safra (ex: "2024/25"). Se None, mais recente.
            tecnologia: Nível tecnológico ("alta", "media", "baixa")
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: cultura, uf, safra, tecnologia,
            categoria, item, unidade, quantidade_ha, preco_unitario,
            valor_ha, participacao_pct
        """
        logger.info(
            "dataset_fetch",
            dataset="custo_producao",
            produto=produto,
            safra=safra,
            tecnologia=tecnologia,
        )

        snapshot = get_snapshot()
        if snapshot and safra is None:
            ano_snap = int(snapshot[:4])
            safra = f"{ano_snap - 1}/{str(ano_snap)[2:]}"

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, uf=uf, safra=safra, tecnologia=tecnologia, **kwargs
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.custo_producao/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="custo_producao",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "cultura" not in df.columns:
            df["cultura"] = produto

        return df


_custo_producao = CustoProducaoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_custo_producao)


async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca custos de produção agrícola.

    Fontes (em ordem de prioridade):
      1. CONAB Custo de Produção

    Args:
        produto: soja, milho, arroz, feijao, trigo, algodao, cafe
        uf: Filtrar por UF (ex: "MT", "PR")
        safra: Safra (ex: "2024/25"). Se None, mais recente.
        tecnologia: Nível tecnológico ("alta", "media", "baixa")
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: cultura, uf, safra, tecnologia,
        categoria, item, unidade, quantidade_ha, preco_unitario,
        valor_ha, participacao_pct
    """
    return await _custo_producao.fetch(
        produto,
        uf=uf,
        safra=safra,
        tecnologia=tecnologia,
        return_meta=return_meta,
        **kwargs,
    )
