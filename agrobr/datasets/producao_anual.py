"""Dataset producao_anual - Produção agrícola anual consolidada."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, TYPE_CHECKING

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot, is_deterministic
from agrobr.models import MetaInfo

if TYPE_CHECKING:
    pass

logger = structlog.get_logger()


async def _fetch_ibge_pam(
    produto: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    result = await ibge.pam(produto, ano=ano, nivel=nivel, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


async def _fetch_conab(
    produto: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
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
)


class ProducaoAnualDataset(BaseDataset):
    info = PRODUCAO_ANUAL_INFO

    async def fetch(
        self,
        produto: str,
        ano: int | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        """Busca produção anual de um produto agrícola.

        Fontes (em ordem de prioridade):
          1. IBGE PAM (Produção Agrícola Municipal)
          2. CONAB Safras

        Args:
            produto: soja, milho, arroz, feijao, trigo, algodao, cafe
            ano: Ano de referência (opcional, default: último disponível)
            nivel: Nível territorial (brasil, uf, municipio)
            uf: Filtrar por UF (ex: "MT", "PR")
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: ano, produto, localidade, area_plantada, producao, rendimento
        """
        logger.info("dataset_fetch", dataset="producao_anual", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf, **kwargs
        )

        df = self._normalize(df, produto)

        if return_meta:
            meta = MetaInfo(
                source=f"datasets.producao_anual/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else datetime.now(timezone.utc),
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="producao_anual",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
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

from agrobr.datasets.registry import register

register(_producao_anual)


async def producao_anual(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca produção anual de um produto agrícola.

    Fontes (em ordem de prioridade):
      1. IBGE PAM (Produção Agrícola Municipal)
      2. CONAB Safras

    Args:
        produto: soja, milho, arroz, feijao, trigo, algodao, cafe
        ano: Ano de referência (opcional, default: último disponível)
        nivel: Nível territorial (brasil, uf, municipio)
        uf: Filtrar por UF (ex: "MT", "PR")
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: ano, produto, localidade, area_plantada, producao, rendimento
    """
    return await _producao_anual.fetch(
        produto, ano=ano, nivel=nivel, uf=uf, return_meta=return_meta, **kwargs
    )
