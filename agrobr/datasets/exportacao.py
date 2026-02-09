"""Dataset exportacao - Exportações agrícolas brasileiras."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_comexstat(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import comexstat

    ano = kwargs.get("ano")
    uf = kwargs.get("uf")

    result = await comexstat.exportacao(produto, ano=ano, uf=uf, return_meta=True)

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


async def _fetch_abiove(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import abiove

    ano: int = kwargs.get("ano", datetime.now(UTC).year)
    mes: int | None = kwargs.get("mes")

    result = await abiove.exportacao(ano=ano, mes=mes, produto=produto, return_meta=True)

    if isinstance(result, tuple):
        df, meta = result
        # Normalizar colunas para contrato do dataset
        rename: dict[str, str] = {}
        if "volume_ton" in df.columns and "kg_liquido" not in df.columns:
            df["kg_liquido"] = df["volume_ton"] * 1000
        if "receita_usd_mil" in df.columns and "valor_fob_usd" not in df.columns:
            df["valor_fob_usd"] = df["receita_usd_mil"] * 1000
        if rename:
            df = df.rename(columns=rename)
        return df, meta
    return result, None


EXPORTACAO_INFO = DatasetInfo(
    name="exportacao",
    description="Exportações agrícolas brasileiras por produto, UF e mês",
    sources=[
        DatasetSource(
            name="comexstat",
            priority=1,
            fetch_fn=_fetch_comexstat,
            description="ComexStat/MDIC (dados oficiais de comércio exterior)",
        ),
        DatasetSource(
            name="abiove",
            priority=2,
            fetch_fn=_fetch_abiove,
            description="ABIOVE (complexo soja — farelo, óleo, grão)",
        ),
    ],
    products=["soja", "milho", "cafe", "algodao", "acucar", "farelo_soja", "oleo_soja"],
    contract_version="1.0",
    update_frequency="monthly",
    typical_latency="M+1",
)


class ExportacaoDataset(BaseDataset):
    info = EXPORTACAO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        uf: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        """Busca dados de exportação agrícola brasileira.

        Fontes (em ordem de prioridade):
          1. ComexStat/MDIC (dados oficiais)
          2. ABIOVE (complexo soja)

        Args:
            produto: soja, milho, cafe, algodao, acucar, farelo_soja, oleo_soja
            ano: Ano de referência (default: ano corrente)
            uf: Filtrar por UF de origem (ex: "MT", "PR")
            return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

        Returns:
            DataFrame com colunas: ano, mes, produto, uf, kg_liquido, valor_fob_usd
        """
        logger.info("dataset_fetch", dataset="exportacao", produto=produto, ano=ano)

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
                source=f"datasets.exportacao/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="exportacao",
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

        return df


_exportacao = ExportacaoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_exportacao)


async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca dados de exportação agrícola brasileira.

    Fontes (em ordem de prioridade):
      1. ComexStat/MDIC (dados oficiais)
      2. ABIOVE (complexo soja)

    Args:
        produto: soja, milho, cafe, algodao, acucar, farelo_soja, oleo_soja
        ano: Ano de referência (default: ano corrente)
        uf: Filtrar por UF de origem (ex: "MT", "PR")
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com colunas: ano, mes, produto, uf, kg_liquido, valor_fob_usd
    """
    return await _exportacao.fetch(produto, ano=ano, uf=uf, return_meta=return_meta, **kwargs)
