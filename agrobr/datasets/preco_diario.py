from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot, is_deterministic
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_cepea(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import cepea

    if is_deterministic():
        raw = await cepea.indicador(produto, offline=True, return_meta=True, **kwargs)
    else:
        raw = await cepea.indicador(produto, return_meta=True, **kwargs)

    return _unpack_result(raw)  # type: ignore[arg-type]


async def _fetch_cache(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, None]:
    from agrobr.cache.duckdb_store import get_store

    store = get_store()

    inicio = kwargs.get("inicio")
    fim = kwargs.get("fim")

    if isinstance(inicio, str):
        inicio = datetime.strptime(inicio, "%Y-%m-%d")
    elif isinstance(inicio, date):
        inicio = datetime.combine(inicio, datetime.min.time())

    if isinstance(fim, str):
        fim = datetime.strptime(fim, "%Y-%m-%d")
    elif isinstance(fim, date):
        fim = datetime.combine(fim, datetime.max.time())

    indicadores = store.indicadores_query(
        produto=produto,
        inicio=inicio,
        fim=fim,
    )

    if not indicadores:
        raise ValueError(f"No cached data for {produto}")

    df = pd.DataFrame(indicadores)
    df["data"] = pd.to_datetime(df["data"])

    return df, None


PRECO_DIARIO_INFO = DatasetInfo(
    name="preco_diario",
    description="Preço diário spot de commodities agrícolas brasileiras",
    sources=[
        DatasetSource(
            name="cepea",
            priority=1,
            fetch_fn=_fetch_cepea,
            description="CEPEA/ESALQ via Notícias Agrícolas",
        ),
        DatasetSource(
            name="cache",
            priority=99,
            fetch_fn=_fetch_cache,
            description="Cache local DuckDB",
        ),
    ],
    products=["soja", "milho", "boi", "cafe", "cafe_robusta", "trigo", "algodao"],
    contract_version="1.0",
    update_frequency="daily",
    typical_latency="D+0",
    source_url="https://cepea.esalq.usp.br",
    source_institution="CEPEA/ESALQ/USP",
    min_date="2004-01-01",
    unit="BRL/unidade",
    license="nc",
)


class PrecoDiarioDataset(BaseDataset):
    info = PRECO_DIARIO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        inicio: str | date | None = None,
        fim: str | date | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="preco_diario", produto=produto)

        snapshot = get_snapshot()
        if snapshot:
            fim = snapshot

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, inicio=inicio, fim=fim, **kwargs
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if snapshot:
            snapshot_date = datetime.strptime(snapshot, "%Y-%m-%d").date()
            df = df[df["data"].dt.date <= snapshot_date]

        if return_meta:
            return df, self._build_meta(
                df,
                source_name,
                source_meta,
                attempted,
                snapshot,
                from_cache=source_name == "cache",
            )

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        required = ["data", "valor", "unidade"]

        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "cepea"

        df = df.sort_values("data", ascending=False).reset_index(drop=True)
        df = df.drop_duplicates(subset=["data", "produto"], keep="first")

        return df


_preco_diario = PrecoDiarioDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_preco_diario)


async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _preco_diario.fetch(
        produto, inicio=inicio, fim=fim, return_meta=return_meta, **kwargs
    )
