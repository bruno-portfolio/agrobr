from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot, is_deterministic
from agrobr.exceptions import SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


def _sem_cache(produto: str) -> bool:
    from agrobr.cache.duckdb_store import get_store

    return get_store().indicadores_ultima_coleta(produto) is None


async def _fetch_cepea(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import cepea

    if is_deterministic():
        snapshot = get_snapshot()
        fim = kwargs.get("fim")
        if snapshot is not None and (fim is None or str(fim) > snapshot):
            kwargs["fim"] = snapshot
        raw = await cepea.indicador(produto, offline=True, return_meta=True, **kwargs)
        if raw[0].empty and _sem_cache(produto):
            raise SourceUnavailableError(
                source="cache",
                last_error=f"modo determinístico: sem dado no cache local para {produto}",
            )
    else:
        raw = await cepea.indicador(produto, return_meta=True, **kwargs)

    return _unpack_result(cast(tuple[pd.DataFrame, MetaInfo], raw))


async def _fetch_cache(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo]:
    from agrobr.cache.duckdb_store import get_store
    from agrobr.cepea import api as cepea_api

    store = get_store()
    inicio, fim = cepea_api._normalize_dates(kwargs.get("inicio"), kwargs.get("fim"))
    indicadores = store.indicadores_query(
        produto=produto,
        inicio=datetime.combine(inicio, datetime.min.time()),
        fim=datetime.combine(fim, datetime.max.time()),
        praca=kwargs.get("praca"),
    )

    if not indicadores:
        raise SourceUnavailableError(source="cache", last_error=f"No cached data for {produto}")

    registros = cepea_api._dicts_to_indicadores(indicadores)
    df = cepea_api._to_dataframe(registros)

    meta = MetaInfo(
        source="cache",
        source_url="",
        source_method="duckdb",
        fetched_at=max(registro.parsed_at for registro in registros),
        fetch_timestamp=max(registro.parsed_at for registro in registros),
        from_cache=True,
        attempted_sources=["cache"],
        selected_source="cache",
        data_sources=sorted(df["fonte"].unique().tolist()),
    )
    cepea_api._registrar_versoes(meta, cepea_api._select_indicadores(registros))
    return df, meta


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
    products=["soja", "milho", "boi", "bezerro", "cafe", "cafe_robusta", "trigo", "algodao"],
    contract_version="1.1",
    update_frequency="daily",
    typical_latency="D+0",
    source_url="https://cepea.esalq.usp.br",
    source_institution="CEPEA/ESALQ/USP",
    min_date="2004-01-01",
    unit="BRL por unidade da coluna unidade; algodão em centavos de BRL por libra-peso (cBRL/lb)",
    license="nc",
)


class PrecoDiarioDataset(BaseDataset):
    info = PRECO_DIARIO_INFO
    honra_deterministico = True

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        inicio: str | date | None = None,
        fim: str | date | None = None,
        *,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        produto = self._produto_do_dataset(produto)
        logger.info("dataset_fetch", dataset="preco_diario", produto=produto)

        snapshot = get_snapshot()
        if snapshot and (fim is None or str(fim) > snapshot):
            fim = snapshot

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, inicio=inicio, fim=fim, **kwargs
        )

        df = self._normalize(df, produto, praca=kwargs.get("praca"))
        self._validate_contract(df)

        if snapshot:
            snapshot_date = datetime.strptime(snapshot, "%Y-%m-%d").date()
            df = df[df["data"].dt.date <= snapshot_date]

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(
        self, df: pd.DataFrame, produto: str, *, praca: str | None = None
    ) -> pd.DataFrame:
        from agrobr.cepea import api as cepea_api
        from agrobr.cepea.parsers import v1
        from agrobr.normalize import regions

        required = ["data", "valor", "unidade"]

        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "cepea"

        df = df.reset_index(drop=True).copy()
        praca_labels = df.get("praca", pd.Series("", index=df.index)).fillna("")
        canonical = praca or v1.PRACAS.get(produto, "")
        order = pd.DataFrame(
            {
                "data": df["data"],
                "produto": df["produto"],
                "praca_slug": praca_labels.map(regions.slugificar_praca),
                "source_priority": df["fonte"].map(cepea_api._source_priority),
                "fonte": df["fonte"],
                "collected_at": pd.to_datetime(
                    df.get("collected_at", pd.Series(pd.NaT, index=df.index)), utc=True
                ),
                "praca_label": praca_labels,
                "valor": df["valor"],
            },
            index=df.index,
        )
        order["canonical"] = order["praca_slug"].eq(regions.slugificar_praca(canonical))
        ordered = order.sort_values(
            [
                "data",
                "produto",
                "canonical",
                "praca_slug",
                "source_priority",
                "fonte",
                "collected_at",
                "praca_label",
                "valor",
            ],
            ascending=[False, True, False, True, True, True, False, True, True],
            kind="stable",
        )
        return (
            df.loc[ordered.index]
            .drop_duplicates(subset=["data", "produto"], keep="first")
            .reset_index(drop=True)
        )


_preco_diario = PrecoDiarioDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_preco_diario)


@overload
async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> DataFrame: ...


@overload
async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult: ...


async def preco_diario(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult:
    return await _preco_diario.fetch(
        produto, inicio=inicio, fim=fim, as_polars=as_polars, return_meta=return_meta, **kwargs
    )
