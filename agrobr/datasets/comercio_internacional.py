from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.comtrade import models
from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.utils import result

logger = structlog.get_logger()

_PRODUCTS = sorted(models.HS_PRODUTOS_AGRO)


async def _fetch_comtrade(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import comtrade

    fetched = await comtrade.comercio(produto, return_meta=True, **kwargs)
    return base._unpack_result(fetched)


COMERCIO_INTERNACIONAL_INFO = base.DatasetInfo(
    name="comercio_internacional",
    description="Comércio internacional bilateral de commodities agrícolas — UN Comtrade",
    sources=[
        base.DatasetSource(
            name="comtrade",
            priority=1,
            fetch_fn=_fetch_comtrade,
            description="UN Comtrade — comércio bilateral global por HS code",
        ),
    ],
    products=_PRODUCTS,
    contract_version="2.1",
    update_frequency="monthly",
    typical_latency="M+2",
    source_url="https://comtradeplus.un.org",
    source_institution="United Nations / Comtrade",
    unit="kg / USD",
    license="zona_cinza",
)


class ComercioInternacionalDataset(base.BaseDataset):
    info = COMERCIO_INTERNACIONAL_INFO

    def _validate_produto(self, produto: str) -> None:
        models.resolve_hs(produto)

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta is None:
            return source_name, attempted
        selected = source_meta.selected_source or source_name
        channels = source_meta.attempted_sources or [selected]
        return selected, list(dict.fromkeys(attempted[:-1] + channels))

    def _build_meta(
        self,
        df: pd.DataFrame,
        source_name: str,
        source_meta: MetaInfo | None,
        attempted: list[str],
        snapshot: str | None,
        *,
        from_cache: bool = False,
        contract_name: str | None = None,
    ) -> MetaInfo:
        meta = super()._build_meta(
            df,
            source_name,
            source_meta,
            attempted,
            snapshot,
            from_cache=from_cache,
            contract_name=contract_name,
        )
        if source_meta is not None:
            meta.raw_content_hash = source_meta.raw_content_hash
            meta.raw_content_size = source_meta.raw_content_size
            meta.fetch_duration_ms = source_meta.fetch_duration_ms
            meta.parse_duration_ms = source_meta.parse_duration_ms
        if snapshot:
            meta.source_details["snapshot_scope"] = (
                "default_year_only; source revisions are not frozen"
            )
        return meta

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        reporter: str = "BR",
        partner: str | None = None,
        fluxo: str = "X",
        periodo: str | int | None = None,
        freq: str = "A",
        api_key: str | None = None,
        require_complete: bool = False,
        as_polars: bool = False,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        from agrobr.comtrade import api

        snapshot = get_snapshot()
        if snapshot and periodo is None:
            periodo = snapshot[:4]
        selection = api.prepare_query(
            produto,
            reporter=reporter,
            partner=partner,
            fluxo=fluxo,
            periodo=periodo,
            freq=freq,
            api_key=api_key,
            require_complete=require_complete,
            as_polars=as_polars,
            return_meta=return_meta,
        )
        logger.info(
            "dataset_fetch", dataset=self.info.name, query=selection.model_dump(mode="json")
        )
        frame, source_name, source_meta, attempted = await self._try_sources(
            produto,
            reporter=reporter,
            partner=partner,
            fluxo=selection.flow,
            periodo=selection.requested_period,
            freq=selection.freq,
            api_key=api_key,
            require_complete=require_complete,
        )
        frame = self._normalize(frame)
        self._validate_contract(frame)
        meta = (
            self._build_meta(frame, source_name, source_meta, attempted, snapshot)
            if return_meta
            else None
        )
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_comercio_internacional = ComercioInternacionalDataset()
registry.register(_comercio_internacional)


@overload
async def comercio_internacional(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def comercio_internacional(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def comercio_internacional(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _comercio_internacional.fetch(
        produto,
        reporter=reporter,
        partner=partner,
        fluxo=fluxo,
        periodo=periodo,
        freq=freq,
        api_key=api_key,
        require_complete=require_complete,
        as_polars=as_polars,
        return_meta=return_meta,
    )
