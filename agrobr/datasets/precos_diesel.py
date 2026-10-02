from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_anp(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.alt.anp_diesel import api

    return await api.acquire_prices(produto=produto, **kwargs)


PRECOS_DIESEL_INFO = base.DatasetInfo(
    name="precos_diesel",
    description="Preços semanais de diesel da ANP e médias mensais derivadas",
    sources=[
        base.DatasetSource(
            name="anp_diesel",
            priority=1,
            fetch_fn=_fetch_anp,
            description="ANP — levantamento semanal de preços",
        )
    ],
    products=["DIESEL", "DIESEL S10"],
    contract_version="1.0",
    update_frequency="weekly",
    typical_latency="conforme divulgação semanal da ANP",
    source_url="https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos",
    source_institution="ANP",
    unit="BRL/litro",
    license="livre",
)


class PrecosDieselDataset(base.BaseDataset):
    info = PRECOS_DIESEL_INFO

    async def fetch(
        self,
        produto: str = "DIESEL S10",
        *,
        return_meta: bool = False,
        uf: str | None = None,
        municipio: int | str | None = None,
        inicio: str | date | None = None,
        fim: str | date | None = None,
        agregacao: str = "semanal",
        nivel: str = "municipio",
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.alt.anp_diesel import api

        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em precos_diesel: {sorted(kwargs)}")
        api.validate_output_options(as_polars=as_polars, return_meta=return_meta)
        query = api.normalize_price_query(uf, municipio, produto, inicio, fim, agregacao, nivel)
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "precos_diesel não suporta deterministic: os arquivos correntes não oferecem edição histórica selecionável"
            )
        canonical = query.pop("produto")
        frame, source_name, source_meta, attempted = await self._try_sources(canonical, **query)
        self._validate_contract(frame)
        meta = (
            self._build_meta(frame, source_name, source_meta, attempted, None)
            if return_meta
            else None
        )
        if meta is not None:
            meta.validation_passed = True
            meta.timestamp = datetime.now(UTC)
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


_precos_diesel = PrecosDieselDataset()
registry.register(_precos_diesel)


@overload
async def precos_diesel(
    produto: str = "DIESEL S10",
    *,
    uf: str | None = None,
    municipio: int | str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def precos_diesel(
    produto: str = "DIESEL S10",
    *,
    uf: str | None = None,
    municipio: int | str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def precos_diesel(
    produto: str = "DIESEL S10",
    *,
    uf: str | None = None,
    municipio: int | str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def precos_diesel(
    produto: str = "DIESEL S10",
    *,
    uf: str | None = None,
    municipio: int | str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _precos_diesel.fetch(
        produto,
        uf=uf,
        municipio=municipio,
        inicio=inicio,
        fim=fim,
        agregacao=agregacao,
        nivel=nivel,
        as_polars=as_polars,
        return_meta=return_meta,
    )
