from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.utils import time as time_utils


async def _fetch_bcb_ptax(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    fetched = await bcb.ptax(as_polars=False, return_meta=True, **kwargs)
    return base._unpack_result(fetched)


COTACOES_CAMBIO_INFO = base.DatasetInfo(
    name="cotacoes_cambio",
    description="Cotações e paridades cambiais dos boletins PTAX/BCB",
    sources=[
        base.DatasetSource(
            name="bcb_ptax",
            priority=1,
            fetch_fn=_fetch_bcb_ptax,
            description="Banco Central do Brasil — boletins PTAX",
        ),
    ],
    products=[],
    contract_version="2.0",
    update_frequency="intraday",
    typical_latency="conforme boletins publicados ao longo do dia",
    source_url="https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios",
    source_institution="BCB",
    license="livre",
)


class CotacoesCambioDataset(base.BaseDataset):
    info = COTACOES_CAMBIO_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError("cotacoes_cambio não aceita produto; use moeda")

    def _contract_name(self, **_kwargs: Any) -> str:
        return "bcb_ptax"

    async def fetch(
        self,
        produto: str = "",
        *,
        return_meta: bool = False,
        data: str | date | datetime | None = None,
        inicio: str | date | datetime | None = None,
        fim: str | date | datetime | None = None,
        moeda: str = "USD",
        boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
        top: int = 1000,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.bcb import ptax_query

        self._validate_produto(produto)
        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em cotacoes_cambio: {sorted(kwargs)}")
        if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
            raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "cotacoes_cambio não suporta deterministic: a data do boletim não "
                "seleciona uma revisão histórica da publicação ou do catálogo de moedas."
            )
        ptax_query.build_query(
            data=data,
            data_inicial=inicio,
            data_final=fim,
            moeda=moeda,
            boletim=boletim,
            top=top,
            reference_date=time_utils.hoje(),
        )
        frame, source_name, source_meta, attempted = await self._try_sources(
            "",
            data=data,
            inicio=inicio,
            fim=fim,
            moeda=moeda,
            boletim=boletim,
            top=top,
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame, source_name, source_meta, attempted, None, contract_name="bcb_ptax"
            )
            if return_meta
            else None
        )
        return result.finalize_result(
            frame,
            meta,
            as_polars=as_polars,
            return_meta=return_meta,
            string_columns=("tipo_boletim",),
        )


_cotacoes_cambio = CotacoesCambioDataset()
registry.register(_cotacoes_cambio)


@overload
async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def cotacoes_cambio(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _cotacoes_cambio.fetch(
        data=data,
        inicio=inicio,
        fim=fim,
        moeda=moeda,
        boletim=boletim,
        top=top,
        as_polars=as_polars,
        return_meta=return_meta,
    )
