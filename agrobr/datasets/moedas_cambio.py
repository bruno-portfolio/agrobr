from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_bcb_ptax_moedas(
    _produto: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    fetched = await bcb.ptax_moedas(as_polars=False, return_meta=True, **kwargs)
    return base._unpack_result(fetched)


MOEDAS_CAMBIO_INFO = base.DatasetInfo(
    name="moedas_cambio",
    description="Catálogo corrente de moedas do serviço PTAX/BCB",
    sources=[
        base.DatasetSource(
            name="bcb_ptax_moedas",
            priority=1,
            fetch_fn=_fetch_bcb_ptax_moedas,
            description="Banco Central do Brasil — catálogo de moedas PTAX",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="unknown",
    typical_latency="conforme catálogo corrente; periodicidade não informada",
    source_url="https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios",
    source_institution="BCB",
    license="livre",
)


class MoedasCambioDataset(base.BaseDataset):
    info = MOEDAS_CAMBIO_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError("moedas_cambio não aceita produto")

    def _contract_name(self, **_kwargs: Any) -> str:
        return "bcb_ptax_moedas"

    async def fetch(
        self,
        produto: str = "",
        *,
        return_meta: bool = False,
        top: int = 1000,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.bcb import ptax_query

        self._validate_produto(produto)
        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em moedas_cambio: {sorted(kwargs)}")
        if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
            raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "moedas_cambio não suporta deterministic: o catálogo corrente não "
                "oferece seleção de uma revisão histórica."
            )
        ptax_query.build_catalog_query(top=top)
        frame, source_name, source_meta, attempted = await self._try_sources("", top=top)
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame, source_name, source_meta, attempted, None, contract_name="bcb_ptax_moedas"
            )
            if return_meta
            else None
        )
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


_moedas_cambio = MoedasCambioDataset()
registry.register(_moedas_cambio)


@overload
async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def moedas_cambio(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _moedas_cambio.fetch(top=top, as_polars=as_polars, return_meta=return_meta)
