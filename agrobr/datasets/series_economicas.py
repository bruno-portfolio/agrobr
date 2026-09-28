from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_bcb_sgs(
    _produto: str, *, codigo: int | str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    fetched = await bcb.sgs(codigo, as_polars=False, return_meta=True, **kwargs)
    return base._unpack_result(fetched)


SERIES_ECONOMICAS_INFO = base.DatasetInfo(
    name="series_economicas",
    description="Séries econômicas do Sistema Gerenciador de Séries Temporais do Banco Central",
    sources=[
        base.DatasetSource(
            name="bcb_sgs",
            priority=1,
            fetch_fn=_fetch_bcb_sgs,
            description="Banco Central do Brasil — SGS",
        ),
    ],
    products=[],
    contract_version="2.1",
    update_frequency="varies_by_series",
    typical_latency="conforme a série consultada",
    source_url="https://www3.bcb.gov.br/sgspub/",
    source_institution="BCB",
    license="livre",
)


class SeriesEconomicasDataset(base.BaseDataset):
    info = SERIES_ECONOMICAS_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError("series_economicas não aceita produto; informe codigo")

    def _contract_name(self, **_kwargs: Any) -> str:
        return "bcb_sgs"

    async def fetch(
        self,
        produto: str = "",
        return_meta: bool = False,
        *,
        codigo: int | str | None = None,
        data_inicial: str | None = None,
        data_final: str | None = None,
        ultimos: int | None = None,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        from agrobr.bcb import sgs_query

        self._validate_produto(produto)
        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em series_economicas: {sorted(kwargs)}")
        if codigo is None:
            raise InvalidParameterError(
                "series_economicas exige codigo inteiro positivo ou alias SGS"
            )
        if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
            raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "series_economicas não suporta deterministic: o período seleciona referências "
                "publicadas, não uma revisão histórica do SGS."
            )
        sgs_query.build_query(
            codigo,
            data_inicial=data_inicial,
            data_final=data_final,
            ultimos=ultimos,
            reference_date=datetime.now(UTC).date(),
        )
        frame, source_name, source_meta, attempted = await self._try_sources(
            "", codigo=codigo, data_inicial=data_inicial, data_final=data_final, ultimos=ultimos
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame, source_name, source_meta, attempted, None, contract_name="bcb_sgs"
            )
            if return_meta
            else None
        )
        return result.finalize_result(
            frame,
            meta,
            as_polars=as_polars,
            return_meta=return_meta,
            string_columns=("nome_serie",),
        )


_series_economicas = SeriesEconomicasDataset()
registry.register(_series_economicas)


@overload
async def series_economicas(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def series_economicas(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def series_economicas(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _series_economicas.fetch(
        codigo=codigo,
        data_inicial=data_inicial,
        data_final=data_final,
        ultimos=ultimos,
        as_polars=as_polars,
        return_meta=return_meta,
    )
