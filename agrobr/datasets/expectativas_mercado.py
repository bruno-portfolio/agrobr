from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_bcb_focus(
    _produto: str, *, indicador: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    fetched = await bcb.focus(indicador, as_polars=False, return_meta=True, **kwargs)
    return base._unpack_result(fetched)


EXPECTATIVAS_MERCADO_INFO = base.DatasetInfo(
    name="expectativas_mercado",
    description="Expectativas anuais e mensais de mercado do Focus/BCB",
    sources=[
        base.DatasetSource(
            name="bcb_focus",
            priority=1,
            fetch_fn=_fetch_bcb_focus,
            description="Banco Central do Brasil — Focus anual/mensal",
        ),
    ],
    products=[],
    contract_version="2.0",
    update_frequency="weekly",
    typical_latency="publicação no primeiro dia útil da semana",
    source_url="https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado",
    source_institution="BCB",
    license="livre",
)


class ExpectativasMercadoDataset(base.BaseDataset):
    info = EXPECTATIVAS_MERCADO_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError("expectativas_mercado não aceita produto; use indicador")

    def _contract_name(self, **_kwargs: Any) -> str:
        return "bcb_focus"

    async def fetch(
        self,
        produto: str = "",
        *,
        return_meta: bool = False,
        indicador: str = "PIB Agropecuária",
        periodicidade: Literal["anual", "mensal"] = "anual",
        top: int = 1000,
        inicio: str | date | datetime | None = None,
        max_registros: int | None = None,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.bcb import focus_query

        self._validate_produto(produto)
        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em expectativas_mercado: {sorted(kwargs)}")
        if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
            raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "expectativas_mercado não suporta deterministic: a data da pesquisa e o "
                "horizonte não selecionam uma revisão histórica do Focus."
            )
        focus_query.build_query(
            indicador,
            periodicidade=periodicidade,
            top=top,
            data_inicial=inicio,
            max_registros=max_registros,
        )
        frame, source_name, source_meta, attempted = await self._try_sources(
            "",
            indicador=indicador,
            periodicidade=periodicidade,
            top=top,
            inicio=inicio,
            max_registros=max_registros,
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame, source_name, source_meta, attempted, None, contract_name="bcb_focus"
            )
            if return_meta
            else None
        )
        return result.finalize_result(
            frame,
            meta,
            as_polars=as_polars,
            return_meta=return_meta,
            string_columns=("indicador_detalhe",),
        )


_expectativas_mercado = ExpectativasMercadoDataset()
registry.register(_expectativas_mercado)


@overload
async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def expectativas_mercado(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _expectativas_mercado.fetch(
        indicador=indicador,
        periodicidade=periodicidade,
        top=top,
        inicio=inicio,
        max_registros=max_registros,
        as_polars=as_polars,
        return_meta=return_meta,
    )
