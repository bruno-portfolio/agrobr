from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.conab._serie_historica.client import SERIES_HISTORICAS_URL
from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_conab_cana_industria(
    _produto: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    return base._unpack_result(await conab.cana_industria(**kwargs, return_meta=True))


PRODUCAO_ACUCAR_ETANOL_INFO = base.DatasetInfo(
    name="producao_acucar_etanol",
    description=(
        "Produção de açúcar, de etanol de cana e de milho e ATR médio por safra e UF "
        "(série histórica industrial da cana, CONAB)"
    ),
    sources=[
        base.DatasetSource(
            name="conab_cana_industria",
            priority=1,
            fetch_fn=_fetch_conab_cana_industria,
            description="CONAB Séries Históricas — cana-de-açúcar, indústria",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="safra anterior; planilha atualizada nos levantamentos quadrimestrais da cana",
    source_url=f"{SERIES_HISTORICAS_URL}/cana-de-acucar/industria",
    source_institution="CONAB",
    min_date="2005/06",
    unit="mil t (açúcar) / mil litros (etanol) / kg/t de cana (ATR)",
    license="livre",
)


class ProducaoAcucarEtanolDataset(base.BaseDataset):
    info = PRODUCAO_ACUCAR_ETANOL_INFO

    def _validate_produto(self, produto: str) -> None:
        if produto != "":
            raise InvalidParameterError("producao_acucar_etanol não aceita produto")

    async def fetch(  # type: ignore[override]
        self,
        produto: str = "",
        *,
        ano_inicio: int | None = None,
        ano_fim: int | None = None,
        uf: str | None = None,
        return_meta: bool = False,
    ) -> result.DataFrameResult:
        snapshot = get_snapshot()
        frame, source_name, source_meta, attempted = await self._try_sources(
            produto, ano_inicio=ano_inicio, ano_fim=ano_fim, uf=uf
        )
        self._validate_contract(frame)
        if return_meta:
            return frame, self._build_meta(frame, source_name, source_meta, attempted, snapshot)
        return frame


_producao_acucar_etanol = ProducaoAcucarEtanolDataset()
registry.register(_producao_acucar_etanol)


@overload
async def producao_acucar_etanol(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def producao_acucar_etanol(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def producao_acucar_etanol(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def producao_acucar_etanol(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


async def producao_acucar_etanol(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    """Açúcar, etanol de cana e de milho e ATR por safra e UF, nas unidades da CONAB.
    `etanol_total_mil_l` inclui o etanol de milho; `ano_inicio`/`ano_fim` filtram pelo 1º ano
    da safra."""
    return await _producao_acucar_etanol.fetch(  # type: ignore[call-arg]
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        uf=uf,
        as_polars=as_polars,
        return_meta=return_meta,
    )
