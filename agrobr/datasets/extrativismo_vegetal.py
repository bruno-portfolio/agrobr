from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.ibge._helpers import SIDRA_BASE
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_ibge_extracao_vegetal(
    produto: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")
    variavel = kwargs.get("variavel", "quantidade_produzida")

    result = await ibge.extracao_vegetal(
        produto, ano=ano, nivel=nivel, uf=uf, variavel=variavel, return_meta=True
    )

    return _unpack_result(result)


EXTRATIVISMO_VEGETAL_INFO = DatasetInfo(
    name="extrativismo_vegetal",
    description="Produção extrativista vegetal (açaí, castanha, erva-mate, etc.) por UF ou município",
    sources=[
        DatasetSource(
            name="ibge_extracao_vegetal",
            priority=1,
            fetch_fn=_fetch_ibge_extracao_vegetal,
            description="IBGE PEVS Extração Vegetal",
        ),
    ],
    products=[
        "acai",
        "castanha_para",
        "erva_mate",
        "palmito",
        "pequi_fruto",
        "babacu",
        "piacava",
        "carnauba_cera",
        "carvao",
        "lenha",
        "madeira_tora",
        "hevea_coagulado",
    ],
    contract_version="1.1",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url=SIDRA_BASE,
    source_institution="IBGE",
    min_date="1986-01-01",
    unit="Toneladas / Metros cúbicos / Mil Reais",
    license="livre",
)


class ExtrativsmoVegetalDataset(BaseDataset):
    info = EXTRATIVISMO_VEGETAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        *,
        uf: str | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        variavel: str = "quantidade_produzida",
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        produto = self._produto_do_dataset(produto)
        logger.info("dataset_fetch", dataset="extrativismo_vegetal", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf, variavel=variavel
        )

        df = df.assign(cod_municipio=regions.cod_municipio(df["localidade_cod"]))
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_extrativismo_vegetal = ExtrativsmoVegetalDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_extrativismo_vegetal)


@overload
async def extrativismo_vegetal(
    produto: str,
    ano: int | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def extrativismo_vegetal(
    produto: str,
    ano: int | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def extrativismo_vegetal(
    produto: str,
    ano: int | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def extrativismo_vegetal(
    produto: str,
    ano: int | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


async def extrativismo_vegetal(
    produto: str,
    ano: int | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _extrativismo_vegetal.fetch(  # type: ignore[call-arg]
        produto,
        ano=ano,
        nivel=nivel,
        uf=uf,
        variavel=variavel,
        return_meta=return_meta,
        as_polars=as_polars,
    )
