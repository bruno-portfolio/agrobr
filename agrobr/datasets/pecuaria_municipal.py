from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.ibge._helpers import SIDRA_BASE
from agrobr.models import MetaInfo
from agrobr.normalize import regions

logger = structlog.get_logger()


async def _fetch_ibge_ppm(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    result = await ibge.ppm(produto, ano=ano, nivel=nivel, uf=uf, return_meta=True)

    return _unpack_result(result)


PECUARIA_MUNICIPAL_INFO = DatasetInfo(
    name="pecuaria_municipal",
    description="Efetivo de rebanhos e produção de origem animal por UF ou município",
    sources=[
        DatasetSource(
            name="ibge_ppm",
            priority=1,
            fetch_fn=_fetch_ibge_ppm,
            description="IBGE Pesquisa da Pecuária Municipal",
        ),
    ],
    products=[
        "bovino",
        "bubalino",
        "equino",
        "suino_total",
        "suino_matrizes",
        "caprino",
        "ovino",
        "galinaceos_total",
        "galinhas",
        "galinhas_poedeiras",
        "codornas",
        "leite",
        "ovos_galinha",
        "ovos_codorna",
        "mel",
        "casulos",
        "la",
    ],
    contract_version="1.1",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url=SIDRA_BASE,
    source_institution="IBGE",
    min_date="1974-01-01",
    unit="cabeças / mil litros / mil dúzias / kg",
    license="livre",
)


class PecuariaMunicipalDataset(BaseDataset):
    info = PECUARIA_MUNICIPAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        uf: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="pecuaria_municipal", produto=produto, ano=ano)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf
        )

        df = df.assign(cod_municipio=regions.cod_municipio(df["localidade_cod"]))
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df


_pecuaria_municipal = PecuariaMunicipalDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_pecuaria_municipal)


@overload
async def pecuaria_municipal(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def pecuaria_municipal(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pecuaria_municipal(
    produto: str,
    ano: int | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _pecuaria_municipal.fetch(  # type: ignore[call-arg]
        produto, ano=ano, nivel=nivel, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
