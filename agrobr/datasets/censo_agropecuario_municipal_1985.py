from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.ibge.censo_municipal_1985 import TEMAS_DISPONIVEIS
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)


async def _fetch_ibge_censo_municipal_1985(
    tema: str, **kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.ibge import censo_municipal_1985

    uf = kwargs.get("uf")
    nivel = kwargs.get("nivel")

    result = await censo_municipal_1985.censo_agro_municipal_1985(
        tema, uf=uf, nivel=nivel, return_meta=True
    )

    return _unpack_result(result)


CENSO_AGROPECUARIO_MUNICIPAL_1985_INFO = DatasetInfo(
    name="censo_agropecuario_municipal_1985",
    description=(
        "Censo Agropecuário 1985 municipal — 53 tabelas (67 a 119) dos 28 volumes do IBGE, 1 linha "
        "por casa do PDF com o status de cada uma; valor só quando confirmado pelas somas impressas"
    ),
    sources=[
        DatasetSource(
            name="ibge_censo_agro_municipal_1985",
            priority=1,
            fetch_fn=_fetch_ibge_censo_municipal_1985,
            description="Censo 1985 municipal — pacote do agrobr extraído dos PDFs do IBGE",
        ),
    ],
    products=TEMAS_DISPONIVEIS,
    contract_version="2.0",
    update_frequency="never",
    typical_latency="N/A",
    source_url="https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=768",
    source_institution="IBGE",
    min_date="1985-01-01",
    unit="por coluna (unidade e unidade_lida)",
    license="livre",
)


class CensoAgropecuarioMunicipal1985Dataset(BaseDataset):
    info = CENSO_AGROPECUARIO_MUNICIPAL_1985_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        uf: str | None = None,
        nivel: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch",
            dataset="censo_agropecuario_municipal_1985",
            produto=produto,
            uf=uf,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, uf=uf, nivel=nivel
        )

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(
                df, source_name, source_meta, attempted, snapshot, from_cache=False
            )

        return df


_censo_agropecuario_municipal_1985 = CensoAgropecuarioMunicipal1985Dataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_censo_agropecuario_municipal_1985)


@overload
async def censo_agropecuario_municipal_1985(
    tema: str,
    uf: str | None = None,
    nivel: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def censo_agropecuario_municipal_1985(
    tema: str,
    uf: str | None = None,
    nivel: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def censo_agropecuario_municipal_1985(
    tema: str,
    uf: str | None = None,
    nivel: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _censo_agropecuario_municipal_1985.fetch(  # type: ignore[call-arg]
        tema, uf=uf, nivel=nivel, return_meta=return_meta, as_polars=as_polars
    )
