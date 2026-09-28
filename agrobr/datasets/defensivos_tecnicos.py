from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _agrofit, base, registry
from agrobr.models import MetaInfo


async def _fetch_defensivos(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import defensivos

    fetched = await defensivos.tecnicos(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


DEFENSIVOS_TECNICOS_INFO = base.DatasetInfo(
    name="defensivos_tecnicos",
    description="Cadastro corrente de produtos técnicos do Agrofit/MAPA",
    sources=[
        base.DatasetSource(
            name="defensivos",
            priority=1,
            fetch_fn=_fetch_defensivos,
            description="Agrofit — Ministério da Agricultura e Pecuária",
        ),
    ],
    products=[],
    contract_version="1.1",
    update_frequency="daily",
    typical_latency="conforme exportação corrente",
    source_url="https://dados.agricultura.gov.br/dataset/sistema-de-agrotoxicos-fitossanitarios-agrofit",
    source_institution="MAPA",
    license="livre",
)


class DefensivosTecnicosDataset(_agrofit.AgrofitDataset):
    info = DEFENSIVOS_TECNICOS_INFO
    source_contract = "agrofit_tecnicos"


_defensivos_tecnicos = DefensivosTecnicosDataset()
registry.register(_defensivos_tecnicos)


@overload
async def defensivos_tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    nr_registro: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def defensivos_tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    nr_registro: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def defensivos_tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    nr_registro: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _defensivos_tecnicos.fetch(
        ingrediente_ativo=ingrediente_ativo,
        titular=titular,
        classe=classe,
        marca=marca,
        nr_registro=nr_registro,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
