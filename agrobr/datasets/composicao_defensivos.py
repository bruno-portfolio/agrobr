from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _agrofit, base, registry
from agrobr.models import MetaInfo


async def _fetch_defensivos(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import defensivos

    fetched = await defensivos.composicao(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


COMPOSICAO_DEFENSIVOS_INFO = base.DatasetInfo(
    name="composicao_defensivos",
    description="Componentes por posição na composição de produtos do Agrofit/MAPA",
    sources=[
        base.DatasetSource(
            name="defensivos",
            priority=1,
            fetch_fn=_fetch_defensivos,
            description="Agrofit — Ministério da Agricultura e Pecuária",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="daily",
    typical_latency="conforme exportação corrente",
    source_url="https://dados.agricultura.gov.br/dataset/sistema-de-agrotoxicos-fitossanitarios-agrofit",
    source_institution="MAPA",
    license="livre",
)


class ComposicaoDefensivosDataset(_agrofit.AgrofitDataset):
    info = COMPOSICAO_DEFENSIVOS_INFO
    source_contract = "agrofit_composicao"


_composicao_defensivos = ComposicaoDefensivosDataset()
registry.register(_composicao_defensivos)


@overload
async def composicao_defensivos(
    *,
    tipo: str = "formulados",
    nr_registro: str | None = None,
    ingrediente_ativo: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def composicao_defensivos(
    *,
    tipo: str = "formulados",
    nr_registro: str | None = None,
    ingrediente_ativo: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def composicao_defensivos(
    *,
    tipo: str = "formulados",
    nr_registro: str | None = None,
    ingrediente_ativo: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _composicao_defensivos.fetch(
        tipo=tipo,
        nr_registro=nr_registro,
        ingrediente_ativo=ingrediente_ativo,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
