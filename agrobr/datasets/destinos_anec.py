from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.anec import models as anec_models
from agrobr.datasets import _anec, base, registry
from agrobr.models import MetaInfo


async def _fetch_anec(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import anec

    result = await anec.destinos(produto=kwargs.pop("produto_filtro"), return_meta=True, **kwargs)
    return base._unpack_result(result)


DESTINOS_ANEC_INFO = base.DatasetInfo(
    name="destinos_anec",
    description="Participação acumulada por destino e edição do boletim ANEC",
    sources=[base.DatasetSource(name="anec", priority=1, fetch_fn=_fetch_anec)],
    products=list(anec_models.DESTINOS_PRODUTOS_PUBLICADOS),
    contract_version="1.0",
    update_frequency="weekly",
    typical_latency="conforme edição",
    source_url="https://www.anec.com.br",
    source_institution="ANEC",
    unit="%",
    license="zona_cinza",
    min_date="2026-01-01",
)


class DestinosANECDataset(_anec.ANECDataset):
    info = DESTINOS_ANEC_INFO


_destinos_anec = DestinosANECDataset()
registry.register(_destinos_anec)


@overload
async def destinos_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def destinos_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def destinos_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _destinos_anec.fetch(
        produto=produto,
        ano=ano,
        semana=semana,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
