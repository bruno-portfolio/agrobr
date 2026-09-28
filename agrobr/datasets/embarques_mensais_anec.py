from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _anec, base, registry
from agrobr.models import MetaInfo


async def _fetch_anec(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import anec

    result = await anec.embarques_mensais(
        produto=kwargs.pop("produto_filtro"), return_meta=True, **kwargs
    )
    return base._unpack_result(result)


EMBARQUES_MENSAIS_ANEC_INFO = base.DatasetInfo(
    name="embarques_mensais_anec",
    description="Embarques mensais e estimativas por edição do boletim ANEC",
    sources=[base.DatasetSource(name="anec", priority=1, fetch_fn=_fetch_anec)],
    products=["soybean", "soybean_meal", "maize", "wheat", "ddgs", "sorghum"],
    contract_version="1.0",
    update_frequency="weekly",
    typical_latency="conforme edição",
    source_url="https://www.anec.com.br",
    source_institution="ANEC",
    unit="ton",
    license="zona_cinza",
    min_date="2026-01-01",
)


class EmbarquesMensaisANECDataset(_anec.ANECDataset):
    info = EMBARQUES_MENSAIS_ANEC_INFO


_embarques_mensais_anec = EmbarquesMensaisANECDataset()
registry.register(_embarques_mensais_anec)


@overload
async def embarques_mensais_anec(
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
async def embarques_mensais_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def embarques_mensais_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _embarques_mensais_anec.fetch(
        produto=produto,
        ano=ano,
        semana=semana,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
