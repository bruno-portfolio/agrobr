from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _agrofit, base, registry
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_defensivos(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import defensivos

    fetched = await defensivos.formulados(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


DEFENSIVOS_FORMULADOS_INFO = base.DatasetInfo(
    name="defensivos_formulados",
    description="Cadastro corrente de produtos formulados do Agrofit/MAPA",
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


class DefensivosFormuladosDataset(_agrofit.AgrofitDataset):
    info = DEFENSIVOS_FORMULADOS_INFO
    source_contract = "agrofit_formulados"


_defensivos_formulados = DefensivosFormuladosDataset()
registry.register(_defensivos_formulados)


@overload
async def defensivos_formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def defensivos_formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def defensivos_formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def defensivos_formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _defensivos_formulados.fetch(
        ingrediente_ativo=ingrediente_ativo,
        classe_toxicologica=classe_toxicologica,
        classe_ambiental=classe_ambiental,
        titular=titular,
        organicos=organicos,
        marca=marca,
        formulacao=formulacao,
        classe=classe,
        nr_registro=nr_registro,
        situacao=situacao,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
