from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _rnc, base, registry
from agrobr.models import MetaInfo


async def _fetch_rnc(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import rnc

    fetched = await rnc.registradas(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


CULTIVARES_REGISTRADAS_INFO = base.DatasetInfo(
    name="cultivares_registradas",
    description="Cadastro corrente de cultivares registradas no RNC/MAPA",
    sources=[
        base.DatasetSource(
            name="rnc",
            priority=1,
            fetch_fn=_fetch_rnc,
            description="CultivarWeb — Ministério da Agricultura e Pecuária",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="continuous",
    typical_latency="conforme exportação corrente",
    source_url="https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php",
    source_institution="MAPA",
    license="livre",
)


class CultivaresRegistradasDataset(_rnc.CultivaresDataset):
    info = CULTIVARES_REGISTRADAS_INFO
    source_contract = "rnc_registradas"


_cultivares_registradas = CultivaresRegistradasDataset()
registry.register(_cultivares_registradas)


@overload
async def cultivares_registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cultivares_registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cultivares_registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _cultivares_registradas.fetch(
        cultivar=cultivar,
        especie=especie,
        grupo=grupo,
        situacao=situacao,
        mantenedor=mantenedor,
        nr_registro=nr_registro,
        nr_formulario=nr_formulario,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
