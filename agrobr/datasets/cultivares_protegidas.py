from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _rnc, base, registry
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_rnc(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import rnc

    fetched = await rnc.protegidas(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


CULTIVARES_PROTEGIDAS_INFO = base.DatasetInfo(
    name="cultivares_protegidas",
    description="Cadastro corrente de cultivares protegidas no SNPC/MAPA",
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
    source_url="https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_protegidas.php",
    source_institution="MAPA",
    license="livre",
)


class CultivaresProtegidasDataset(_rnc.CultivaresDataset):
    info = CULTIVARES_PROTEGIDAS_INFO
    source_contract = "rnc_protegidas"


_cultivares_protegidas = CultivaresProtegidasDataset()
registry.register(_cultivares_protegidas)


@overload
async def cultivares_protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cultivares_protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cultivares_protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def cultivares_protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _cultivares_protegidas.fetch(
        cultivar=cultivar,
        especie=especie,
        situacao=situacao,
        titular=titular,
        nr_processo=nr_processo,
        nr_certificado=nr_certificado,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
