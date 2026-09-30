from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import _agrofit, base, registry
from agrobr.models import MetaInfo
from agrobr.utils import result


async def _fetch_defensivos(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import defensivos

    fetched = await defensivos.autorizacoes(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


AUTORIZACOES_DEFENSIVOS_INFO = base.DatasetInfo(
    name="autorizacoes_defensivos",
    description="Relações cadastrais de produtos, culturas e pragas publicadas pelo Agrofit/MAPA",
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


class AutorizacoesDefensivosDataset(_agrofit.AgrofitDataset):
    info = AUTORIZACOES_DEFENSIVOS_INFO
    source_contract = "agrofit_autorizacoes"


_autorizacoes_defensivos = AutorizacoesDefensivosDataset()
registry.register(_autorizacoes_defensivos)


@overload
async def autorizacoes_defensivos(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def autorizacoes_defensivos(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def autorizacoes_defensivos(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def autorizacoes_defensivos(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    return await _autorizacoes_defensivos.fetch(
        nr_registro=nr_registro,
        cultura=cultura,
        ingrediente_ativo=ingrediente_ativo,
        classe=classe,
        situacao=situacao,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
