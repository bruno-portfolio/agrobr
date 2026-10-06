from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

import pandas as pd

from agrobr import constants
from agrobr.datasets import registry
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.models import MetaInfo
from agrobr.utils.time import utcnow_aware

if TYPE_CHECKING:
    import polars as pl

    Frame: TypeAlias = pd.DataFrame | pl.DataFrame


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.conab._custo_producao import _sociobio_api

    return _unpack_result(
        await _sociobio_api.custo_sociobiodiversidade(produto, **kwargs, return_meta=True)
    )


CUSTO_SOCIOBIODIVERSIDADE_INFO = DatasetInfo(
    name="custo_sociobiodiversidade",
    description="Custos da sociobiodiversidade CONAB nas bases e unidades publicadas",
    sources=[
        DatasetSource(
            name="conab_sociobio",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Sociobiodiversidade (planilhas oficiais)",
        )
    ],
    products=list(constants.CONAB_SOCIOBIODIVERSIDADE_PRODUTOS),
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="Y+0",
    source_url="https://www.gov.br/conab/",
    source_institution="CONAB",
    min_date=None,
    unit="base e unidade publicadas",
    license="livre",
)


class CustoSociobiodiversidadeDataset(BaseDataset):
    info = CUSTO_SOCIOBIODIVERSIDADE_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        uf: str | None = None,
        ano: int | None = None,
        *,
        return_meta: bool = False,
        local: str | None = None,
        planilha: str | None = None,
        aba: str | None = None,
        as_polars: bool = False,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> Frame | tuple[Frame, MetaInfo]:
        produto = self._produto_do_dataset(produto)
        from agrobr import contracts
        from agrobr.conab._custo_producao import _sociobio_api

        if kwargs:
            raise TypeError(f"Parâmetros não suportados: {sorted(kwargs)}")
        query = _sociobio_api.prepare_query(
            as_polars,
            return_meta,
            use_cache,
            produto=produto,
            uf=uf,
            ano=ano,
            local=local,
            planilha=planilha,
            aba=aba,
        )
        if query.produto is None:
            raise TypeError("produto é obrigatório")
        self._validate_produto(query.produto)
        frame, source_meta = await _fetch_conab(
            query.produto,
            uf=uf,
            ano=ano,
            local=local,
            planilha=planilha,
            aba=aba,
            use_cache=use_cache,
        )
        contracts.validate_dataset(frame, "custo_sociobiodiversidade")
        meta = self._build_meta(frame, "conab_sociobio", source_meta, ["conab_sociobio"], None)
        meta.timestamp = utcnow_aware()
        return _sociobio_api.finalize_output(frame, meta, as_polars, return_meta)


_custo_sociobiodiversidade = CustoSociobiodiversidadeDataset()
registry.register(_custo_sociobiodiversidade)


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pl.DataFrame: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> Frame | tuple[Frame, MetaInfo]: ...


async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> Frame | tuple[Frame, MetaInfo]:
    return await _custo_sociobiodiversidade.fetch(
        produto,
        uf=uf,
        ano=ano,
        local=local,
        planilha=planilha,
        aba=aba,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
