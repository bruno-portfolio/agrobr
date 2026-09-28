from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.models import MetaInfo
from agrobr.utils.time import utcnow_aware

if TYPE_CHECKING:
    import polars as pl

    Frame: TypeAlias = pd.DataFrame | pl.DataFrame

logger = structlog.get_logger()


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.conab.custo_producao.api import custo_producao as _custo_producao

    result = await _custo_producao(produto, **kwargs, return_meta=True)

    return _unpack_result(result)


CUSTO_PRODUCAO_INFO = DatasetInfo(
    name="custo_producao",
    description="Custos publicados por cultura, local, referência e aba CONAB",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Custo de Produção (planilhas oficiais)",
        ),
    ],
    products=[
        "soja",
        "milho",
        "arroz",
        "feijao",
        "trigo",
        "algodao",
        "cafe_arabica",
        "cafe_conilon",
    ],
    contract_version="3.0",
    update_frequency="yearly",
    typical_latency="Y+0",
    source_url="https://www.gov.br/conab/",
    source_institution="CONAB",
    min_date=None,
    unit="BRL/ha",
    license="livre",
)


class CustoProducaoDataset(BaseDataset):
    info = CUSTO_PRODUCAO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        uf: str | None = None,
        safra: str | None = None,
        return_meta: bool = False,
        local: str | None = None,
        ano: int | None = None,
        planilha: str | None = None,
        aba: str | None = None,
        as_polars: bool = False,
        *,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> Frame | tuple[Frame, MetaInfo]:
        produto = self._produto_do_dataset(produto)
        from agrobr.conab.custo_producao.api import finalize_output, prepare_query
        from agrobr.conab.custo_producao.models import normalize_cultura
        from agrobr.contracts import validate_dataset
        from agrobr.contracts.conab_custos import CONAB_CUSTOS_V3

        if kwargs:
            raise TypeError(f"Parâmetros não suportados: {sorted(kwargs)}")
        prepare_query(
            as_polars,
            return_meta,
            use_cache=use_cache,
            cultura=produto,
            uf=uf,
            safra=safra,
            local=local,
            ano=ano,
            planilha=planilha,
            aba=aba,
        )
        self._validate_produto(normalize_cultura(produto))
        logger.info(
            "dataset_fetch",
            dataset="custo_producao",
            produto=produto,
            safra=safra,
        )

        df, source_meta = await _fetch_conab(
            produto,
            uf=uf,
            safra=safra,
            local=local,
            ano=ano,
            planilha=planilha,
            aba=aba,
            use_cache=use_cache,
        )
        validate_dataset(df, CONAB_CUSTOS_V3)
        meta = self._build_meta(df, "conab", source_meta, ["conab"], None)
        meta.timestamp = utcnow_aware()
        return finalize_output(df, meta, as_polars, return_meta)

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "cultura" not in df.columns:
            df["cultura"] = produto

        return df


_custo_producao = CustoProducaoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_custo_producao)


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    **kwargs: Any,
) -> pl.DataFrame: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[True],
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    **kwargs: Any,
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    return_meta: bool = False,
    *,
    use_cache: bool = True,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    as_polars: bool = False,
    **kwargs: Any,
) -> Frame | tuple[Frame, MetaInfo]: ...


async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    return_meta: bool = False,
    *,
    use_cache: bool = True,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    as_polars: bool = False,
    **kwargs: Any,
) -> Frame | tuple[Frame, MetaInfo]:
    return await _custo_producao.fetch(
        produto,
        uf=uf,
        safra=safra,
        return_meta=return_meta,
        local=local,
        ano=ano,
        planilha=planilha,
        aba=aba,
        as_polars=as_polars,
        use_cache=use_cache,
        **kwargs,
    )
