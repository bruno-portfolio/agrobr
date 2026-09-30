from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.datasets import _comercio_exterior
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)


async def _fetch_comexstat(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import comexstat
    from agrobr.comexstat import models

    ano = kwargs.get("ano")
    uf = kwargs.get("uf")

    result = await comexstat.importacao(produto, ano=ano, uf=uf, return_meta=True)

    df, meta = _unpack_result(result)
    return _comercio_exterior.adapt_comexstat(
        df, meta, combine_ncms=not models.resolve_ncm(produto).codigo_unico
    )


IMPORTACAO_INFO = DatasetInfo(
    name="importacao",
    description="Importações agrícolas brasileiras por produto, UF e mês",
    sources=[
        DatasetSource(
            name="comexstat",
            priority=1,
            fetch_fn=_fetch_comexstat,
            description="ComexStat/MDIC (dados oficiais de comércio exterior)",
        ),
    ],
    products=["soja", "milho", "cafe", "algodao", "acucar", "farelo_soja", "oleo_soja"],
    contract_version="1.2",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://comexstat.mdic.gov.br",
    source_institution="MDIC/ComexStat",
    min_date="1997-01-01",
    unit="kg / USD",
    license="livre",
)


class ImportacaoDataset(BaseDataset):
    info = IMPORTACAO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        uf: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="importacao", produto=produto, ano=ano)

        _comercio_exterior.validate_options(return_meta)

        df, source_name, source_meta, attempted = await self._try_sources(produto, ano=ano, uf=uf)

        if source_name == "comexstat":
            df = _comercio_exterior.restore_empty_comexstat(df, source_meta)
        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            return df, _comercio_exterior.dataset_meta(
                self._build_meta(df, source_name, source_meta, attempted, None)
            )

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = pd.Series(produto, index=df.index, dtype="string[python]")

        colunas = [column.name for column in contracts.get_contract("importacao").columns]
        return df[[coluna for coluna in colunas if coluna in df.columns]]


_importacao = ImportacaoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_importacao)


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _importacao.fetch(  # type: ignore[call-arg]
        produto, ano=ano, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
