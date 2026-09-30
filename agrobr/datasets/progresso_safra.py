from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)

PRODUCTS = ["algodao", "arroz", "feijao_1", "milho_1", "milho_2", "soja", "trigo"]


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.conab.progresso import api as progresso_api
    from agrobr.conab.progresso.models import normalizar_cultura

    cultura = normalizar_cultura(produto)
    estado = kwargs.get("estado")
    operacao = kwargs.get("operacao")

    result = await progresso_api.progresso_safra(
        cultura=cultura,
        estado=estado,
        operacao=operacao,
        semana_url=kwargs.get("semana_url"),
        return_meta=True,
    )

    return _unpack_result(result)


PROGRESSO_SAFRA_INFO = DatasetInfo(
    name="progresso_safra",
    description="Progresso semanal de semeadura e colheita (CONAB)",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB (Companhia Nacional de Abastecimento)",
        ),
    ],
    products=PRODUCTS,
    contract_version="2.0",
    update_frequency="weekly",
    typical_latency="W+0",
    source_url="https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra",
    source_institution="CONAB",
    unit="fração (0-1)",
    license="livre",
)


class ProgressoSafraDataset(BaseDataset):
    info = PROGRESSO_SAFRA_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        estado: str | None = None,
        operacao: str | None = None,
        return_meta: bool = False,
        semana_url: str | None = None,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="progresso_safra", produto=produto)

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, estado=estado, operacao=operacao, semana_url=semana_url
        )

        df = self._normalize(df)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_progresso_safra = ProgressoSafraDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_progresso_safra)


@overload
async def progresso_safra(
    produto: str,
    estado: str | None = None,
    operacao: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
    semana_url: str | None = None,
) -> pd.DataFrame: ...


@overload
async def progresso_safra(
    produto: str,
    estado: str | None = None,
    operacao: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
    semana_url: str | None = None,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def progresso_safra(
    produto: str,
    estado: str | None = None,
    operacao: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
    semana_url: str | None = None,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _progresso_safra.fetch(  # type: ignore[call-arg]
        produto,
        estado=estado,
        operacao=operacao,
        return_meta=return_meta,
        as_polars=as_polars,
        semana_url=semana_url,
    )
