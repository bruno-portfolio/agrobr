from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)


async def _fetch_anec(
    produto: str,  # noqa: ARG001
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import anec

    result = await anec.embarques(
        ano=kwargs["ano"],
        semana=kwargs.get("semana"),
        porto=kwargs.get("porto"),
        produto=kwargs.get("produto_filtro"),
        tipo=kwargs.get("tipo"),
        use_cache=kwargs.get("use_cache", True),
        return_meta=True,
    )
    return _unpack_result(result)


EMBARQUES_ANEC_INFO = DatasetInfo(
    name="embarques_anec",
    description="Embarques semanais por porto x produto — ANEC",
    sources=[
        DatasetSource(
            name="anec",
            priority=1,
            fetch_fn=_fetch_anec,
            description="ANEC — Associação Nacional dos Exportadores de Cereais",
        ),
    ],
    products=[],
    contract_version="1.1",
    update_frequency="weekly",
    typical_latency="W+1",
    source_url="https://www.anec.com.br",
    source_institution="ANEC",
    unit="ton",
    license="zona_cinza",
    min_date="2026-01-01",
)


class EmbarquesANECDataset(BaseDataset):
    info = EMBARQUES_ANEC_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        ano: int,
        semana: int | None = None,
        porto: str | None = None,
        tipo: str | None = None,
        use_cache: bool = True,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="embarques_anec",
            ano=ano,
            semana=semana,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            "",
            ano=ano,
            semana=semana,
            porto=porto,
            produto_filtro=produto,
            tipo=tipo,
            use_cache=use_cache,
        )

        df = self._normalize(df)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_embarques_anec = EmbarquesANECDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_embarques_anec)


@overload
async def embarques_anec(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: str | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def embarques_anec(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: str | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def embarques_anec(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: str | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _embarques_anec.fetch(  # type: ignore[call-arg]
        ano=ano,
        semana=semana,
        porto=porto,
        produto=produto,
        tipo=tipo,
        use_cache=use_cache,
        return_meta=return_meta,
        as_polars=as_polars,
    )
