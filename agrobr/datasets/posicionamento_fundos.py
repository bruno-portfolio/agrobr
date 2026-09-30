from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.cftc.models import CFTC_CONTRACTS
from agrobr.contracts.datasets import POSICIONAMENTO_FUNDOS_COLUNAS_V2
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult
from agrobr.utils.validation import parse_data

logger = _log.get_logger(__name__)

_PRODUCTS = sorted(set(CFTC_CONTRACTS.values()))


async def _fetch_cftc_cot(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import cftc

    result = await cftc.cot(
        produto,
        inicio=kwargs.get("inicio"),
        fim=kwargs.get("fim"),
        combined=kwargs.get("combinado", False),
        return_meta=True,
    )
    return _unpack_result(result)


POSICIONAMENTO_FUNDOS_INFO = DatasetInfo(
    name="posicionamento_fundos",
    description="Posicionamento semanal de fundos (managed money) em futuros agro — CFTC COT",
    sources=[
        DatasetSource(
            name="cftc",
            priority=1,
            fetch_fn=_fetch_cftc_cot,
            description="CFTC Commitments of Traders — Disaggregated Report",
        ),
    ],
    products=_PRODUCTS,
    contract_version="2.0",
    update_frequency="weekly",
    typical_latency="D+3",
    source_url="https://publicreporting.cftc.gov",
    source_institution="CFTC",
    min_date="2006-06-13",
    unit="contratos",
    license="livre",
)


class PosicionamentoFundosDataset(BaseDataset):
    info = POSICIONAMENTO_FUNDOS_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        *,
        inicio: str | date | datetime | None = None,
        fim: str | date | datetime | None = None,
        combinado: bool = False,
        return_meta: bool = False,
    ) -> DataFrameResult:
        logger.info("dataset_fetch", dataset="posicionamento_fundos", produto=produto)

        inicio_dt, fim_dt = parse_data(inicio, "inicio"), parse_data(fim, "fim")
        snapshot = get_snapshot()
        if snapshot and fim_dt is None:
            fim_dt = parse_data(snapshot[:10], "snapshot")
        if inicio_dt is not None and fim_dt is not None and inicio_dt > fim_dt:
            raise InvalidParameterError(f"inicio ({inicio_dt}) posterior a fim ({fim_dt})")

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            inicio=inicio_dt,
            fim=fim_dt,
            combinado=combinado,
        )
        df = df.rename(columns=POSICIONAMENTO_FUNDOS_COLUNAS_V2)

        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df


_posicionamento_fundos = PosicionamentoFundosDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_posicionamento_fundos)


@overload
async def posicionamento_fundos(
    produto: str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combinado: bool = False,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def posicionamento_fundos(
    produto: str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combinado: bool = False,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def posicionamento_fundos(
    produto: str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combinado: bool = False,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def posicionamento_fundos(
    produto: str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combinado: bool = False,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _posicionamento_fundos.fetch(  # type: ignore[call-arg]
        produto,
        inicio=inicio,
        fim=fim,
        combinado=combinado,
        return_meta=return_meta,
        as_polars=as_polars,
    )
