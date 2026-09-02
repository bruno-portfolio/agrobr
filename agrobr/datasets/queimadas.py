from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.utils.validation import validate_bioma

logger = structlog.get_logger()


async def _fetch_queimadas(
    produto: str,  # noqa: ARG001
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import queimadas as queimadas_api

    result = await queimadas_api.focos(
        ano=kwargs["ano"],
        mes=kwargs["mes"],
        dia=kwargs.get("dia"),
        uf=kwargs.get("uf"),
        bioma=kwargs.get("bioma"),
        satelite=kwargs.get("satelite"),
        return_meta=True,
    )
    return _unpack_result(result)


QUEIMADAS_INFO = DatasetInfo(
    name="queimadas",
    description="Focos de calor detectados por satélite — INPE Queimadas",
    sources=[
        DatasetSource(
            name="inpe",
            priority=1,
            fetch_fn=_fetch_queimadas,
            description="INPE Programa Queimadas — focos de calor diários",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="daily",
    typical_latency="D+1",
    source_url="https://terrabrasilis.dpi.inpe.br/queimadas/portal/",
    source_institution="INPE",
    unit="focos",
    license="livre",
)


class QueimadasDataset(BaseDataset):
    info = QUEIMADAS_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        bioma: str | None = None,
        *,
        ano: int,
        mes: int,
        dia: int | None = None,
        uf: str | None = None,
        satelite: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        bioma = validate_bioma(bioma)
        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="queimadas",
            ano=ano,
            mes=mes,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            "",
            ano=ano,
            mes=mes,
            dia=dia,
            uf=uf,
            bioma=bioma,
            satelite=satelite,
            **kwargs,
        )

        df = self._normalize(df)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_queimadas = QueimadasDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_queimadas)


async def queimadas(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _queimadas.fetch(
        ano=ano,
        mes=mes,
        dia=dia,
        uf=uf,
        bioma=bioma,
        satelite=satelite,
        return_meta=return_meta,
        **kwargs,
    )
