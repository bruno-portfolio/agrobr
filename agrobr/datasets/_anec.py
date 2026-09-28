from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.datasets import base, deterministic
from agrobr.models import MetaInfo
from agrobr.utils import result

logger = structlog.get_logger()


class ANECDataset(base.BaseDataset):
    def _validate_produto(self, produto: str) -> None:
        if produto:
            super()._validate_produto(produto)

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.reset_index(drop=True)
        for column in ("publicado_em", "revisado_em"):
            if column in result:
                result[column] = pd.to_datetime(result[column], utc=True)
        return result

    async def fetch(
        self,
        produto: str | None = None,
        return_meta: bool = False,
        *,
        ano: int | None = None,
        semana: int | None = None,
        use_cache: bool = True,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        snapshot = deterministic.get_snapshot()
        logger.info("dataset_fetch", dataset=self.info.name, ano=ano, semana=semana)
        df, source_name, source_meta, attempted = await self._try_sources(
            "",
            ano=ano,
            semana=semana,
            produto_filtro=produto,
            use_cache=use_cache,
            **kwargs,
        )
        df = self._normalize(df)
        self._validate_contract(df)
        meta = (
            self._build_meta(df, source_name, source_meta, attempted, snapshot)
            if return_meta
            else None
        )
        return result.finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
