from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.datasets import base
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result

logger = structlog.get_logger()


class AgrofitDataset(base.BaseDataset):
    source_contract: str

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError(
                f"{self.info.name} não aceita produto; use os filtros nomeados do cadastro"
            )

    def _contract_name(self, **_kwargs: Any) -> str:
        return self.source_contract

    async def fetch(
        self,
        produto: str = "",
        return_meta: bool = False,
        *,
        use_cache: bool = True,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        self._validate_produto(produto)
        for name, value in (
            ("use_cache", use_cache),
            ("as_polars", as_polars),
            ("return_meta", return_meta),
        ):
            if not isinstance(value, bool):
                raise InvalidParameterError(f"{name} deve ser booleano")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                f"{self.info.name} não suporta deterministic: o Agrofit entrega uma exportação "
                "corrente e seu cache preserva uma coleta, não um cadastro histórico arbitrário."
            )
        logger.info("dataset_fetch", dataset=self.info.name)
        frame, source_name, source_meta, attempted = await self._try_sources(
            "", use_cache=use_cache, **kwargs
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame,
                source_name,
                source_meta,
                attempted,
                None,
                contract_name=self.source_contract,
            )
            if return_meta
            else None
        )
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)
