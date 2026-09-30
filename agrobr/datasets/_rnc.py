from __future__ import annotations

from typing import Any

from agrobr.datasets import base
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result


class CultivaresDataset(base.BaseDataset):
    source_contract: str

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError(
                f"{self.info.name} não aceita produto; use os filtros nomeados do cadastro"
            )

    def _contract_name(self, **_kwargs: Any) -> str:
        return self.source_contract

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta and source_meta.attempted_sources:
            resolved = list(dict.fromkeys(attempted[:-1] + source_meta.attempted_sources))
            return source_meta.selected_source or source_name, resolved
        return super()._resolve_provenance(source_name, source_meta, attempted)

    async def fetch(
        self,
        produto: str = "",
        return_meta: bool = False,
        *,
        use_cache: bool = True,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
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
                f"{self.info.name} não suporta deterministic: o CultivarWeb entrega uma exportação "
                "corrente e seu cache preserva uma coleta, não um cadastro histórico arbitrário."
            )
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
