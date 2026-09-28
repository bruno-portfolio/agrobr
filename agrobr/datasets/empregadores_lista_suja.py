from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result

logger = structlog.get_logger()


async def _fetch_lista_suja(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import lista_suja

    fetched = await lista_suja.empregadores(return_meta=True, **kwargs)
    return base._unpack_result(fetched)


EMPREGADORES_LISTA_SUJA_INFO = base.DatasetInfo(
    name="empregadores_lista_suja",
    description="Cadastro corrente de empregadores publicado pelo MTE",
    sources=[
        base.DatasetSource(
            name="lista_suja",
            priority=1,
            fetch_fn=_fetch_lista_suja,
            description="Cadastro de Empregadores — Ministério do Trabalho e Emprego",
        ),
    ],
    products=[],
    contract_version="2.0",
    update_frequency="semiannual",
    typical_latency="conforme publicação corrente e atualizações do cadastro",
    source_url=constants.URLS[constants.Fonte.LISTA_SUJA]["page"],
    source_institution="MTE",
    license="livre",
)


class EmpregadoresListaSujaDataset(base.BaseDataset):
    info = EMPREGADORES_LISTA_SUJA_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError(
                "empregadores_lista_suja não aceita produto; use uf ou id_registro"
            )

    def _contract_name(self, **_kwargs: Any) -> str:
        return "lista_suja_empregadores"

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta and source_meta.attempted_sources:
            resolved = list(dict.fromkeys([*attempted[:-1], *source_meta.attempted_sources]))
            return source_meta.selected_source or source_name, resolved
        return super()._resolve_provenance(source_name, source_meta, attempted)

    async def fetch(
        self,
        produto: str = "",
        return_meta: bool = False,
        *,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        self._validate_produto(produto)
        for name, value in (("as_polars", as_polars), ("return_meta", return_meta)):
            if not isinstance(value, bool):
                raise InvalidParameterError(f"{name} deve ser booleano")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "empregadores_lista_suja não suporta deterministic: a fonte entrega uma "
                "publicação corrente e não permite selecionar um cadastro histórico arbitrário."
            )
        logger.info("dataset_fetch", dataset=self.info.name)
        frame, source_name, source_meta, attempted = await self._try_sources("", **kwargs)
        self._validate_contract(frame)
        meta = (
            self._build_meta(
                frame,
                source_name,
                source_meta,
                attempted,
                None,
                contract_name=self._contract_name(),
            )
            if return_meta
            else None
        )
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


_empregadores_lista_suja = EmpregadoresListaSujaDataset()
registry.register(_empregadores_lista_suja)


@overload
async def empregadores_lista_suja(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def empregadores_lista_suja(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def empregadores_lista_suja(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _empregadores_lista_suja.fetch(
        uf=uf,
        id_registro=id_registro,
        formato=formato,
        as_polars=as_polars,
        return_meta=return_meta,
    )
