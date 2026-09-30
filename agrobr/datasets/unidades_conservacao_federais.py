from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult


async def _fetch_icmbio(_produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import icmbio

    return base._unpack_result(await icmbio.ucs(return_meta=True, **kwargs))


UNIDADES_CONSERVACAO_FEDERAIS_INFO = base.DatasetInfo(
    name="unidades_conservacao_federais",
    description="Cadastro corrente de unidades de conservação federais da camada ICMBio/INDE",
    sources=[
        base.DatasetSource(
            name="icmbio", priority=1, fetch_fn=_fetch_icmbio, description="ICMBio — WFS INDE"
        )
    ],
    products=[],
    contract_version="1.0",
    update_frequency="continuous",
    typical_latency="conforme atualização da camada corrente",
    source_url="https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
    source_institution="ICMBio",
    license="livre",
)


class UnidadesConservacaoFederaisDataset(base.BaseDataset):
    info = UNIDADES_CONSERVACAO_FEDERAIS_INFO

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError("unidades_conservacao_federais não aceita produto")

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta and source_meta.attempted_sources:
            return source_meta.selected_source or source_name, list(source_meta.attempted_sources)
        return super()._resolve_provenance(source_name, source_meta, attempted)

    async def fetch(
        self,
        produto: str = "",
        *,
        return_meta: bool = False,
        uf: str | None = None,
        grupo: str | None = None,
        bioma: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        from agrobr.icmbio import api

        self._validate_produto(produto)
        if kwargs:
            raise TypeError(
                f"Argumentos desconhecidos em unidades_conservacao_federais: {sorted(kwargs)}"
            )
        api._validate_output(as_polars=as_polars, return_meta=return_meta)
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "unidades_conservacao_federais não suporta deterministic: a camada é corrente, sem edição histórica selecionável"
            )
        frame, source_name, source_meta, attempted = await self._try_sources(
            "", uf=uf, grupo=grupo, bioma=bioma, bbox=bbox
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(frame, source_name, source_meta, attempted, None)
            if return_meta
            else None
        )
        if meta is not None:
            meta.timestamp = datetime.now(UTC)
        result = api._to_polars(frame) if as_polars else frame
        return (result, meta) if meta is not None else result


_unidades_conservacao_federais = UnidadesConservacaoFederaisDataset()
registry.register(_unidades_conservacao_federais)


@overload
async def unidades_conservacao_federais(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def unidades_conservacao_federais(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def unidades_conservacao_federais(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def unidades_conservacao_federais(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    return await _unidades_conservacao_federais.fetch(
        uf=uf, grupo=grupo, bioma=bioma, bbox=bbox, as_polars=as_polars, return_meta=return_meta
    )
