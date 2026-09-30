from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.datasets.registry import register
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils

logger = _log.get_logger(__name__)


async def _fetch_sicar(uf: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.alt import sicar

    result = await sicar.imoveis(
        uf,
        municipio=kwargs.get("municipio"),
        status=kwargs.get("status"),
        tipo=kwargs.get("tipo"),
        area_min=kwargs.get("area_min"),
        area_max=kwargs.get("area_max"),
        criado_apos=kwargs.get("criado_apos"),
        atualizado_apos=kwargs.get("atualizado_apos"),
        return_meta=True,
    )

    return _unpack_result(result)


CADASTRO_RURAL_INFO = DatasetInfo(
    name="cadastro_rural",
    description="Cadastro Ambiental Rural — registros de imóveis rurais por UF",
    sources=[
        DatasetSource(
            name="sicar",
            priority=1,
            fetch_fn=_fetch_sicar,
            description="SICAR/GeoServer WFS (Serviço Florestal Brasileiro)",
        ),
    ],
    products=sorted(
        [
            "AC",
            "AL",
            "AM",
            "AP",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MG",
            "MS",
            "MT",
            "PA",
            "PB",
            "PE",
            "PI",
            "PR",
            "RJ",
            "RN",
            "RO",
            "RR",
            "RS",
            "SC",
            "SE",
            "SP",
            "TO",
        ]
    ),
    contract_version="2.1",
    update_frequency="continuous",
    typical_latency="D+0",
    source_url="https://geoserver.car.gov.br/geoserver/sicar/wfs",
    source_institution="Serviço Florestal Brasileiro / MMA",
    min_date="2012-01-01",
    unit="imóveis rurais",
    license="livre",
)


class CadastroRuralDataset(BaseDataset):
    info = CADASTRO_RURAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        municipio: int | str | None = None,
        status: str | None = None,
        tipo: str | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        criado_apos: str | None = None,
        *,
        return_meta: bool = False,
        atualizado_apos: str | None = None,
        as_polars: bool = False,
    ) -> result_utils.DataFrameResult:
        if not isinstance(produto, str):
            raise InvalidParameterError("UF deve ser uma string de duas letras")
        produto = produto.strip().upper()
        self._validate_produto(produto)
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "cadastro_rural não suporta deterministic: o SICAR entrega o cadastro corrente. "
                "criado_apos e atualizado_apos são filtros incrementais, não um histórico "
                "do cadastro na data do snapshot."
            )
        logger.info(
            "dataset_fetch",
            dataset="cadastro_rural",
            produto=produto,
            municipio=municipio,
            criado_apos=criado_apos,
            atualizado_apos=atualizado_apos,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            municipio=municipio,
            status=status,
            tipo=tipo,
            area_min=area_min,
            area_max=area_max,
            criado_apos=criado_apos,
            atualizado_apos=atualizado_apos,
        )

        self._validate_contract(df)

        meta = (
            self._build_meta(df, source_name, source_meta, attempted, None) if return_meta else None
        )
        return result_utils.finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


_cadastro_rural = CadastroRuralDataset()

register(_cadastro_rural)


@overload
async def cadastro_rural(
    uf: str,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    *,
    return_meta: Literal[False] = False,
    atualizado_apos: str | None = None,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cadastro_rural(
    uf: str,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    *,
    return_meta: Literal[True],
    atualizado_apos: str | None = None,
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cadastro_rural(
    uf: str,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    *,
    return_meta: bool = False,
    atualizado_apos: str | None = None,
    as_polars: bool = False,
) -> result_utils.DataFrameResult: ...


async def cadastro_rural(
    uf: str,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    *,
    return_meta: bool = False,
    atualizado_apos: str | None = None,
    as_polars: bool = False,
) -> result_utils.DataFrameResult:
    return await _cadastro_rural.fetch(
        uf,
        municipio=municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        atualizado_apos=atualizado_apos,
        return_meta=return_meta,
        as_polars=as_polars,
    )
