from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.contracts import desmatamento as contracts
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.utils import geo, result

from . import acquisition, client, metadata, query

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _geoframe(acquired: acquisition.DesmatamentoAcquisition, gpd: Any) -> pd.DataFrame:
    geometries = acquired.geometries
    if geometries is None or len(geometries) != len(acquired.frame):
        raise ParseError(
            source="desmatamento", parser_version=2, reason="Geometrias divergem das ocorrências"
        )
    if not geometries:
        geometry = gpd.GeoSeries([], crs="EPSG:4326")
    else:
        features = (
            {"type": "Feature", "properties": {}, "geometry": geometry} for geometry in geometries
        )
        geometry = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326").geometry
    return cast(pd.DataFrame, gpd.GeoDataFrame(acquired.frame, geometry=geometry, crs="EPSG:4326"))


async def _fetch(
    *,
    product: Literal["PRODES", "DETER"],
    include_geometry: bool,
    as_polars: bool,
    return_meta: bool,
    unknown: dict[str, Any],
    **selection: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    from agrobr.datasets.deterministic import get_snapshot

    if unknown:
        raise TypeError(f"Argumentos desconhecidos em desmatamento: {sorted(unknown)}")
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "desmatamento não suporta deterministic: ano/data não selecionam "
            "uma edição imutável do WFS."
        )
    validated = query.build_query(product=product, include_geometry=include_geometry, **selection)
    gpd = geo.check_geopandas() if include_geometry else None
    logger.info(
        "desmatamento_fetch",
        product=product,
        bioma=validated.biome,
        include_geometry=include_geometry,
        max_registros=validated.max_records,
        tamanho_pagina=validated.page_size,
    )
    acquired = await client.fetch_acquisition(validated)
    contract = contracts.PRODES_FEICOES_V2 if product == "PRODES" else contracts.DETER_FEICOES_V2
    valid, errors = contract.validate(acquired.frame)
    if not valid:
        raise ContractViolationError(dataset=contract.name, violation="; ".join(errors))
    frame = _geoframe(acquired, gpd) if include_geometry else acquired.frame
    meta = metadata.build_meta(acquired, frame)
    if product == "PRODES" and validated.year is not None and acquired.frame.empty:
        aviso = (
            f"PRODES sem feição no WFS para {validated.biome}/{validated.year} (ano ainda não "
            "publicado ou sem desmatamento no recorte); o resultado vem vazio"
        )
        meta.validation_warnings.append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=3)
    if acquired.coverage.truncated:
        warnings.warn(
            f"{product}: retornadas {len(frame)} de {acquired.coverage.expected_rows} "
            "ocorrências por limite local; restrinja filtros ou use max_registros=None.",
            UserWarning,
            stacklevel=3,
        )
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            column.name for column in contract.columns if column.type.value == "str"
        ),
    )


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _fetch(
        product="PRODES",
        include_geometry=False,
        as_polars=as_polars,
        return_meta=return_meta,
        unknown=kwargs,
        bioma=bioma,
        uf=uf,
        ano=ano,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )


@overload
async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> Any:
    return await _fetch(
        product="PRODES",
        include_geometry=True,
        as_polars=False,
        return_meta=return_meta,
        unknown=kwargs,
        bioma=bioma,
        uf=uf,
        ano=ano,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )


@overload
async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _fetch(
        product="DETER",
        include_geometry=False,
        as_polars=as_polars,
        return_meta=return_meta,
        unknown=kwargs,
        bioma=bioma,
        uf=uf,
        data_inicio=data_inicio,
        data_fim=data_fim,
        classe=classe,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )


@overload
async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> Any:
    return await _fetch(
        product="DETER",
        include_geometry=True,
        as_polars=False,
        return_meta=return_meta,
        unknown=kwargs,
        bioma=bioma,
        uf=uf,
        data_inicio=data_inicio,
        data_fim=data_fim,
        classe=classe,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )
