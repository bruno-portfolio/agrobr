from __future__ import annotations

import importlib
import warnings
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.contracts import embrapa_solos as contracts
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.utils import geo, result
from agrobr.utils.warnings import warn_once

from . import acquisition, client, metadata, query

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl

logger = _log.get_logger(__name__)


def _geoframe(acquired: acquisition.SolosAcquisition, geopandas: Any) -> pd.DataFrame:
    geometries = acquired.geometries
    if geometries is None or len(geometries) != len(acquired.frame):
        raise ParseError(
            source="embrapa_solos",
            parser_version=3,
            reason="Geometrias divergem das ocorrências selecionadas",
        )
    crs = acquired.query.output_crs
    if geometries:
        features = (
            {"type": "Feature", "properties": {}, "geometry": geometry} for geometry in geometries
        )
        geometry = geopandas.GeoDataFrame.from_features(features, crs=crs).geometry
    else:
        geometry = geopandas.GeoSeries([], crs=crs)
    return cast(pd.DataFrame, geopandas.GeoDataFrame(acquired.frame, geometry=geometry, crs=crs))


async def _fetch(
    *,
    product: Literal["perfis", "mapa"],
    include_geometry: bool,
    as_polars: bool,
    return_meta: bool,
    unknown: dict[str, Any],
    **selection: Any,
) -> result.DataFrameResult:
    from agrobr.datasets.deterministic import get_snapshot

    if unknown:
        raise TypeError(f"Argumentos desconhecidos em embrapa_solos: {sorted(unknown)}")
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "embrapa_solos não suporta deterministic: WFS sem edição imutável"
        )
    validated = query.build_query(product=product, include_geometry=include_geometry, **selection)
    warn_once("embrapa_solos", constants.EMBRAPA_SOLOS_NC_WARNING)
    geopandas = geo.check_geopandas() if include_geometry else None
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None
    logger.info("embrapa_solos_fetch", product=product, include_geometry=include_geometry)
    acquired = await client.fetch_acquisition(validated)
    contract = contracts.PERFIS_V3 if product == "perfis" else contracts.MAPA_V2
    valid, errors = contract.validate(acquired.frame)
    if not valid:
        raise ContractViolationError(dataset=contract.name, violation="; ".join(errors))
    frame = _geoframe(acquired, geopandas) if include_geometry else acquired.frame
    meta = metadata.build_meta(acquired, frame)
    remote = acquired.coverage.remote
    if validated.ordem is not None and not remote.truncated and frame.empty:
        aviso = (
            f"EMBRAPA Solos: ordem={validated.ordem!r} não encontrou registros na leitura completa. "
            f"Valores de ordem1 presentes na leitura: {acquired.local_filters['ordens_observadas']!r}"
        )
        meta.validation_warnings.append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=3)
    if remote.truncated:
        warnings.warn(
            f"EMBRAPA Solos: prefixo remoto de {remote.accepted_rows} de {remote.expected_before} ocorrências; "
            f"filtro local retornou {len(frame)} linhas. A seleção permanece parcial; "
            "use max_registros=None para retirar o teto de ocorrências.",
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
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _fetch(
        product="perfis",
        include_geometry=False,
        as_polars=as_polars,
        return_meta=return_meta,
        unknown=kwargs,
        uf=uf,
        bbox=bbox,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _fetch(
        product="mapa",
        include_geometry=False,
        as_polars=as_polars,
        return_meta=return_meta,
        unknown=kwargs,
        ordem=ordem,
        bbox=bbox,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
    )


@overload
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["perfis"],
    tamanho_pagina: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["perfis"],
    tamanho_pagina: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["perfis"],
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> result.GeoDataFrameResult: ...


async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["perfis"],
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.GeoDataFrameResult:
    return cast(
        result.GeoDataFrameResult,
        await _fetch(
            product="perfis",
            include_geometry=True,
            as_polars=False,
            return_meta=return_meta,
            unknown=kwargs,
            uf=uf,
            bbox=bbox,
            max_registros=max_registros,
            tamanho_pagina=tamanho_pagina,
        ),
    )


@overload
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["mapa"],
    tamanho_pagina: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["mapa"],
    tamanho_pagina: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["mapa"],
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> result.GeoDataFrameResult: ...


async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["mapa"],
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.GeoDataFrameResult:
    return cast(
        result.GeoDataFrameResult,
        await _fetch(
            product="mapa",
            include_geometry=True,
            as_polars=False,
            return_meta=return_meta,
            unknown=kwargs,
            ordem=ordem,
            bbox=bbox,
            max_registros=max_registros,
            tamanho_pagina=tamanho_pagina,
        ),
    )
