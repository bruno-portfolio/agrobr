from __future__ import annotations

import importlib
import warnings
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.contracts import incra as contracts
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.utils import geo, result

from . import acquisition, client, metadata, parser, query

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _geoframe(acquired: acquisition.IncraAcquisition, geopandas: Any) -> pd.DataFrame:
    geometries = acquired.geometries
    if geometries is None or len(geometries) != len(acquired.frame):
        raise ParseError(
            source="incra",
            parser_version=2,
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
    include_geometry: bool,
    as_polars: bool,
    return_meta: bool,
    unknown: dict[str, Any],
    **selection: Any,
) -> result.DataFrameResult:
    from agrobr.datasets.deterministic import get_snapshot

    if unknown:
        raise TypeError(f"Argumentos desconhecidos em incra: {sorted(unknown)}")
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if get_snapshot() is not None:
        raise InvalidParameterError("incra não suporta deterministic: WFS sem edição imutável")
    validated = query.build_query(include_geometry=include_geometry, **selection)
    geopandas = geo.check_geopandas() if include_geometry else None
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None
    logger.info("incra_quilombolas", include_geometry=include_geometry)
    acquired = await client.fetch_acquisition(validated)
    parser.converter_datas(acquired.frame)
    contract = contracts.QUILOMBOLAS_V2
    valid, errors = contract.validate(acquired.frame)
    if not valid:
        raise ContractViolationError(dataset=contract.name, violation="; ".join(errors))
    frame = _geoframe(acquired, geopandas) if include_geometry else acquired.frame
    meta = metadata.build_meta(acquired, frame)
    meta.validation_warnings.extend(acquired.frame.attrs.get(result.ATRIBUTO_AVISOS, []))
    remote = acquired.coverage.remote
    if remote.truncated:
        warnings.warn(
            f"INCRA: prefixo remoto de {remote.accepted_rows} de {remote.expected_before} ocorrências; "
            f"filtro local retornou {len(frame)} linhas. A seleção permanece parcial; "
            "use max_registros=None para retirar o teto de ocorrências.",
            UserWarning,
            stacklevel=3,
        )
    finalized = result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            column.name for column in contract.columns if column.type.value == "str"
        ),
    )
    if as_polars:
        output = cast(Any, finalized[0] if return_meta else finalized)
        meta.source_details["output_dtypes"] = {
            name: str(dtype) for name, dtype in output.schema.items()
        }
    return finalized


@overload
async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _fetch(
        include_geometry=False,
        uf=uf,
        fase=fase,
        bbox=bbox,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
        as_polars=as_polars,
        return_meta=return_meta,
        unknown=kwargs,
    )


@overload
async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = constants.INCRA_GEO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.GeoDataFrameResult:
    return await _fetch(
        include_geometry=True,
        uf=uf,
        fase=fase,
        bbox=bbox,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
        as_polars=False,
        return_meta=return_meta,
        unknown=kwargs,
    )
