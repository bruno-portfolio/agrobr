from __future__ import annotations

import copy
import json
import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions
from agrobr.utils import validation

from . import models


class FunaiQuery(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    include_geometry: bool
    fetch_geometry: bool
    fetch_crs: Literal["EPSG:4326"] | None
    uf: str | None = None
    fase: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    bbox_crs: Literal["EPSG:4326"] = "EPSG:4326"
    output_crs: Literal["EPSG:4326"] | None = None
    max_records: int | None = Field(ge=1)
    page_size: int = Field(ge=1)
    requested: dict[str, Any]

    @model_validator(mode="after")
    def validate_selection(self) -> FunaiQuery:
        if self.uf is not None and self.uf not in regions.UFS_VALIDAS:
            raise ValueError("UF inválida ou não canônica")
        if self.fase is not None and self.fase not in models.FASES_VALIDAS:
            raise ValueError("Fase fora dos seis seletores públicos")
        maximum = (
            constants.FUNAI_GEO_MAX_PAGE_SIZE
            if self.fetch_geometry
            else constants.FUNAI_MAX_PAGE_SIZE
        )
        if self.page_size > maximum:
            raise ValueError(f"tamanho_pagina deve ser no máximo {maximum}")
        _bbox(self.bbox)
        json.dumps(self.requested, allow_nan=False)
        return self


def _bbox(value: object) -> tuple[float, float, float, float] | None:
    if value is None:
        return None
    if not isinstance(value, (tuple, list)) or len(value) != 4:
        raise ValueError("bbox deve conter quatro números")
    if any(type(item) not in (int, float) for item in value):
        raise ValueError("bbox exige números reais, sem booleanos")
    west, south, east, north = (float(item) for item in value)
    if not all(math.isfinite(item) for item in (west, south, east, north)):
        raise ValueError("bbox deve ser finita")
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("bbox exige minlon<maxlon e minlat<maxlat em EPSG:4326")
    return west, south, east, north


def build_query(
    *,
    include_geometry: bool,
    max_registros: object,
    uf: object = None,
    fase: object = None,
    bbox: object = None,
    tamanho_pagina: object = None,
) -> FunaiQuery:
    try:
        if type(include_geometry) is not bool:
            raise ValueError("Modo deve ser booleano")
        if uf is not None and type(uf) is not str:
            raise ValueError("uf deve ser string ou None")
        if fase is not None and type(fase) is not str:
            raise ValueError("fase deve ser string ou None")
        requested = copy.deepcopy(
            {
                "uf": uf,
                "fase": fase,
                "bbox": bbox,
                "max_registros": max_registros,
                "tamanho_pagina": tamanho_pagina,
            }
        )
        canonical_bbox = _bbox(bbox)
        fetch_geometry = include_geometry or canonical_bbox is not None
        page_size = tamanho_pagina
        if page_size is None:
            page_size = (
                constants.FUNAI_GEO_DEFAULT_PAGE_SIZE
                if fetch_geometry
                else constants.FUNAI_DEFAULT_PAGE_SIZE
            )
        return FunaiQuery.model_validate(
            {
                "include_geometry": include_geometry,
                "fetch_geometry": fetch_geometry,
                "fetch_crs": constants.FUNAI_CRS if fetch_geometry else None,
                "uf": validation.validate_uf(uf),
                "fase": fase,
                "bbox": canonical_bbox,
                "bbox_crs": constants.FUNAI_CRS,
                "output_crs": constants.FUNAI_CRS if include_geometry else None,
                "max_records": max_registros,
                "page_size": page_size,
                "requested": requested,
            }
        )
    except (ValidationError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidParameterError(f"Consulta FUNAI inválida: {exc}") from exc


def validate_query(query: FunaiQuery) -> FunaiQuery:
    if not isinstance(query, FunaiQuery):
        raise InvalidParameterError("Consulta deve ser FunaiQuery")
    try:
        checked = FunaiQuery.model_validate(query.model_dump())
        if set(checked.requested) != {"uf", "fase", "bbox", "max_registros", "tamanho_pagina"}:
            raise ValueError("Seletores originais incompletos ou desconhecidos")
        rebuilt = build_query(include_geometry=checked.include_geometry, **checked.requested)
        if checked.model_dump(mode="json") != rebuilt.model_dump(mode="json"):
            raise ValueError("Seletores originais divergem da consulta canônica")
        return checked
    except (ValidationError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidParameterError(f"Consulta FUNAI adulterada: {exc}") from exc
