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

from .models import ORDENS_ALIASES, ORDENS_SOLO


class SolosQuery(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    product: Literal["perfis", "mapa"]
    include_geometry: bool
    fetch_geometry: bool
    fetch_crs: Literal["EPSG:4326"] | None
    uf: str | None = None
    ordem: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    bbox_crs: Literal["EPSG:4326"] = "EPSG:4326"
    output_crs: Literal["EPSG:4326"] | None = None
    max_records: int | None = Field(ge=1)
    page_size: int = Field(ge=1)
    requested: dict[str, Any]

    @model_validator(mode="after")
    def validate_selection(self) -> SolosQuery:
        if self.uf is not None and (
            self.uf not in regions.UFS_VALIDAS or self.uf != self.uf.strip().upper()
        ):
            raise ValueError("UF inválida ou não canônica")
        if (self.product == "mapa" and self.uf is not None) or (
            self.product == "perfis" and self.ordem is not None
        ):
            raise ValueError("Filtro incompatível com a família")
        maximum = (
            constants.EMBRAPA_SOLOS_GEO_MAX_PAGE_SIZE
            if self.fetch_geometry
            else constants.EMBRAPA_SOLOS_MAX_PAGE_SIZE
        )
        if self.page_size > maximum:
            raise ValueError(f"tamanho_pagina deve ser no máximo {maximum}")
        _bbox(self.bbox)
        json.dumps(self.requested, allow_nan=False)
        return self


def _ordem(value: str | None) -> str | None:
    if value is None:
        return None
    canonica = ORDENS_ALIASES.get(" ".join(regions.remover_acentos(value).upper().split()))
    if canonica is None:
        raise ValueError(f"ordem {value!r} fora das classes publicadas: {', '.join(ORDENS_SOLO)}")
    return canonica


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
        raise ValueError("bbox exige minlon<maxlon e minlat<maxlat no domínio EPSG:4326")
    return west, south, east, north


def build_query(
    *,
    product: Literal["perfis", "mapa"],
    include_geometry: bool,
    max_registros: object,
    uf: object = None,
    ordem: object = None,
    bbox: object = None,
    tamanho_pagina: object = None,
) -> SolosQuery:
    try:
        if product not in ("perfis", "mapa") or type(include_geometry) is not bool:
            raise ValueError("Produto/modo inválido")
        if uf is not None and type(uf) is not str:
            raise ValueError("uf deve ser string ou None")
        if ordem is not None and type(ordem) is not str:
            raise ValueError("ordem deve ser string ou None")
        if isinstance(ordem, str) and not ordem.strip():
            raise ValueError("ordem deve ser texto não vazio")
        requested = copy.deepcopy(
            {
                "uf": uf,
                "ordem": ordem,
                "bbox": bbox,
                "max_registros": max_registros,
                "tamanho_pagina": tamanho_pagina,
            }
        )
        canonical_bbox = _bbox(bbox)
        fetch_geometry = include_geometry or canonical_bbox is not None
        page_size = tamanho_pagina
        if page_size is None:
            sizes = (
                constants.EMBRAPA_SOLOS_GEO_DEFAULT_PAGE_SIZES
                if fetch_geometry
                else constants.EMBRAPA_SOLOS_DEFAULT_PAGE_SIZES
            )
            page_size = sizes[product]
        return SolosQuery.model_validate(
            {
                "product": product,
                "include_geometry": include_geometry,
                "fetch_geometry": fetch_geometry,
                "fetch_crs": constants.EMBRAPA_SOLOS_CRS if fetch_geometry else None,
                "uf": validation.validate_uf(uf),
                "ordem": _ordem(ordem),
                "bbox": canonical_bbox,
                "bbox_crs": constants.EMBRAPA_SOLOS_CRS,
                "output_crs": constants.EMBRAPA_SOLOS_CRS if include_geometry else None,
                "max_records": max_registros,
                "page_size": page_size,
                "requested": requested,
            }
        )
    except (ValidationError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidParameterError(f"Consulta Embrapa Solos inválida: {exc}") from exc


def validate_query(query: SolosQuery) -> SolosQuery:
    if not isinstance(query, SolosQuery):
        raise InvalidParameterError("Consulta deve ser SolosQuery")
    try:
        checked = SolosQuery.model_validate(query.model_dump())
        keys = {"uf", "ordem", "bbox", "max_registros", "tamanho_pagina"}
        if set(checked.requested) != keys:
            raise ValueError("Seletores originais incompletos ou desconhecidos")
        rebuilt = build_query(
            product=checked.product, include_geometry=checked.include_geometry, **checked.requested
        )
        if checked.model_dump(mode="json") != rebuilt.model_dump(mode="json"):
            raise ValueError("Seletores originais divergem da consulta canônica")
        return checked
    except (ValidationError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidParameterError(f"Consulta Embrapa Solos adulterada: {exc}") from exc
