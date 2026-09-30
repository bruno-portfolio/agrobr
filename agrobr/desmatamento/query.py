from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions
from agrobr.utils import time as time_utils
from agrobr.utils import validation

from . import models


class DesmatamentoQuery(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    product: Literal["PRODES", "DETER"]
    biome: str
    include_geometry: bool
    year: int | None = Field(default=None, ge=1, le=9999)
    uf: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    class_name: str | None = None
    max_records: int | None = Field(ge=1)
    page_size: int = Field(ge=1)
    requested: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_selection(self) -> DesmatamentoQuery:
        supported = (
            models.PRODES_WORKSPACES if self.product == "PRODES" else models.DETER_WORKSPACES
        )
        if self.biome not in supported:
            raise ValueError("Bioma incompatível com o produto")
        if self.uf is not None and (
            self.uf != self.uf.strip().upper() or self.uf not in regions.UFS_VALIDAS
        ):
            raise ValueError("UF inválida ou não canônica")
        if self.class_name is not None and not self.class_name.strip():
            raise ValueError("Classe vazia")
        if self.product == "PRODES" and any(
            value is not None for value in (self.start_date, self.end_date, self.class_name)
        ):
            raise ValueError("Filtros DETER não são aplicáveis ao PRODES")
        if self.product == "DETER" and self.year is not None:
            raise ValueError("Ano não é filtro DETER")
        corrente = time_utils.hoje().year
        if self.product == "PRODES" and self.year is not None and self.year > corrente:
            raise ValueError(f"Ano {self.year} posterior ao corrente ({corrente})")
        if (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date > self.end_date
        ):
            raise ValueError("inicio deve ser anterior ou igual a fim")
        maximum = (
            constants.DESMATAMENTO_GEO_MAX_PAGE_SIZE
            if self.include_geometry
            else constants.DESMATAMENTO_MAX_PAGE_SIZE
        )
        if self.page_size > maximum:
            raise ValueError(f"tamanho_pagina deve ser no máximo {maximum}")
        json.dumps(self.requested, allow_nan=False)
        return self


def build_query(
    *,
    product: Literal["PRODES", "DETER"],
    include_geometry: bool,
    bioma: object,
    max_registros: object,
    ano: object = None,
    uf: object = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    classe: object = None,
    tamanho_pagina: object = None,
) -> DesmatamentoQuery:
    if not isinstance(bioma, str):
        raise InvalidParameterError("bioma deve ser string")
    if uf is not None and not isinstance(uf, str):
        raise InvalidParameterError("uf deve ser string ou None")
    if classe is not None and not isinstance(classe, str):
        raise InvalidParameterError("classe deve ser string ou None")
    start_date = validation.parse_data(inicio, "inicio")
    end_date = validation.parse_data(fim, "fim")
    requested = {
        "bioma": bioma,
        "ano": ano,
        "uf": uf,
        "inicio": start_date.isoformat() if start_date else None,
        "fim": end_date.isoformat() if end_date else None,
        "classe": classe,
        "max_registros": max_registros,
        "tamanho_pagina": tamanho_pagina,
    }
    if tamanho_pagina is None:
        tamanho_pagina = (
            constants.DESMATAMENTO_GEO_DEFAULT_PAGE_SIZE
            if include_geometry is True
            else constants.DESMATAMENTO_DEFAULT_PAGE_SIZE
        )
    try:
        return DesmatamentoQuery.model_validate(
            {
                "product": product,
                "biome": regions.normalizar_bioma(bioma),
                "include_geometry": include_geometry,
                "year": ano,
                "uf": validation.validate_uf(uf),
                "start_date": start_date,
                "end_date": end_date,
                "class_name": classe,
                "max_records": max_registros,
                "page_size": tamanho_pagina,
                "requested": requested,
            }
        )
    except (ValidationError, ValueError, TypeError) as exc:
        raise InvalidParameterError(f"Consulta desmatamento inválida: {exc}") from exc


def revalidate(query: DesmatamentoQuery) -> DesmatamentoQuery:
    if not isinstance(query, DesmatamentoQuery):
        raise InvalidParameterError("Consulta deve ser DesmatamentoQuery")
    try:
        return DesmatamentoQuery.model_validate(query.model_dump())
    except (ValidationError, ValueError, TypeError) as exc:
        raise InvalidParameterError(f"Consulta desmatamento inválida: {exc}") from exc
