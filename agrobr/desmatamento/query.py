from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions
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
        if self.product == "PRODES" and self.year is not None and self.year > date.today().year:
            raise ValueError(f"Ano {self.year} posterior ao corrente ({date.today().year})")
        if (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date > self.end_date
        ):
            raise ValueError("data_inicio deve ser anterior ou igual a data_fim")
        maximum = (
            constants.DESMATAMENTO_GEO_MAX_PAGE_SIZE
            if self.include_geometry
            else constants.DESMATAMENTO_MAX_PAGE_SIZE
        )
        if self.page_size > maximum:
            raise ValueError(f"tamanho_pagina deve ser no máximo {maximum}")
        json.dumps(self.requested, allow_nan=False)
        return self


def _date(value: object, name: str) -> date | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or re.fullmatch(constants.DESMATAMENTO_DATE_PATTERN, value) is None
    ):
        raise InvalidParameterError(f"{name} deve ser YYYY-MM-DD ou None")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidParameterError(f"{name} não é uma data civil válida") from exc


def build_query(
    *,
    product: Literal["PRODES", "DETER"],
    include_geometry: bool,
    bioma: object,
    max_registros: object,
    ano: object = None,
    uf: object = None,
    data_inicio: object = None,
    data_fim: object = None,
    classe: object = None,
    tamanho_pagina: object = None,
) -> DesmatamentoQuery:
    if not isinstance(bioma, str):
        raise InvalidParameterError("bioma deve ser string")
    if uf is not None and not isinstance(uf, str):
        raise InvalidParameterError("uf deve ser string ou None")
    if classe is not None and not isinstance(classe, str):
        raise InvalidParameterError("classe deve ser string ou None")
    requested = {
        "bioma": bioma,
        "ano": ano,
        "uf": uf,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
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
                "start_date": _date(data_inicio, "data_inicio"),
                "end_date": _date(data_fim, "data_fim"),
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
