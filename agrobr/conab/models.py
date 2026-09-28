from __future__ import annotations

from datetime import date
from typing import Self
from urllib.parse import urlsplit

import pydantic

from agrobr.exceptions import InvalidParameterError
from agrobr.utils import validation


def validate_selection(safra: str | None, levantamento: int | None) -> str | None:
    if levantamento is not None and (
        isinstance(levantamento, bool)
        or not isinstance(levantamento, int)
        or not 1 <= levantamento <= 12
    ):
        raise InvalidParameterError("levantamento deve ser inteiro de 1 a 12 ou None")
    return validation.validate_safra(safra)


class ConabLevantamento(pydantic.BaseModel):
    url: str
    levantamento: int = pydantic.Field(ge=1, le=12, strict=True)
    safra: str
    ano_inicio: int = pydantic.Field(ge=1900, strict=True)
    ano_fim: int = pydantic.Field(ge=0, le=99, strict=True)
    data_publicacao: date | None = None

    @pydantic.field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("URL de levantamento deve ser HTTP(S) absoluta")
        return value

    @pydantic.model_validator(mode="after")
    def validate_safra(self) -> Self:
        normalized = validate_selection(self.safra, self.levantamento)
        if normalized != f"{self.ano_inicio}/{self.ano_fim:02d}":
            raise ValueError("Safra inconsistente com os anos do catálogo")
        return self
