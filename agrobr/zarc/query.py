from __future__ import annotations

import difflib
import re
from typing import Any

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import municipalities
from agrobr.utils import validation

from . import models


class ZarcQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)

    cultura: str | None = None
    uf: str | None = None
    municipio: str | None = None
    safra: str | None = None
    solo: int | None = None
    ciclo: int | None = None
    requested: dict[str, Any] = pydantic.Field(default_factory=dict)


def _text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise InvalidParameterError(f"{name} deve ser texto não vazio")
    return value.strip()


def build_query(
    *,
    produto: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
) -> ZarcQuery:
    requested = {
        "produto": produto,
        "uf": uf,
        "municipio": municipio,
        "safra": safra,
        "solo": solo,
        "ciclo": ciclo,
    }
    cultura = _text(produto, "produto")
    if cultura is not None:
        cultura = models.normalize_cultura(cultura)
        if cultura not in models.CULTURAS_CANONICAS:
            message = (
                f"Produto {requested['produto']!r} não está no catálogo ZARC ("
                f"{len(models.CULTURAS_CANONICAS)} culturas; veja zarc.culturas())."
            )
            suggestions = difflib.get_close_matches(
                cultura, sorted(models.CULTURAS_CANONICAS), n=3, cutoff=0.6
            )
            if suggestions:
                message += f" Semelhantes: {', '.join(suggestions)}"
            raise InvalidParameterError(message)
    uf = validation.validate_uf(uf)
    safra = _text(safra, "safra")
    if safra is not None:
        safra = safra.lower()
        if safra != "perene" and (
            not re.fullmatch(r"[0-9]{4}/[0-9]{4}", safra) or int(safra[5:]) != int(safra[:4]) + 1
        ):
            raise InvalidParameterError("safra deve usar anos consecutivos YYYY/YYYY ou perene")
    geocodigo = (
        None
        if municipio is None
        else f"{municipalities.resolver_municipio(municipio, uf)['codigo_ibge']:07d}"
    )
    for name, value, allowed in (
        ("solo", solo, constants.ZARC_SOIL_CODES),
        ("ciclo", ciclo, constants.ZARC_CYCLE_CODES),
    ):
        if value is not None and (type(value) is not int or value not in allowed):
            raise InvalidParameterError(
                f"{name} deve ser código inteiro homologado: {sorted(allowed)}"
            )
    return ZarcQuery(
        cultura=cultura,
        uf=uf,
        municipio=geocodigo,
        safra=safra,
        solo=solo,
        ciclo=ciclo,
        requested=requested,
    )
