from __future__ import annotations

from datetime import date, datetime
from typing import Literal, cast
from urllib.parse import parse_qsl, quote, urljoin, urlsplit

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.utils.validation import parse_data

from . import focus_acquisition


def build_query(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: str = "anual",
    top: int = 1000,
    data_inicial: str | date | datetime | None = None,
    max_registros: int | None = None,
) -> focus_acquisition.FocusQuery:
    if not isinstance(indicador, str) or not indicador.strip():
        raise InvalidParameterError("indicador deve ser texto não vazio")
    if isinstance(periodicidade, str):
        periodicidade = periodicidade.strip().casefold()
    if not isinstance(periodicidade, str) or periodicidade not in constants.BCB_FOCUS_ENTITIES:
        raise InvalidParameterError("periodicidade deve ser anual ou mensal")
    for field, value in (("top", top), ("max_registros", max_registros)):
        if value is None and field == "max_registros":
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise InvalidParameterError(f"{field} deve ser inteiro positivo")
    start = parse_data(data_inicial, "inicio")
    escaped = indicador.replace("'", "''")
    filter_expression = f"Indicador eq '{escaped}'"
    if start is not None:
        filter_expression += f" and Data ge '{start.isoformat()}'"
    try:
        return focus_acquisition.FocusQuery(
            indicador=indicador,
            periodicidade=cast(Literal["anual", "mensal"], periodicidade),
            entity=constants.BCB_FOCUS_ENTITIES[periodicidade],
            data_inicial=start,
            top=top,
            max_registros=max_registros,
            filter=filter_expression,
            order_by=constants.BCB_FOCUS_ORDER_BY[periodicidade],
        )
    except pydantic.ValidationError:
        raise InvalidParameterError("Seleção Focus inválida") from None


def page_parameters(query: focus_acquisition.FocusQuery, offset: int) -> dict[str, str]:
    return {
        "$format": "json",
        "$filter": query.filter,
        "$orderby": query.order_by,
        "$top": str(query.top),
        "$skip": str(offset),
    }


def _url(query: focus_acquisition.FocusQuery, parameters: dict[str, str]) -> str:
    encoded = "&".join(f"{key}=" + quote(value, safe="',") for key, value in parameters.items())
    return f"{constants.URLS[constants.Fonte.BCB]['focus']}/{query.entity}?{encoded}"


def build_page_url(query: focus_acquisition.FocusQuery, *, skip: int) -> str:
    return _url(query, page_parameters(query, skip))


def build_query_url(query: focus_acquisition.FocusQuery) -> str:
    parameters = page_parameters(query, 0)
    return _url(query, {key: parameters[key] for key in ("$format", "$filter", "$orderby")})


def validate_page_url(query: focus_acquisition.FocusQuery, url: str, *, skip: int) -> str:
    base = urlsplit(f"{constants.URLS[constants.Fonte.BCB]['focus']}/{query.entity}")
    try:
        target = urlsplit(url)
        pairs = parse_qsl(target.query, keep_blank_values=True, strict_parsing=True)
        valid = (
            target.scheme == "https"
            and (target.hostname, target.port or 443, target.path)
            == (base.hostname, base.port or 443, base.path)
            and target.username is None
            and target.password is None
            and not target.fragment
            and len(pairs) == len(dict(pairs))
            and dict(pairs) == page_parameters(query, skip)
        )
    except ValueError:
        valid = False
    if not valid:
        raise ParseError(
            source="bcb_focus",
            parser_version=2,
            reason="Continuação ou redirect Focus altera origem, entidade ou seleção",
        )
    return url


def validate_next_link(
    query: focus_acquisition.FocusQuery,
    current_url: str,
    next_link: str,
    *,
    skip: int,
) -> str:
    try:
        target = urljoin(current_url, next_link)
    except ValueError:
        raise ParseError(
            source="bcb_focus", parser_version=2, reason="URL de continuação inválida"
        ) from None
    return validate_page_url(query, target, skip=skip)
