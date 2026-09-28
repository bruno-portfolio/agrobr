from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Literal, cast
from urllib.parse import parse_qsl, quote, urljoin, urlsplit

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError

from . import ptax_acquisition


def _parse_date(value: str | None, field: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or re.fullmatch(constants.BCB_PTAX_DATE_PATTERN, value) is None:
        raise InvalidParameterError(f"{field} deve ter formato DD/MM/YYYY")
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        raise InvalidParameterError(f"{field} contém data inválida") from None


def _validate_top(top: int) -> None:
    if isinstance(top, bool) or not isinstance(top, int) or top <= 0:
        raise InvalidParameterError("top deve ser inteiro positivo")


def _default_start(end: date) -> date:
    try:
        return end - timedelta(days=constants.BCB_PTAX_DEFAULT_DAYS)
    except OverflowError:
        raise InvalidParameterError(
            "Período padrão PTAX excede o calendário representável"
        ) from None


def build_query(
    *,
    data: str | None = None,
    data_inicial: str | None = None,
    data_final: str | None = None,
    moeda: str = "USD",
    boletim: str = "fechamento",
    top: int = 1000,
    reference_date: date,
) -> ptax_acquisition.PtaxQuery:
    _validate_top(top)
    if (
        not isinstance(moeda, str)
        or re.fullmatch(constants.BCB_PTAX_INPUT_CURRENCY_PATTERN, moeda) is None
    ):
        raise InvalidParameterError("moeda deve conter três letras ASCII, sem espaços")
    if not isinstance(boletim, str) or boletim not in {
        "todos",
        "fechamento",
        "abertura",
        "intermediario",
    }:
        raise InvalidParameterError("boletim deve ser todos, fechamento, abertura ou intermediario")
    if type(reference_date) is not date:
        raise InvalidParameterError("reference_date deve ser data civil")
    day = _parse_date(data, "data")
    start = _parse_date(data_inicial, "data_inicial")
    end = _parse_date(data_final, "data_final")
    original_start, original_end = start, end
    defaults: list[str] = []
    if day is not None:
        if start is not None or end is not None:
            raise InvalidParameterError("data não pode ser combinada com limites de período")
        start = end = day
    else:
        if end is None:
            end = reference_date
            defaults.append("data_final")
        if start is None:
            start = _default_start(end)
            defaults.append("data_inicial")
    if start > end:
        raise InvalidParameterError("Intervalo PTAX invertido")
    return ptax_acquisition.PtaxQuery(
        requested_moeda=moeda,
        moeda=moeda.upper(),
        boletim=cast(Literal["todos", "fechamento", "abertura", "intermediario"], boletim),
        top=top,
        mode="dia" if day is not None else "periodo",
        data=day,
        data_inicial=original_start,
        data_final=original_end,
        inicio=start,
        fim=end,
        reference_date=reference_date,
        defaulted_fields=defaults,
    )


def build_catalog_query(top: int = 1000) -> ptax_acquisition.PtaxCatalogQuery:
    _validate_top(top)
    return ptax_acquisition.PtaxCatalogQuery(top=top)


def format_date(value: date) -> str:
    return f"{value.month:02d}-{value.day:02d}-{value.year:04d}"


def _route(query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery) -> str:
    if isinstance(query, ptax_acquisition.PtaxCatalogQuery):
        return "Moedas"
    if query.mode == "dia":
        return "CotacaoMoedaDia(moeda=@m,dataCotacao=@d)"
    return "CotacaoMoedaPeriodo(moeda=@m,dataInicial=@di,dataFinalCotacao=@df)"


def page_parameters(
    query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery,
    offset: int,
) -> dict[str, str]:
    parameters = {}
    if isinstance(query, ptax_acquisition.PtaxQuery):
        parameters["@m"] = f"'{query.moeda}'"
        if query.mode == "dia":
            parameters["@d"] = f"'{format_date(query.inicio)}'"
        else:
            parameters["@di"] = f"'{format_date(query.inicio)}'"
            parameters["@df"] = f"'{format_date(query.fim)}'"
    parameters.update(
        {
            "$format": "json",
            "$orderby": constants.BCB_PTAX_QUOTES_ORDER_BY
            if isinstance(query, ptax_acquisition.PtaxQuery)
            else constants.BCB_PTAX_CATALOG_ORDER_BY,
            "$top": str(query.top),
            "$skip": str(offset),
        }
    )
    return parameters


def build_page_url(
    query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery,
    *,
    skip: int,
) -> str:
    return _url(query, page_parameters(query, skip))


def build_query_url(query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery) -> str:
    parameters = page_parameters(query, 0)
    return _url(query, {k: v for k, v in parameters.items() if k not in ("$top", "$skip")})


def _url(
    query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery,
    parameters: dict[str, str],
) -> str:
    encoded = "&".join(f"{key}=" + quote(value, safe="',") for key, value in parameters.items())
    return f"{constants.URLS[constants.Fonte.BCB]['ptax']}/{_route(query)}?{encoded}"


def validate_page_url(
    query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery,
    url: str,
    *,
    skip: int,
) -> str:
    base = urlsplit(f"{constants.URLS[constants.Fonte.BCB]['ptax']}/{_route(query)}")
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
            source="bcb_ptax",
            parser_version=2,
            reason="Continuação ou redirect PTAX altera origem, rota ou seleção",
        )
    return url


def validate_next_link(
    query: ptax_acquisition.PtaxQuery | ptax_acquisition.PtaxCatalogQuery,
    current_url: str,
    next_link: str,
    *,
    skip: int,
) -> str:
    try:
        target = urljoin(current_url, next_link)
    except ValueError:
        raise ParseError(
            source="bcb_ptax", parser_version=2, reason="URL de continuação PTAX inválida"
        ) from None
    return validate_page_url(query, target, skip=skip)
