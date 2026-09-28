from __future__ import annotations

import httpx
import pydantic
import structlog
from bs4 import BeautifulSoup

from agrobr import constants
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses, retry
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import acquisition, parser

logger = structlog.get_logger()

_REGISTRADAS_URL = constants.RNC_PUBLIC_URLS["registradas"]
_PROTEGIDAS_URL = constants.RNC_PUBLIC_URLS["protegidas"]

TIMEOUT = get_timeout(read=120.0)
MIN_CSV_SIZE = constants.RNC_MIN_CSV_SIZE


def _form_url(base_url: str, action: str) -> str:
    try:
        base = httpx.URL(base_url)
        target = base.join(action)
    except httpx.InvalidURL as error:
        raise ParseError(
            source="rnc", parser_version=parser.PARSER_VERSION, reason="URL de formulario invalida"
        ) from error
    if (
        target.scheme != base.scheme
        or target.host != base.host
        or target.port != base.port
        or target.userinfo
    ):
        raise ParseError(
            source="rnc",
            parser_version=parser.PARSER_VERSION,
            reason="Formulario CultivarWeb aponta para uma origem diferente",
        )
    return str(target)


def _read_form(response: httpx.Response, *, export: bool = False) -> tuple[str, str]:
    soup = BeautifulSoup(response.content, "lxml")
    selector = '[name="exportar"][value="csv"]' if export else 'input[name="csrf_token"]'
    control = soup.select_one(selector)
    form = control.find_parent("form") if control is not None else None
    if control is None or form is None or str(form.get("method", "get")).lower() != "post":
        raise ParseError(
            source="rnc",
            parser_version=parser.PARSER_VERSION,
            reason="Formulario de exportacao CSV ausente" if export else "Formulario CSRF ausente",
        )
    token = "" if export else str(control.get("value", "")).strip()
    if not export and not token:
        raise ParseError(
            source="rnc", parser_version=parser.PARSER_VERSION, reason="Token CSRF ausente"
        )
    return _form_url(str(response.url), str(form.get("action", ""))), token


async def _post_with_token(
    http: httpx.AsyncClient,
    base_url: str,
    data: dict[str, str],
    *,
    action: str | None = None,
) -> httpx.Response:
    async def attempt() -> httpx.Response:
        page = await http.get(base_url, follow_redirects=True)
        _form_url(base_url, str(page.url))
        if retry.should_retry_status(page.status_code):
            return page
        responses.raise_for_status(page, source="rnc")
        form_action, token = _read_form(page)
        target = _form_url(base_url, action or form_action)
        response = await http.post(
            target, data={**data, "csrf_token": token}, follow_redirects=False
        )
        if response.status_code == 403 and "CSRF" in response.text[:512].upper():
            raise retry.RetriableStatusError(
                "CultivarWeb rejeitou o token CSRF",
                request=response.request,
                response=response,
            )
        return response

    response = await retry.retry_on_status(attempt, source="rnc")
    responses.raise_for_status(response, source="rnc")
    return response


def _csv_content(response: httpx.Response, label: str) -> bytes:
    content = response.content
    media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type == "text/html" or content.lstrip().lower().startswith((b"<!doctype", b"<html")):
        raise ParseError(
            source="rnc",
            parser_version=parser.PARSER_VERSION,
            reason=f"Exportacao {label} retornou HTML em vez de CSV",
        )
    if len(content) < MIN_CSV_SIZE:
        raise SourceUnavailableError(
            source="rnc",
            url=str(response.url),
            last_error=f"CSV {label} too small ({len(content)} bytes)",
        )
    return content


async def _fetch_csv_bundle(base_url: str, label: acquisition.Family) -> acquisition.CSVAcquisition:
    logger.debug("rnc_fetch", label=label, url=base_url)

    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            headers=UserAgentRotator.get_headers(source="rnc"),
            follow_redirects=False,
        ) as http:
            search = await _post_with_token(
                http,
                base_url,
                {"postado": "1", "cod_pagina": "1", "validar": "0", "acao": "Pesquisar"},
            )
            export_url, _ = _read_form(search, export=True)
            search_resource = acquisition.SearchResource.model_validate(
                {
                    **acquisition.describe_response(search).model_dump(),
                    "reported_total": parser.parse_reported_total(search.content),
                }
            )
            response = await _post_with_token(
                http, base_url, {"exportar": "csv"}, action=export_url
            )
            content = _csv_content(response, label)
            captured = acquisition.CSVAcquisition(
                kind=label,
                resource=acquisition.describe_response(response),
                search=search_resource,
                content=content,
            )
    except httpx.HTTPError as error:
        raise SourceUnavailableError(
            source="rnc", url=base_url, last_error=f"{type(error).__name__}: {error}"
        ) from error
    except pydantic.ValidationError as error:
        raise ParseError(
            source="rnc",
            parser_version=parser.PARSER_VERSION,
            reason="Metadados da aquisição RNC/SNPC incompatíveis com a exportação",
        ) from error

    logger.info("rnc_fetch_ok", label=label, size_bytes=len(content))
    return captured


async def fetch_registradas_bundle() -> acquisition.CSVAcquisition:
    return await _fetch_csv_bundle(_REGISTRADAS_URL, "registradas")


async def fetch_protegidas_bundle() -> acquisition.CSVAcquisition:
    return await _fetch_csv_bundle(_PROTEGIDAS_URL, "protegidas")
