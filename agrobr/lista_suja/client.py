from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
import structlog

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import discovery, models

logger = structlog.get_logger()
TIMEOUT = get_timeout(read=60.0)


async def _fetch_http(http: httpx.AsyncClient, url: str) -> models.HTTPResource:
    response = await retry_on_status(lambda: http.get(url), source="lista_suja")
    responses.raise_for_status(response, source="lista_suja")
    final_url = discovery.validate_url(str(response.url), url)
    raw = response.content
    return models.HTTPResource(
        content=raw,
        requested_url=url,
        url=final_url,
        fetched_at=datetime.now(UTC),
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        content_type=response.headers.get("content-type"),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )


def _eligible(exc: httpx.HTTPError | SourceUnavailableError) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status in {403, 404, 408, 410, 429} or 500 <= status < 600
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, SourceUnavailableError):
        if isinstance(exc.__cause__, httpx.HTTPError):
            return _eligible(exc.__cause__)
        match = re.search(r"HTTP (\d{3})", exc.last_error)
        if match:
            status = int(match[1])
            return status in {403, 404, 408, 410, 429} or 500 <= status < 600
    return False


def _validate_body(resource: models.HTTPResource, kind: str) -> None:
    content = resource.content
    prefix = content[:512].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    invalid = not content.strip()
    if kind == "pdf":
        invalid = invalid or not content.startswith(b"%PDF")
    else:
        invalid = (
            invalid
            or prefix.startswith((b"<", b"%pdf", b"pk\x03\x04", b"\xd0\xcf\x11\xe0"))
            or b"<html" in prefix
            or b"\x00" in content
        )
    if invalid:
        raise ParseError(
            source="lista_suja",
            parser_version=4,
            reason=f"Corpo {kind.upper()} incompatível em resposta HTTP bem-sucedida",
        )


async def _companion(
    http: httpx.AsyncClient, publication: models.PublicationResources, warnings: list[str]
) -> models.HTTPResource | None:
    url = publication.resources.get("txt")
    if url is None:
        warnings.append(
            "TXT companheiro não anunciado; contexto de edição não comprovado pelo CSV."
        )
        return None
    try:
        resource = await _fetch_http(http, url)
    except (httpx.HTTPError, SourceUnavailableError) as exc:
        if not _eligible(exc):
            raise
        warnings.append(
            f"TXT companheiro indisponível ({type(exc).__name__}); contexto de edição não comprovado pelo CSV."
        )
        return None
    _validate_body(resource, "txt")
    return resource


async def _acquire(
    http: httpx.AsyncClient,
    formato: str,
    page: models.HTTPResource,
    publication: models.PublicationResources,
) -> models.Acquisition:
    warnings: list[str] = []
    attempts: list[str] = []
    fallback: dict[str, Any] | None = None
    selected: Literal["csv", "pdf"] = "pdf" if formato == "pdf" else "csv"
    if selected not in publication.resources:
        if formato != "auto" or "pdf" not in publication.resources:
            raise SourceUnavailableError(
                source="lista_suja",
                url=page.url,
                last_error=f"Formato {selected} não anunciado para o cadastro principal",
            )
        selected = "pdf"
        fallback = {"from": "csv", "to": "pdf", "reason": "csv_not_advertised"}
    attempts.append(f"lista_suja_{selected}")
    try:
        resource = await _fetch_http(http, publication.resources[selected])
    except (httpx.HTTPError, SourceUnavailableError) as exc:
        if (
            formato != "auto"
            or selected != "csv"
            or "pdf" not in publication.resources
            or not _eligible(exc)
        ):
            raise
        fallback = {
            "from": "csv",
            "to": "pdf",
            "reason": "transport_unavailable",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "url": publication.resources["csv"],
        }
        if isinstance(exc, SourceUnavailableError):
            status = re.search(r"HTTP (\d{3})", exc.last_error)
            if status:
                fallback["status_code"] = int(status[1])
        selected = "pdf"
        attempts.append("lista_suja_pdf")
        resource = await _fetch_http(http, publication.resources["pdf"])
    _validate_body(resource, selected)
    companion = await _companion(http, publication, warnings) if selected == "csv" else None
    if fallback:
        warnings.append(
            "Rota PDF selecionada por indisponibilidade do CSV anunciado ou ausência de anúncio; causa registrada em fallback."
        )
    return models.Acquisition(
        formato=selected,
        resource=resource,
        discovery=page,
        publication=publication,
        companion=companion,
        attempted_sources=attempts,
        selected_source=f"lista_suja_{selected}",
        warnings=warnings,
        fallback=fallback,
    )


async def fetch_empregadores(*, formato: str = "auto") -> models.Acquisition:
    if not isinstance(formato, str) or formato not in {"auto", "csv", "pdf"}:
        raise InvalidParameterError("formato deve ser auto, csv ou pdf")
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        page = await _fetch_http(http, constants.URLS[constants.Fonte.LISTA_SUJA]["page"])
        publication = discovery.parse_publication(page.content, page.url)
        result = await _acquire(http, formato, page, publication)
        logger.info(
            "lista_suja_download_ok", formato=result.formato, size=result.resource.size_bytes
        )
        return result
