from __future__ import annotations

import hashlib
import re
import time
from datetime import UTC, date, datetime
from typing import Any, Literal, cast
from urllib.parse import urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from agrobr import constants
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses, retry, settings, user_agents
from agrobr.normalize import encoding

from . import models


def resolve_edition(content: bytes, page_url: str) -> models.LinkedEdition:
    soup = BeautifulSoup(content.decode(encoding.detect_encoding_chain(content)), "lxml")
    choices: dict[str, models.LinkedEdition] = {}
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        if "andamento_dos_processos_quilombolas" not in href.lower():
            continue
        url = urljoin(page_url, href)
        source, target = urlsplit(page_url), urlsplit(url)
        if (
            target.scheme != "https"
            or target.netloc != source.netloc
            or target.username is not None
            or target.query
            or target.fragment
            or not target.path.startswith(source.path.rstrip("/") + "/")
        ):
            raise ParseError(
                "incra",
                constants.INCRA_ANDAMENTO_PARSER_VERSION,
                "Link administrativo fora da família oficial revisada",
            )
        matched = re.fullmatch(
            re.escape(source.path.rstrip("/"))
            + r"/andamento_dos_processos_quilombolas-(\d{2})_(\d{2})_(\d{4})\.pdf/@@display-file/file",
            target.path,
        )
        if not matched:
            raise ParseError(
                "incra",
                constants.INCRA_ANDAMENTO_PARSER_VERSION,
                "Formato não reconhecido do recurso administrativo",
            )
        try:
            published = date(int(matched[3]), int(matched[2]), int(matched[1]))
        except ValueError as exc:
            raise ParseError(
                "incra",
                constants.INCRA_ANDAMENTO_PARSER_VERSION,
                "Data inválida no recurso administrativo",
            ) from exc
        choices[url] = models.LinkedEdition(
            file_date=published, href=href, url=url, label=link.get_text(" ", strip=True)
        )
    if len(choices) != 1:
        raise ParseError(
            "incra",
            constants.INCRA_ANDAMENTO_PARSER_VERSION,
            "A publicação deve resolver exatamente um recurso administrativo",
        )
    return next(iter(choices.values()))


class Download:
    def __init__(self) -> None:
        self.resources: list[models.Resource] = []
        self.total_bytes = 0

    async def request(
        self, http: httpx.AsyncClient, url: str, role: Literal["publisher", "pdf"]
    ) -> httpx.Response:
        if len(self.resources) >= constants.INCRA_ANDAMENTO_MAX_ATTEMPTS:
            raise SourceUnavailableError(
                "incra", url, "Orçamento de envios administrativos esgotado"
            )
        resource = models.Resource(
            role=role,
            url=url,
            attempt=len(self.resources) + 1,
            requested_at=datetime.now(UTC),
            request_headers={
                key: value
                for key, value in http.headers.items()
                if key.lower() in {"user-agent", "accept", "accept-encoding"}
            },
        )
        self.resources.append(resource)
        body = bytearray()
        response: httpx.Response | None = None
        try:
            response = await http.send(http.build_request("GET", url), stream=True)
            resource.status = response.status_code
            resource.response_headers = {
                key: value
                for key, value in response.headers.items()
                if key.lower()
                in {
                    "content-type",
                    "content-length",
                    "content-encoding",
                    "date",
                    "etag",
                    "last-modified",
                    "location",
                    "retry-after",
                }
            }
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                self.total_bytes += len(chunk)
                if (
                    len(body) > constants.INCRA_ANDAMENTO_MAX_BODY_BYTES
                    or self.total_bytes > constants.INCRA_ANDAMENTO_MAX_TOTAL_BODY_BYTES
                ):
                    raise SourceUnavailableError(
                        "incra", url, "Orçamento de bytes administrativos excedido"
                    )
            response._content = bytes(body)
            resource.complete_body = True
            return response
        except (httpx.HTTPError, SourceUnavailableError) as exc:
            resource.error_type, resource.error = type(exc).__name__, str(exc)
            raise
        finally:
            resource.size_bytes = len(body)
            if response is not None:
                resource.sha256 = hashlib.sha256(body).hexdigest()
                try:
                    await response.aclose()
                except httpx.HTTPError as exc:
                    resource.close_error = str(exc)
                    if resource.error_type is None:
                        resource.error_type, resource.error = type(exc).__name__, str(exc)
                        resource.finished_at = datetime.now(UTC)
                        raise
            resource.finished_at = datetime.now(UTC)

    async def fetch(
        self, http: httpx.AsyncClient, url: str, role: Literal["publisher", "pdf"]
    ) -> bytes:
        response = await retry.retry_on_status(
            lambda: self.request(http, url, role), source="incra", max_attempts=3
        )
        if response.is_redirect:
            raise SourceUnavailableError(
                "incra", url, "Redirecionamento administrativo não demonstrado"
            )
        responses.raise_for_status(response, source="incra")
        return response.content


async def fetch_publication() -> models.Acquisition:
    started = time.monotonic()
    downloader = Download()
    page_url = constants.INCRA_ANDAMENTO_PAGE_URL
    try:
        async with httpx.AsyncClient(
            timeout=settings.get_timeout(read=120.0),
            headers=user_agents.UserAgentRotator.get_bot_headers(),
            follow_redirects=False,
        ) as http:
            html = await downloader.fetch(http, page_url, "publisher")
            linked = resolve_edition(html, page_url)
            pdf = await downloader.fetch(http, linked.url, "pdf")
            if not pdf.startswith(b"%PDF"):
                raise ParseError(
                    "incra",
                    constants.INCRA_ANDAMENTO_PARSER_VERSION,
                    "Recurso administrativo não é um PDF",
                )
        return models.Acquisition(
            content=pdf,
            linked_edition=linked,
            page_url=page_url,
            resources=downloader.resources,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
    except (httpx.HTTPError, SourceUnavailableError, ParseError) as exc:
        cast(Any, exc).resources = [
            resource.model_dump(mode="json") for resource in downloader.resources
        ]
        raise
