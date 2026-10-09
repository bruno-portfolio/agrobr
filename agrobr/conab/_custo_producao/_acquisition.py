from __future__ import annotations

import copy
import hashlib
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal
from urllib.parse import urljoin, urlsplit

import httpx
import pydantic
from bs4 import BeautifulSoup, Tag

from agrobr import constants
from agrobr.constants import (
    CONAB_CUSTOS_CATALOG_URL as CATALOG_URL,
)
from agrobr.constants import (
    CONAB_CUSTOS_MAX_BODY_BYTES as MAX_BODY_BYTES,
)
from agrobr.constants import (
    CONAB_CUSTOS_MAX_REQUESTS as MAX_REQUESTS,
)
from agrobr.constants import (
    CONAB_CUSTOS_MAX_TOTAL_BYTES as MAX_TOTAL_BYTES,
)
from agrobr.constants import (
    CONAB_CUSTOS_TAB_URL as TAB_URL,
)
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.io import validate_download
from agrobr.utils.time import utcnow_aware

from . import models
from ._context import key


class _CatalogSnapshot(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid", frozen=True)

    resources: dict[str, models.RecursoCusto]
    culturas_catalogo: list[str]
    received_at: datetime
    receipts: list[dict[str, Any]]
    received_bytes: int


_state_lock = threading.RLock()
_catalog_cache: _CatalogSnapshot | None = None
_sociobio_catalog_cache: _CatalogSnapshot | None = None


def now() -> datetime:
    return utcnow_aware()


def clear() -> None:
    global _catalog_cache, _sociobio_catalog_cache
    with _state_lock:
        _catalog_cache = None
        _sociobio_catalog_cache = None


def _get_cached_catalog(family: str = "agricolas") -> _CatalogSnapshot | None:
    with _state_lock:
        snapshot = _catalog_cache if family == "agricolas" else _sociobio_catalog_cache
        if snapshot is None or now() >= snapshot.received_at + timedelta(
            seconds=constants.CONAB_CUSTOS_CATALOG_TTL_SECONDS
        ):
            return None
        return snapshot.model_copy(deep=True)


def _store_catalog(value: _CatalogSnapshot, family: str = "agricolas") -> None:
    global _catalog_cache, _sociobio_catalog_cache
    with _state_lock:
        previous = _catalog_cache if family == "agricolas" else _sociobio_catalog_cache
        if previous is None or previous.received_at <= value.received_at:
            if family == "agricolas":
                _catalog_cache = value.model_copy(deep=True)
            else:
                _sociobio_catalog_cache = value.model_copy(deep=True)


def checked_url(url: str) -> str:
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != "www.gov.br"
        or not parts.path.startswith("/conab/")
        or parts.username
        or parts.password
        or parts.port not in {None, 443}
    ):
        raise InvalidParameterError("URL de catálogo fora do canal oficial CONAB")
    return url


def culture(text: str) -> str:
    text = re.sub(r"^serie[\s-]*historica[\s-]*custos?[\s-]*", "", key(text).lower())
    text = re.sub(r"[\s-]*[0-9]{4}[\s-]*a[\s-]*[0-9]{4}.*$", "", text)
    text = text.replace("-", " ").strip()
    return "algodao" if text == "algodao em pluma" else models.normalize_cultura(text)


def _catalog_resources(
    content: Tag, url: str, folder_culture: str | None
) -> list[models.RecursoCusto]:
    resources = []
    for anchor in content.select('article.entry a[title="File"][href]'):
        target = urljoin(url, str(anchor["href"]))
        if not target.endswith("/view"):
            continue
        checked_url(target)
        identifier = urlsplit(target).path.removesuffix("/view").rsplit("/", 1)[-1]
        title = anchor.get_text(" ", strip=True)
        resources.append(
            models.RecursoCusto(
                planilha=identifier,
                cultura=folder_culture or culture(title),
                titulo=title,
                pagina_url=target,
            )
        )
    return resources


def _catalog_subfolders(
    content: Tag, resources: dict[str, models.RecursoCusto]
) -> list[tuple[str, str]]:
    root_path = urlsplit(CATALOG_URL).path.rstrip("/") + "/"
    folders: dict[str, str] = {}
    for anchor in content.select("a.internal-link[href]"):
        target = urljoin(TAB_URL, str(anchor["href"]))
        parts = urlsplit(target)
        if not parts.path.startswith(root_path):
            continue
        slug = parts.path.removeprefix(root_path).rstrip("/")
        if not slug or "/" in slug or "." in slug or "b_start" in parts.query or slug in resources:
            continue
        folders[checked_url(target)] = models.normalize_cultura(slug)
    return list(folders.items())


def _catalog_next_pages(soup: BeautifulSoup, url: str) -> list[str]:
    next_links = soup.select(".pagination a[rel=next], .batching a.next, a[rel=next], a.proximo")
    urls = {urljoin(url, str(a["href"])) for a in next_links if a.get("href")}
    if len(urls) > 1:
        raise ParseError(
            source="conab_custo",
            parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
            reason="Paginação ambígua",
        )
    return [checked_url(target) for target in urls]


def _sociobio_culture(title: str) -> str:
    value = key(title).lower().removesuffix(".xlsx").replace("_", "-")
    value = re.sub(r"^serie-historica-custos?-", "", value)
    value = re.split(r"-serie-historica|-\d{4}", value, maxsplit=1)[0]
    return models.normalize_produto_sociobio(value)


def _sociobio_active_links(content: Tag, tab_url: str) -> set[str]:
    root = constants.CONAB_SOCIOBIO_CATALOG_URL.rstrip("/") + "/"
    targets = {
        checked_url(urljoin(tab_url, str(anchor["href"]))).removesuffix("/view")
        for anchor in content.select("a[href]")
        if urljoin(tab_url, str(anchor["href"])).startswith(root)
        and re.search(r"\.xlsx?(?:/view)?$", str(anchor["href"]), re.I)
    }
    if not targets:
        raise ParseError(
            source="conab_sociobio",
            parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
            reason="Aba sem arquivos ativos",
        )
    return targets


@dataclass
class Acquisition:
    receipts: list[dict[str, Any]] = field(default_factory=list)
    received_bytes: int = 0
    culturas_catalogo: list[str] = field(default_factory=list)
    catalog_cache: Literal["hit", "miss", "bypass"] | None = None
    catalog_snapshot: _CatalogSnapshot | None = field(default=None, repr=False)
    family: Literal["agricolas", "sociobiodiversidade"] = field(default="agricolas", kw_only=True)

    def details(self) -> dict[str, Any]:
        snapshot = self.catalog_snapshot
        return copy.deepcopy(
            {
                "resources": self.receipts,
                "received_bytes": self.received_bytes,
                "catalog_cache": self.catalog_cache,
                "catalog_received_at": snapshot.received_at.isoformat() if snapshot else None,
                "catalog_acquisition": {
                    "resources": snapshot.receipts,
                    "received_bytes": snapshot.received_bytes,
                }
                if snapshot
                else None,
            }
        )

    def last_receipt(self) -> dict[str, Any]:
        if self.receipts:
            return self.receipts[-1]
        if self.catalog_snapshot is not None:
            return self.catalog_snapshot.receipts[-1]
        raise RuntimeError("Aquisição sem recibo de catálogo ou planilha")

    async def get(self, url: str, role: str) -> bytes:
        checked_url(url)
        http = httpx.AsyncClient(
            timeout=get_timeout(),
            headers={
                **UserAgentRotator.get_headers(source="conab_custo"),
                "Accept-Encoding": "identity",
            },
            follow_redirects=False,
        )
        primary = None
        try:

            async def attempt() -> httpx.Response:
                content, response = await self._attempt(http, url, role)
                response._content = content
                return response

            try:
                response = await retry_on_status(attempt, source="conab_custo")
                responses.raise_for_status(response, source="conab_custo")
                return response.content
            except BaseException as error:
                primary = error
                error.__dict__["conab_custos_acquisition"] = self.details()
                raise
        finally:
            try:
                await http.aclose()
            except (OSError, httpx.HTTPError) as error:
                if self.receipts:
                    self.receipts[-1]["client_close_error"] = str(error)
                if primary is None:
                    raise
                primary.__dict__["conab_custos_acquisition"] = self.details()

    async def _attempt(
        self, http: httpx.AsyncClient, url: str, role: str
    ) -> tuple[bytes, httpx.Response]:
        if len(self.receipts) >= MAX_REQUESTS:
            raise SourceUnavailableError(
                source="conab_custo", last_error="Orçamento de requisições excedido"
            )
        record: dict[str, Any] = {
            "index": len(self.receipts),
            "url": url,
            "role": role,
            "started_at": utcnow_aware().isoformat(),
            "bytes": 0,
            "eof": False,
            "closed": False,
        }
        self.receipts.append(record)
        content = bytearray()
        digest = hashlib.sha256()
        response = None
        primary = None
        try:
            response = await http.send(http.build_request("GET", url), stream=True)
            record.update(
                status=response.status_code,
                effective_url=str(response.url),
                headers=dict(response.headers),
            )
            if response.status_code == 206 or "content-range" in response.headers:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Resposta parcial não constitui workbook completo",
                )
            if response.is_redirect:
                raise SourceUnavailableError(
                    source="conab_custo",
                    url=url,
                    last_error="Redirecionamento HTTP recusado: a aquisição só aceita resposta direta da URL pedida",
                )
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Content-Encoding incompatível",
                )
            length = response.headers.get("content-length")
            if length is not None and (
                not re.fullmatch(r"[0-9]{1,10}", length) or int(length) > MAX_BODY_BYTES
            ):
                raise SourceUnavailableError(
                    source="conab_custo", last_error="Content-Length inválido ou excessivo"
                )
            async for chunk in response.aiter_bytes():
                record["bytes"] += len(chunk)
                self.received_bytes += len(chunk)
                digest.update(chunk)
                if record["bytes"] > MAX_BODY_BYTES or self.received_bytes > MAX_TOTAL_BYTES:
                    raise SourceUnavailableError(
                        source="conab_custo", last_error="Orçamento de bytes excedido"
                    )
                content.extend(chunk)
            record["eof"] = True
            if length is not None and int(length) != record["bytes"]:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Corpo difere de Content-Length",
                )
        except BaseException as error:
            primary = error
            record["error"] = {"type": type(error).__name__, "message": str(error)}
            raise
        finally:
            record.update(
                sha256=digest.hexdigest() if response is not None else None,
                finished_at=utcnow_aware().isoformat(),
            )
            if response is not None:
                try:
                    await response.aclose()
                    record["closed"] = True
                except (OSError, httpx.HTTPError) as error:
                    record["close_error"] = str(error)
                    if primary is None:
                        raise
        return bytes(content), response

    async def catalog(
        self, requested: str | None, *, use_cache: bool = True
    ) -> list[models.RecursoCusto]:
        if type(use_cache) is not bool:
            raise InvalidParameterError("use_cache deve ser bool")
        self.catalog_cache = "miss" if use_cache else "bypass"
        snapshot = _get_cached_catalog(self.family) if use_cache else None
        if snapshot is not None:
            self.catalog_cache = "hit"
        else:
            first_receipt = len(self.receipts)
            received_before = self.received_bytes
            resources = await self._crawl_catalog()
            snapshot = _CatalogSnapshot(
                resources=resources,
                culturas_catalogo=sorted({r.cultura for r in resources.values()}),
                received_at=now(),
                receipts=copy.deepcopy(self.receipts[first_receipt:]),
                received_bytes=self.received_bytes - received_before,
            )
            if use_cache:
                _store_catalog(snapshot, self.family)
        self.catalog_snapshot = snapshot
        self.culturas_catalogo = list(snapshot.culturas_catalogo)
        return sorted(
            [
                r
                for r in snapshot.resources.values()
                if requested is None
                or key(r.cultura)
                == key(
                    models.normalize_produto_sociobio(requested)
                    if self.family == "sociobiodiversidade"
                    else models.normalize_cultura(requested)
                )
            ],
            key=lambda r: r.planilha,
        )

    async def _crawl_catalog(self) -> dict[str, models.RecursoCusto]:
        if self.family == "sociobiodiversidade":
            return await self._crawl_sociobio_catalog()
        resources: dict[str, models.RecursoCusto] = {}
        pending: list[tuple[str, str | None]] = [(CATALOG_URL, None), (TAB_URL, None)]
        seen: set[str] = set()
        while pending:
            url, folder_culture = pending.pop(0)
            if url in seen:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Ciclo no catálogo",
                )
            seen.add(url)
            body = await self.get(url, "catalog")
            soup = BeautifulSoup(body, "lxml")
            content = soup.select_one("#content-core") or soup.select_one("#content")
            if folder_culture and (content is None or content.select_one("article.entry") is None):
                continue
            if content is None:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Catálogo sem área de conteúdo",
                )
            if url == TAB_URL:
                pending.extend(_catalog_subfolders(content, resources))
                continue
            for resource in _catalog_resources(content, url, folder_culture):
                existing = resources.get(resource.planilha)
                if existing is not None and existing.pagina_url != resource.pagina_url:
                    raise ParseError(
                        source="conab_custo",
                        parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                        reason="Identificador de recurso ambíguo",
                    )
                resources.setdefault(resource.planilha, resource)
            pending[:0] = [(target, folder_culture) for target in _catalog_next_pages(soup, url)]
        if not resources:
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason="Catálogo sem planilhas reconhecidas",
            )
        return resources

    async def _crawl_sociobio_catalog(self) -> dict[str, models.RecursoCusto]:
        tab_url = constants.CONAB_SOCIOBIO_TAB_URL
        tab = BeautifulSoup(await self.get(tab_url, "catalog"), "lxml")
        content = tab.select_one("#content-core") or tab.select_one("#content")
        if content is None:
            raise ParseError(
                source="conab_sociobio",
                parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
                reason="Aba sem conteúdo",
            )
        active = _sociobio_active_links(content, tab_url)
        pending = [constants.CONAB_SOCIOBIO_CATALOG_URL]
        seen: set[str] = set()
        resources: dict[str, models.RecursoCusto] = {}
        while pending:
            url = pending.pop(0)
            if url in seen:
                raise ParseError(
                    source="conab_sociobio",
                    parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
                    reason="Ciclo no catálogo",
                )
            seen.add(url)
            soup = BeautifulSoup(await self.get(url, "catalog"), "lxml")
            content = soup.select_one("#content-core") or soup.select_one("#content")
            if content is None:
                raise ParseError(
                    source="conab_sociobio",
                    parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
                    reason="Catálogo sem conteúdo",
                )
            for resource in _catalog_resources(content, url, None):
                value = resource.model_dump()
                value["cultura"] = _sociobio_culture(resource.titulo)
                value["ativo"] = resource.pagina_url.removesuffix("/view") in active
                if resource.planilha in resources:
                    raise ParseError(
                        source="conab_sociobio",
                        parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
                        reason="Recurso repetido no catálogo",
                    )
                resources[resource.planilha] = models.RecursoSociobio(**value)
            pending.extend(_catalog_next_pages(soup, url))
        found = {resource.pagina_url.removesuffix("/view") for resource in resources.values()}
        if not resources or active - found:
            raise ParseError(
                source="conab_sociobio",
                parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
                reason=f"Recursos ativos ausentes da pasta: {sorted(active - found)}",
            )
        return resources

    async def workbook(self, resource: models.RecursoCusto) -> bytes:
        metadata = await self.get(resource.pagina_url, "workbook_metadata")
        soup = BeautifulSoup(metadata, "lxml")
        node = soup.select_one("#content-core") or soup.select_one("#content")
        if node is None:
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason="Ficha de planilha sem conteúdo",
            )
        links = {
            urljoin(resource.pagina_url, str(a["href"]))
            for a in node.select("a[href]")
            if "@@download" in str(a["href"]) or re.search(r"\.xlsx?$", str(a["href"]), re.I)
        }
        if len(links) != 1:
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason=f"Ficha contém {len(links)} downloads candidatos",
            )
        url = checked_url(next(iter(links)))
        content = await self.get(url, "workbook")
        validate_download(content, kinds=("xls", "xlsx"), source="conab_custo", url=url, min_size=8)
        return content
