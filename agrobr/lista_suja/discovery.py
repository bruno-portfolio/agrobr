from __future__ import annotations

import re
import unicodedata
from pathlib import PurePosixPath
from urllib.parse import unquote, urldefrag, urljoin, urlsplit

import bs4

from agrobr.exceptions import ParseError
from agrobr.normalize import encoding

from . import models


def _text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return " ".join("".join(c for c in normalized if not unicodedata.combining(c)).split())


def _principal(value: str) -> bool:
    normalized = _text(value)
    return not ("ceac" in normalized or "ajustamento de conduta" in normalized) and (
        "lista suja" in normalized
        or ("cadastro de empregadores" in normalized and "escrav" in normalized)
    )


def validate_url(url: str, base_url: str) -> str:
    resolved = urldefrag(urljoin(base_url, url))[0]
    parsed = urlsplit(resolved)
    base = urlsplit(base_url)
    path = unquote(parsed.path)
    prefix = "/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/"
    if (
        parsed.scheme != "https"
        or parsed.netloc != base.netloc
        or not path.startswith(prefix)
        or ".." in PurePosixPath(path).parts
    ):
        raise ParseError(
            source="lista_suja",
            parser_version=4,
            reason="URL de publicação fora do domínio/caminho oficial",
        )
    return resolved


def _links(block: bs4.Tag, base_url: str) -> dict[str, str]:
    resources: dict[str, str] = {}
    anchors = [block] if block.name == "a" and block.get("href") else block.find_all("a", href=True)
    for anchor in anchors:
        label = anchor.get_text(" ", strip=True)
        if not label or "ceac" in _text(label):
            continue
        href = str(anchor["href"])
        suffix = PurePosixPath(urlsplit(href).path).suffix.lower().removeprefix(".")
        if suffix not in {"csv", "pdf", "txt"}:
            match = re.search(r"\.(csv|pdf|txt)\b", label, re.IGNORECASE)
            if match is None:
                continue
            suffix = match[1].lower()
        resolved = validate_url(href, base_url)
        if "ceac" in _text(unquote(resolved)):
            continue
        if suffix in resources and resources[suffix] != resolved:
            raise ParseError(
                source="lista_suja",
                parser_version=4,
                reason=f"Publicação ambígua para formato {suffix}",
            )
        resources[suffix] = resolved
    return resources


def _merge(target: dict[str, str], incoming: dict[str, str]) -> None:
    for kind, url in incoming.items():
        if kind in target and target[kind] != url:
            raise ParseError(
                source="lista_suja",
                parser_version=4,
                reason=f"Mais de um recurso principal para {kind}",
            )
        target[kind] = url


def _table_publications(soup: bs4.BeautifulSoup, base_url: str) -> list[tuple[str, dict[str, str]]]:
    found = []
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"], recursive=False)
        if not cells:
            continue
        labels = cells[0].find_all("p", recursive=False) or [cells[0]]
        for index, label in enumerate(labels):
            title = label.get_text(" ", strip=True)
            if not _principal(title):
                continue
            resources: dict[str, str] = {}
            for cell in cells[1:]:
                paragraphs = cell.find_all("p", recursive=False)
                if len(labels) > 1:
                    if len(paragraphs) != len(labels):
                        raise ParseError(
                            source="lista_suja",
                            parser_version=4,
                            reason="Tabela principal/CEAC sem correspondência entre títulos e links",
                        )
                    block = paragraphs[index]
                else:
                    block = cell
                _merge(resources, _links(block, base_url))
            found.append((title, resources))
    return found


def _section_publications(
    soup: bs4.BeautifulSoup, base_url: str
) -> list[tuple[str, dict[str, str]]]:
    found = []
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        title = heading.get_text(" ", strip=True)
        if not _principal(title):
            continue
        resources: dict[str, str] = {}
        for sibling in heading.next_siblings:
            if not isinstance(sibling, bs4.Tag):
                continue
            if re.fullmatch(r"h[1-6]", sibling.name) or "ceac" in _text(
                sibling.get_text(" ", strip=True)
            ):
                break
            _merge(resources, _links(sibling, base_url))
        found.append((title, resources))
    return found


def parse_publication(content: bytes, url: str) -> models.PublicationResources:
    html, _ = encoding.decode_content(content, source="lista_suja")
    soup = bs4.BeautifulSoup(html, "lxml")
    publications = _table_publications(soup, url) + _section_publications(soup, url)
    selected = [
        (title, resources)
        for title, resources in publications
        if {"csv", "pdf"}.intersection(resources)
    ]
    if not selected:
        raise ParseError(
            source="lista_suja",
            parser_version=4,
            reason="Página sem recursos reconhecidos do cadastro principal",
        )
    title, resources = selected[0]
    for _, other in selected[1:]:
        if other != resources:
            raise ParseError(
                source="lista_suja",
                parser_version=4,
                reason="Página com publicações principais ambíguas",
            )
    return models.PublicationResources(title=title, resources=resources)
