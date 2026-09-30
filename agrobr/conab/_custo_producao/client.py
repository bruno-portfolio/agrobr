from __future__ import annotations

import re
from io import BytesIO

import httpx

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.regions import UFS_VALIDAS
from agrobr.utils.html import parse_links_from_html as _parse_links

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.CONAB]["base"]

CUSTOS_PAGE = (
    f"{BASE_URL}/pt-br/atuacao/informacoes-agropecuarias"
    "/custos-de-producao/planilhas-de-custos-de-producao"
)

_TAB_SLUGS = [
    "copy_of_agricolas",
    "pecuarios",
    "copy",
]

TIMEOUT = get_timeout()

ACCEPT_EXCEL_HTML = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
    "*/*;q=0.8"
)

_XLS_PATTERN = r"\.xlsx?"
_UF_RE = re.compile(r"\b([A-Z]{2})\b")


async def fetch_custos_page() -> str:
    combined_html = ""
    headers = UserAgentRotator.get_headers(source="conab_custo")
    headers["Accept"] = ACCEPT_EXCEL_HTML

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
        for slug in _TAB_SLUGS:
            url = f"{CUSTOS_PAGE}/{slug}"
            try:
                response = await retry_on_status(
                    lambda _u=url: client.get(_u),  # type: ignore[misc]
                    source="conab_custo",
                )
                response.raise_for_status()
                combined_html += response.text
                logger.info("conab_custo_tab_ok", slug=slug, content_length=len(response.text))
            except (httpx.HTTPError, SourceUnavailableError) as e:
                logger.warning("conab_custo_tab_error", slug=slug, error=str(e))

        if not combined_html:
            try:
                response = await retry_on_status(
                    lambda: client.get(CUSTOS_PAGE),
                    source="conab_custo",
                )
                response.raise_for_status()
                combined_html = response.text
                logger.info("conab_custo_page_ok", content_length=len(response.text))
            except (httpx.HTTPError, SourceUnavailableError) as e:
                raise SourceUnavailableError(
                    source="conab_custo",
                    url=CUSTOS_PAGE,
                    last_error=str(e),
                ) from e

    return combined_html


async def download_xlsx(url: str) -> BytesIO:
    if not url.startswith("http"):
        url = f"{BASE_URL}{url}"

    logger.debug("conab_custo_download_xlsx", url=url)

    headers = UserAgentRotator.get_headers(source="conab_custo")
    headers["Accept"] = ACCEPT_EXCEL_HTML

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
        try:
            response = await retry_on_status(
                lambda: client.get(url),
                source="conab_custo",
            )
            response.raise_for_status()
            content = response.content

            logger.info(
                "conab_custo_download_ok",
                source="conab_custo",
                size_bytes=len(content),
            )

            return BytesIO(content)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                source="conab_custo",
                url=url,
                last_error=str(e),
            ) from e


def _enrich_link_hints(link: dict[str, str]) -> None:
    text = link["text"]
    safra_match = re.search(r"(\d{4})/(\d{2})", text)
    if safra_match:
        link["safra_hint"] = safra_match.group(0)
    uf_match = _UF_RE.search(text)
    if uf_match and uf_match.group(1) in UFS_VALIDAS:
        link["uf_hint"] = uf_match.group(1)


def parse_links_from_html(html: str) -> list[dict[str, str]]:
    links = _parse_links(html, base_url=BASE_URL, pattern=_XLS_PATTERN)
    for link in links:
        _enrich_link_hints(link)
    logger.info("conab_custo_links_parsed", count=len(links))
    return links


_AGRICOLAS_PATH = "/arquivos-custo-de-producao/"


def _extract_folder_urls(html: str) -> list[str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    folders: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = str(a["href"]).rstrip("/")
        if _AGRICOLAS_PATH not in href:
            continue
        if re.search(_XLS_PATTERN, href) or "resolveuid/" in href:
            continue
        slug = href.rsplit("/", 1)[-1]
        if "serie-historica" in slug:
            continue
        url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if url not in seen:
            seen.add(url)
            folders.append(url)
    return folders


async def _crawl_folder(folder_url: str) -> list[dict[str, str]]:
    headers = UserAgentRotator.get_headers(source="conab_custo")
    headers["Accept"] = ACCEPT_EXCEL_HTML
    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            headers=headers,
            follow_redirects=True,
        ) as http:
            response = await retry_on_status(
                lambda: http.get(folder_url),
                source="conab_custo",
            )
            response.raise_for_status()
            folder_html = response.text
    except (httpx.HTTPError, SourceUnavailableError) as e:
        logger.debug("conab_custo_folder_error_detail", url=folder_url)
        logger.warning("conab_custo_folder_error", source="conab_custo", error=str(e))
        return []

    links = _parse_links(folder_html, base_url=BASE_URL, pattern=_XLS_PATTERN)
    for link in links:
        link["url"] = link["url"].removesuffix("/view")
        _enrich_link_hints(link)
    logger.info("conab_custo_folder_ok", source="conab_custo", links=len(links))
    return links
