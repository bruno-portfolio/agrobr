from __future__ import annotations

import posixpath
from functools import partial
from urllib.parse import urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io

from .models import BASE_URL

_CONAB_BASE = URLS[Fonte.CONAB]["base"]

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=60.0)

PAGE_SIZE = 20

_CONAB_HOST = urlsplit(BASE_URL).netloc


async def _so_na_conab(request: httpx.Request) -> None:
    """Recusa, antes de enviar, o pedido (a ``semana_url``, o redirecionamento dela ou um link) fora da CONAB."""
    url = request.url
    if (
        url.scheme != "https"
        or url.host != _CONAB_HOST
        or url.port is not None
        or url.userinfo
        or not f"{url.path}/".startswith("/conab/")
    ):
        raise InvalidParameterError(
            f"pedido fora da CONAB recusado: {url}; a semana_url e os links dela têm de ficar em "
            f"https://{_CONAB_HOST}/conab/"
        )


def _extract_week_links(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    results: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        text = a.get_text(strip=True)
        if "acompanhamento-das-lavouras" in href and "Acompanhamento" in text and href not in seen:
            seen.add(href)
            full = href if href.startswith("http") else f"{_CONAB_BASE}{href}"
            results.append((text, full))
    return results


def _is_spreadsheet_link(url: str) -> bool:
    return urlsplit(url).path.lower().endswith((".xlsx", ".xls", "/@@download/file"))


def _extract_plantio_link(html: str, page_url: str = _CONAB_BASE + "/") -> str | None:
    soup = BeautifulSoup(html, "lxml")
    node = soup.select_one("#content-core") or soup.select_one("#content") or soup
    candidates = []
    for anchor in node.select("a[href]"):
        label = (str(anchor["href"]) + " " + anchor.get_text(" ", strip=True)).lower()
        if "plantio" in label and "colheita" in label:
            candidates.append(anchor)
    for anchor in candidates:
        if _is_spreadsheet_link(str(anchor["href"])):
            return urljoin(page_url, str(anchor["href"]))
    for anchor in candidates:
        if (
            urlsplit(str(anchor["href"])).path.rstrip("/").endswith("/view")
            or anchor.get("title") == "File"
        ):
            return urljoin(page_url, str(anchor["href"]))
    for anchor in candidates:
        if not posixpath.splitext(urlsplit(str(anchor["href"])).path)[1]:
            return urljoin(page_url, str(anchor["href"]))
    return None


def _extract_download_link(html: str, page_url: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    node = soup.select_one("#content-core") or soup.select_one("#content") or soup
    links = {
        urljoin(page_url, str(anchor["href"]))
        for anchor in node.select("a[href]")
        if _is_spreadsheet_link(str(anchor["href"]))
    }
    if len(links) != 1:
        raise SourceUnavailableError(
            source="conab_progresso",
            url=page_url,
            last_error=f"Ficha contém {len(links)} downloads de planilha candidatos: {page_url}",
        )
    return next(iter(links))


async def _get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    response = await retry_on_status(partial(client.get, url), source="conab")
    if response.status_code != 200:
        raise SourceUnavailableError(
            source="conab_progresso", url=url, last_error=f"HTTP {response.status_code}"
        )
    return response


def _validate_spreadsheet(content: bytes, url: str) -> None:
    try:
        io.validate_download(
            content, kinds=("xlsx", "xls"), source="conab_progresso", url=url, min_size=8
        )
    except SourceUnavailableError as error:
        preview = content[:60].decode("utf-8", errors="replace")
        reason = (
            "página HTML recebida no lugar do arquivo"
            if content.lstrip().startswith(b"<")
            else "assinatura XLSX/XLS inválida"
        )
        raise SourceUnavailableError(
            source="conab_progresso",
            url=url,
            last_error=f"{reason}; url={url}; início={preview!r}",
        ) from error


async def _download_spreadsheet(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    response = await _get(client, url)
    try:
        _validate_spreadsheet(response.content, url)
    except SourceUnavailableError:
        if _is_spreadsheet_link(url):
            raise
        url = _extract_download_link(response.text, str(response.url))
        response = await _get(client, url)
        _validate_spreadsheet(response.content, url)
    return response.content, url


async def list_semanas(max_pages: int = 4) -> list[tuple[str, str]]:
    all_weeks: list[tuple[str, str]] = []
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="conab_progresso"),
        follow_redirects=True,
    ) as client:
        for page in range(max_pages):
            offset = page * PAGE_SIZE
            url = f"{BASE_URL}?b_start:int={offset}" if offset else BASE_URL
            logger.debug("conab_progresso_list", url=url, page=page)
            response = await retry_on_status(partial(client.get, url), source="conab")
            if response.status_code != 200:
                break
            weeks = _extract_week_links(response.text)
            if not weeks:
                break
            all_weeks.extend(weeks)
    return all_weeks


async def fetch_xlsx_semanal(week_url: str) -> tuple[bytes, str]:
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="conab_progresso"),
        follow_redirects=True,
        event_hooks={"request": [_so_na_conab]},
    ) as client:
        logger.debug("conab_progresso_week", url=week_url)
        resp = await _get(client, week_url)
        xlsx_url = _extract_plantio_link(resp.text, str(resp.url))
        if xlsx_url is None:
            raise SourceUnavailableError(
                source="conab_progresso",
                url=week_url,
                last_error="Link plantio/colheita nao encontrado na pagina semanal",
            )

        logger.debug("conab_progresso_xlsx", url=xlsx_url)
        content, download_url = await _download_spreadsheet(client, xlsx_url)
        logger.info("conab_progresso_xlsx_ok", source="conab_progresso", size=len(content))
        return content, download_url


async def fetch_latest() -> tuple[bytes, str, str]:
    weeks = await list_semanas(max_pages=1)
    if not weeks:
        raise SourceUnavailableError(
            source="conab_progresso",
            url=BASE_URL,
            last_error="Nenhuma semana encontrada na listagem",
        )

    desc, week_url = weeks[0]
    xlsx_bytes, xlsx_url = await fetch_xlsx_semanal(week_url)
    return xlsx_bytes, xlsx_url, desc
