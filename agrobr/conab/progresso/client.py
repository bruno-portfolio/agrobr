from __future__ import annotations

import re
from functools import partial

import httpx
import structlog
from bs4 import BeautifulSoup

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

from .models import BASE_URL

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)",
    "Accept": "*/*",
}

PAGE_SIZE = 20


def _extract_week_links(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    results: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        text = a.get_text(strip=True)
        if "acompanhamento-das-lavouras" in href and "Acompanhamento" in text and href not in seen:
            seen.add(href)
            full = href if href.startswith("http") else f"https://www.gov.br{href}"
            results.append((text, full))
    return results


def _extract_plantio_link(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        if "plantio" in href.lower() and "colheita" in href.lower():
            return href if href.startswith("http") else f"https://www.gov.br{href}"
    return None


def _parse_week_date(text: str) -> str:
    match = re.search(r"(\d{2}/\d{2})\s*a\s*(\d{2}/\d{2}/\d{2})", text)
    if match:
        return match.group(2)
    match = re.search(r"(\d{2}/\d{2})\s*a\s*(\d{2}/\d{2})", text)
    if match:
        return match.group(2)
    return text.strip()


async def list_semanas(max_pages: int = 4) -> list[tuple[str, str]]:
    all_weeks: list[tuple[str, str]] = []
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
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
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        logger.debug("conab_progresso_week", url=week_url)
        resp = await retry_on_status(lambda: client.get(week_url), source="conab")
        if resp.status_code != 200:
            raise SourceUnavailableError(
                source="conab_progresso",
                url=week_url,
                last_error=f"HTTP {resp.status_code}",
            )

        xlsx_url = _extract_plantio_link(resp.text)
        if xlsx_url is None:
            raise SourceUnavailableError(
                source="conab_progresso",
                url=week_url,
                last_error="Link plantio/colheita nao encontrado na pagina semanal",
            )

        logger.debug("conab_progresso_xlsx", url=xlsx_url)
        xlsx_resp = await retry_on_status(lambda: client.get(xlsx_url), source="conab")
        if xlsx_resp.status_code != 200:
            raise SourceUnavailableError(
                source="conab_progresso",
                url=xlsx_url,
                last_error=f"HTTP {xlsx_resp.status_code}",
            )

        ct = xlsx_resp.headers.get("content-type", "")
        if "spreadsheet" not in ct and "excel" not in ct and len(xlsx_resp.content) < 1000:
            raise SourceUnavailableError(
                source="conab_progresso",
                url=xlsx_url,
                last_error=f"Content-Type inesperado: {ct}",
            )

        logger.info("conab_progresso_xlsx_ok", url=xlsx_url, size=len(xlsx_resp.content))
        return xlsx_resp.content, xlsx_url


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
