from __future__ import annotations

import re

import httpx
import structlog

from agrobr.constants import MIN_HTML_SIZE, MIN_ZIP_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils
from agrobr.utils.html import parse_links_from_html as _parse_links

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.ANDA]["base"]
ESTATISTICAS_URL = URLS[Fonte.ANDA]["estatisticas"]

TIMEOUT = get_timeout(read=60.0)


async def _get_with_retry(url: str) -> httpx.Response:
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        follow_redirects=True,
        headers=UserAgentRotator.get_bot_headers(),
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anda",
        )
        response.raise_for_status()
        return response


async def fetch_estatisticas_page() -> str:
    logger.debug("anda_fetch_page", url=ESTATISTICAS_URL)
    logger.info("anda_fetch_page", source="anda")
    response = await _get_with_retry(ESTATISTICAS_URL)
    html = response.text

    if len(html) < MIN_HTML_SIZE or "<a" not in html.lower():
        raise SourceUnavailableError(
            source="anda",
            url=ESTATISTICAS_URL,
            last_error=(
                f"Resposta HTML muito pequena ou sem links ({len(html)} caracteres, "
                "tag '<a' não encontrada)"
            ),
        )

    return html


async def download_file(url: str) -> bytes:
    logger.debug("anda_download", url=url)
    logger.info("anda_download", source="anda")
    response = await _get_with_retry(url)
    content = response.content

    clean_url = url.lower().split("?", 1)[0]
    kinds: tuple[str, ...]
    if clean_url.endswith(".pdf"):
        kinds = ("pdf",)
    elif clean_url.endswith(".xlsx"):
        kinds = ("xlsx",)
    elif clean_url.endswith(".xls"):
        kinds = ("xls",)
    else:
        kinds = ("pdf", "xls", "xlsx")

    io_utils.validate_download(
        content,
        kinds=kinds,
        source="anda",
        url=url,
        min_size=MIN_ZIP_SIZE,
    )

    return content


def parse_links_from_html(html: str, pattern: str = r"\.pdf|\.xlsx?") -> list[dict[str, str]]:
    links = _parse_links(html, base_url=BASE_URL, pattern=pattern, dedup=False)
    logger.info("anda_links_found", count=len(links))
    return links


def _select_pdf_target(links: list[dict[str, str]], ano_str: str) -> dict[str, str] | None:
    candidates = [link for link in links if ano_str in link["text"]]
    if not candidates:
        candidates = [link for link in links if ano_str in link["url"]]

    priority = [
        link
        for link in candidates
        if re.search(r"entrega|fertiliz|indicador", f"{link['text']} {link['url']}", re.IGNORECASE)
    ]
    return priority[0] if priority else (candidates[0] if candidates else None)


def _extract_ano_real(target: dict[str, str], ano: int) -> int:
    text_years = re.findall(r"20\d{2}", target["text"])
    if text_years:
        return int(text_years[-1])

    filename = target["url"].split("/")[-1]
    filename_years = re.findall(r"20\d{2}", filename)
    if filename_years:
        return int(filename_years[-1])

    return ano


async def fetch_entregas_pdf(ano: int) -> tuple[bytes, int]:
    html = await fetch_estatisticas_page()
    links = parse_links_from_html(html, pattern=r"\.pdf")

    target = _select_pdf_target(links, str(ano))

    if not target:
        anos_disponiveis = sorted(
            {m.group(0) for link in links for m in re.finditer(r"20\d{2}", link["text"])},
            reverse=True,
        )
        raise SourceUnavailableError(
            source="anda",
            url=ESTATISTICAS_URL,
            last_error=(
                f"PDF de entregas ANDA para {ano} não encontrado. "
                f"Anos disponíveis no site: {anos_disponiveis or 'nenhum'}"
            ),
        )

    ano_real = _extract_ano_real(target, ano)

    logger.debug("anda_pdf_found_detail", url=target["url"])
    logger.info("anda_pdf_found", source="anda", ano=ano, ano_real=ano_real, text=target["text"])
    pdf_bytes = await download_file(target["url"])
    return pdf_bytes, ano_real
