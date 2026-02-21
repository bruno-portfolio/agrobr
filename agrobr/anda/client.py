from __future__ import annotations

import re

import httpx
import structlog

from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://anda.org.br"
ESTATISTICAS_URL = f"{BASE_URL}/recursos/"

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

HEADERS = {"User-Agent": "agrobr/0.7.1 (https://github.com/bruno-portfolio/agrobr)"}


async def _get_with_retry(url: str) -> httpx.Response:
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        follow_redirects=True,
        headers=HEADERS,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anda",
        )
        response.raise_for_status()
        return response


async def fetch_estatisticas_page() -> str:
    from agrobr.exceptions import SourceUnavailableError

    logger.info("anda_fetch_page", url=ESTATISTICAS_URL)
    response = await _get_with_retry(ESTATISTICAS_URL)
    html = response.text

    if len(html) < 2_000 or "<a" not in html.lower():
        raise SourceUnavailableError(
            source="anda",
            url=ESTATISTICAS_URL,
            last_error=(
                f"HTML response too small or missing links ({len(html)} chars, no '<a' tag found)"
            ),
        )

    return html


async def download_file(url: str) -> bytes:
    from agrobr.exceptions import SourceUnavailableError

    logger.info("anda_download", url=url)
    response = await _get_with_retry(url)
    content = response.content

    if len(content) < 500:
        raise SourceUnavailableError(
            source="anda",
            url=url,
            last_error=(
                f"Downloaded file too small ({len(content)} bytes), "
                f"expected a valid PDF or Excel file"
            ),
        )

    return content


def parse_links_from_html(html: str, pattern: str = r"\.pdf|\.xlsx?") -> list[dict[str, str]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    links: list[dict[str, str]] = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if re.search(pattern, str(href), re.IGNORECASE):
            full_url = str(href)
            if full_url.startswith("/"):
                full_url = f"{BASE_URL}{full_url}"

            links.append(
                {
                    "url": full_url,
                    "text": a_tag.get_text(strip=True),
                }
            )

    logger.info("anda_links_found", count=len(links))
    return links


async def fetch_entregas_pdf(ano: int) -> tuple[bytes, int]:
    html = await fetch_estatisticas_page()
    links = parse_links_from_html(html, pattern=r"\.pdf")

    ano_str = str(ano)
    candidates = [link for link in links if ano_str in link["text"]]

    if not candidates:
        candidates = [link for link in links if ano_str in link["url"]]

    priority = [
        link
        for link in candidates
        if re.search(r"entrega|fertiliz|indicador", f"{link['text']} {link['url']}", re.IGNORECASE)
    ]

    target = priority[0] if priority else (candidates[0] if candidates else None)

    if not target:
        all_years = sorted(
            {m.group(0) for link in links for m in re.finditer(r"20\d{2}", link["text"]) if m},
            reverse=True,
        )
        if all_years:
            fallback_ano = int(all_years[0])
            logger.warning("anda_ano_fallback", requested=ano, fallback=fallback_ano)
            return await fetch_entregas_pdf(fallback_ano)

        raise FileNotFoundError(
            f"PDF de entregas ANDA para {ano} não encontrado. "
            f"Links disponíveis: {[link['text'] for link in links[:10]]}"
        )

    ano_real = ano
    text_years = re.findall(r"20\d{2}", target["text"])
    if text_years:
        ano_real = int(text_years[-1])
    else:
        filename = target["url"].split("/")[-1]
        filename_years = re.findall(r"20\d{2}", filename)
        if filename_years:
            ano_real = int(filename_years[-1])

    logger.info(
        "anda_pdf_found", ano=ano, ano_real=ano_real, url=target["url"], text=target["text"]
    )
    pdf_bytes = await download_file(target["url"])
    return pdf_bytes, ano_real
