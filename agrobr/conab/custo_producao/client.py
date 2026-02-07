"""Cliente HTTP para download de planilhas de custo de produção CONAB.

A CONAB publica planilhas Excel (.xlsx) com custos detalhados por hectare.
Não há API REST — os arquivos são baixados diretamente via HTTP.

Fonte: https://www.conab.gov.br/info-agro/custos-de-producao
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import Any

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BASE_URL = "https://www.gov.br"

CUSTOS_PAGE = (
    f"{BASE_URL}/conab/pt-br/atuacao/informacoes-agropecuarias"
    "/custos-de-producao/planilhas-de-custos-de-producao"
)

_TAB_SLUGS = [
    "copy_of_agricolas",  # Hortícolas / sociobiodiversidade
    "pecuarios",  # Pecuários
    "copy",  # Grãos / fibras (custos-de-producao/custos-de-producao)
]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=_settings.timeout_read,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
        "*/*;q=0.8"
    ),
}


async def fetch_custos_page() -> str:
    """Busca HTML combinado das tabs de planilhas de custo de produção.

    O gov.br carrega as tabs via sub-URLs separadas.  Concatenamos o HTML
    de todas para que ``parse_links_from_html`` encontre todos os xlsx.

    Returns:
        HTML combinado de todas as tabs.

    Raises:
        SourceUnavailableError: Se nenhuma tab estiver acessível.
    """
    combined_html = ""

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        for slug in _TAB_SLUGS:
            url = f"{CUSTOS_PAGE}/{slug}"
            try:
                response = await client.get(url)
                response.raise_for_status()
                combined_html += response.text
                logger.info("conab_custo_tab_ok", slug=slug, content_length=len(response.text))
            except httpx.HTTPError as e:
                logger.warning("conab_custo_tab_error", slug=slug, error=str(e))

        if not combined_html:
            # Fallback: tenta a página principal
            try:
                response = await client.get(CUSTOS_PAGE)
                response.raise_for_status()
                combined_html = response.text
                logger.info("conab_custo_page_ok", content_length=len(response.text))
            except httpx.HTTPError as e:
                raise SourceUnavailableError(
                    source="conab_custo",
                    url=CUSTOS_PAGE,
                    last_error=str(e),
                ) from e

    return combined_html


async def download_xlsx(url: str) -> BytesIO:
    """Baixa um arquivo Excel diretamente via HTTP.

    Args:
        url: URL completa do arquivo .xlsx.

    Returns:
        BytesIO com conteúdo do arquivo.

    Raises:
        SourceUnavailableError: Se não conseguir baixar.
    """
    if not url.startswith("http"):
        url = f"{BASE_URL}{url}"

    logger.info("conab_custo_download_xlsx", url=url)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            content = response.content

            logger.info(
                "conab_custo_download_ok",
                url=url,
                size_bytes=len(content),
            )

            return BytesIO(content)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                source="conab_custo",
                url=url,
                last_error=str(e),
            ) from e


def parse_links_from_html(html: str) -> list[dict[str, str]]:
    """Extrai links de planilhas .xlsx da página HTML.

    Args:
        html: HTML combinado das tabs de custos CONAB (gov.br).

    Returns:
        Lista de dicts com chaves: url, titulo, cultura_hint, uf_hint, safra_hint.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    links: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag["href"])
        if ".xlsx" not in href.lower():
            continue

        if href in seen_urls:
            continue
        seen_urls.add(href)

        titulo = a_tag.get_text(strip=True)
        if not titulo:
            titulo = href.split("/")[-1].replace("-", " ").replace(".xlsx", "")

        full_url = href
        if full_url.startswith("/"):
            full_url = f"{BASE_URL}{full_url}"

        link_info: dict[str, str] = {
            "url": full_url,
            "titulo": titulo,
        }

        safra_match = re.search(r"(\d{4})/(\d{2})", titulo)
        if safra_match:
            link_info["safra_hint"] = safra_match.group(0)

        uf_match = re.search(
            r"\b(AC|AL|AM|AP|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO)\b",
            titulo,
        )
        if uf_match:
            link_info["uf_hint"] = uf_match.group(1)

        links.append(link_info)

    logger.info("conab_custo_links_parsed", count=len(links))
    return links


async def fetch_xlsx_for_cultura(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
) -> tuple[BytesIO, dict[str, Any]]:
    """Busca e baixa planilha de custo para uma cultura específica.

    Faz scraping da página de custos, encontra o link correto,
    e baixa o arquivo Excel.

    Args:
        cultura: Nome da cultura (ex: "soja", "milho").
        uf: Filtrar por UF (ex: "MT").
        safra: Filtrar por safra (ex: "2023/24").

    Returns:
        Tupla (BytesIO com Excel, metadata dict).

    Raises:
        SourceUnavailableError: Se não encontrar planilha adequada.
    """
    html = await fetch_custos_page()
    links = parse_links_from_html(html)

    if not links:
        raise SourceUnavailableError(
            source="conab_custo",
            url=CUSTOS_PAGE,
            last_error="Nenhum link de planilha encontrado na página",
        )

    cultura_lower = cultura.lower()
    candidates = [link for link in links if cultura_lower in link["titulo"].lower()]

    if uf:
        uf_upper = uf.upper()
        filtered = [link for link in candidates if link.get("uf_hint") == uf_upper]
        if filtered:
            candidates = filtered

    if safra:
        filtered = [link for link in candidates if link.get("safra_hint") == safra]
        if filtered:
            candidates = filtered

    if not candidates:
        raise SourceUnavailableError(
            source="conab_custo",
            url=CUSTOS_PAGE,
            last_error=f"Nenhuma planilha encontrada para cultura={cultura}, uf={uf}, safra={safra}",
        )

    selected = candidates[0]

    xlsx = await download_xlsx(selected["url"])

    metadata = {
        "url": selected["url"],
        "titulo": selected["titulo"],
        "cultura": cultura,
    }
    if selected.get("uf_hint"):
        metadata["uf"] = selected["uf_hint"]
    if selected.get("safra_hint"):
        metadata["safra"] = selected["safra_hint"]

    return xlsx, metadata
