"""Cliente HTTP para dados ANDA (Associação Nacional para Difusão de Adubos).

ANDA publica relatórios em PDF e Excel com dados de entregas de fertilizantes.
Este cliente faz download dos arquivos e retorna o conteúdo bruto (bytes)
para o parser processar.
"""

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
    """GET com retry exponencial via retry_on_status centralizado.

    Args:
        url: URL para acessar.

    Returns:
        Response HTTP.

    Raises:
        SourceUnavailableError: Se URL indisponível após retries.
    """
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
    """Busca a página de estatísticas da ANDA.

    Returns:
        HTML da página de estatísticas.
    """
    logger.info("anda_fetch_page", url=ESTATISTICAS_URL)
    response = await _get_with_retry(ESTATISTICAS_URL)
    return response.text


async def download_file(url: str) -> bytes:
    """Faz download de um arquivo (PDF ou Excel) da ANDA.

    Args:
        url: URL completa do arquivo.

    Returns:
        Conteúdo do arquivo em bytes.
    """
    logger.info("anda_download", url=url)
    response = await _get_with_retry(url)
    return response.content


def parse_links_from_html(html: str, pattern: str = r"\.pdf|\.xlsx?") -> list[dict[str, str]]:
    """Extrai links de arquivos (PDF/Excel) da página HTML.

    Args:
        html: HTML da página de estatísticas.
        pattern: Regex para filtrar extensões de arquivo.

    Returns:
        Lista de dicts com 'url' e 'text' (texto do link).
    """
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


async def fetch_entregas_pdf(ano: int) -> bytes:
    """Busca o PDF de entregas de fertilizantes para um ano.

    Navega na página de estatísticas, localiza o link do relatório
    e faz o download.

    Args:
        ano: Ano do relatório.

    Returns:
        Bytes do PDF.

    Raises:
        FileNotFoundError: Se PDF do ano não for encontrado.
    """
    html = await fetch_estatisticas_page()
    links = parse_links_from_html(html, pattern=r"\.pdf")

    # Busca link que contenha o ano no texto ou URL
    ano_str = str(ano)
    candidates = [link for link in links if ano_str in link["url"] or ano_str in link["text"]]

    # Prioriza links com "entrega" ou "fertilizante" no texto/URL
    priority = [
        link
        for link in candidates
        if re.search(r"entrega|fertiliz", f"{link['text']} {link['url']}", re.IGNORECASE)
    ]

    target = priority[0] if priority else (candidates[0] if candidates else None)

    if not target:
        raise FileNotFoundError(
            f"PDF de entregas ANDA para {ano} não encontrado. "
            f"Links disponíveis: {[link['text'] for link in links[:10]]}"
        )

    logger.info("anda_pdf_found", ano=ano, url=target["url"], text=target["text"])
    return await download_file(target["url"])
