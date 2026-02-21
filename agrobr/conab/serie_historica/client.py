from __future__ import annotations

from io import BytesIO
from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.CONAB]["base"]

SERIES_HISTORICAS_URL = (
    f"{BASE_URL}/conab/pt-br/atuacao/informacoes-agropecuarias/safras/series-historicas"
)

_PRODUCT_REGISTRY: dict[str, tuple[str, str, str]] = {
    "soja": ("graos", "soja", "sojaseriehist.xls"),
    "milho": ("graos", "milho", "milhototalseriehist.xls"),
    "milho_1": ("graos", "milho", "milho1aseriehist.xls"),
    "milho_2": ("graos", "milho", "milho2aseriehist.xls"),
    "milho_3": ("graos", "milho", "milho3aseriehist.xls"),
    "arroz": ("graos", "arroz", "arroztotalseriehist.xls"),
    "arroz_irrigado": ("graos", "arroz", "arrozirrigadoseriehist.xls"),
    "arroz_sequeiro": ("graos", "arroz", "arrozsequeiroseriehist.xls"),
    "feijao": ("graos", "feijao", "feijaototalseriehist.xls"),
    "feijao_1": ("graos", "feijao", "feijao1aseriehist.xls"),
    "feijao_2": ("graos", "feijao", "feijao2aseriehist.xls"),
    "feijao_3": ("graos", "feijao", "feijao3aseriehist.xls"),
    "algodao": ("graos", "algodao", "algodaoseriehist.xls"),
    "trigo": ("graos", "trigo", "trigoseriehist.xls"),
    "sorgo": ("graos", "sorgo", "sorgoseriehist.xls"),
    "aveia": ("graos", "aveia", "aveiaseriehist.xls"),
    "cevada": ("graos", "cevada", "cevadaseriehist.xls"),
    "canola": ("graos", "canola", "canolaseriehist.xls"),
    "girassol": ("graos", "girassol", "girassolseriehist.xls"),
    "mamona": ("graos", "mamona", "mamonaseriehist.xls"),
    "amendoim": ("graos", "amendoim", "amendoimtotalseriehist.xls"),
    "amendoim_1": ("graos", "amendoim", "amendoim1aseriehist.xls"),
    "amendoim_2": ("graos", "amendoim", "amendoim2aseriehist.xls"),
    "centeio": ("graos", "centeio", "centeioseriehist.xls"),
    "triticale": ("graos", "triticale", "triticaleseriehist.xls"),
    "gergelim": ("graos", "girassol", "gergelimseriehist.xls"),
    "cafe": ("cafe", "total-arabica-e-conilon", "cafetotalseriehist.xls"),
    "cafe_arabica": ("cafe", "arabica", "cafearabicaseriehist.xls"),
    "cafe_conilon": ("cafe", "conilon", "cafeconilonseriehist.xls"),
    "cana": ("cana-de-acucar", "agricola", "canaseriehist-agricola.xls"),
    "cana_area_total": ("cana-de-acucar", "area-total", "canaseriehist-area-total.xls"),
    "cana_industria": ("cana-de-acucar", "industria", "canaseriehist-industria.xls"),
}

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=_settings.timeout_read,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

ACCEPT_EXCEL = (
    "application/vnd.ms-excel,"
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
    "*/*;q=0.8"
)


def get_xls_url(produto: str) -> str:
    produto_lower = produto.lower().strip()

    if produto_lower not in _PRODUCT_REGISTRY:
        available = sorted(_PRODUCT_REGISTRY.keys())
        raise SourceUnavailableError(
            source="conab_serie_historica",
            url=SERIES_HISTORICAS_URL,
            last_error=(f"Produto '{produto}' nao encontrado. Disponiveis: {', '.join(available)}"),
        )

    categoria, subcategoria, filename = _PRODUCT_REGISTRY[produto_lower]
    return f"{SERIES_HISTORICAS_URL}/{categoria}/{subcategoria}/{filename}/view"


def list_produtos() -> list[dict[str, str]]:
    result = []
    for prod in sorted(_PRODUCT_REGISTRY.keys()):
        categoria, _, _ = _PRODUCT_REGISTRY[prod]
        result.append(
            {
                "produto": prod,
                "categoria": categoria,
                "url": get_xls_url(prod),
            }
        )
    return result


async def download_xls(produto: str) -> tuple[BytesIO, dict[str, Any]]:
    from agrobr.http.retry import retry_on_status

    url = get_xls_url(produto)
    logger.info("conab_serie_historica_download", produto=produto, url=url)

    headers = UserAgentRotator.get_headers(source="conab_serie")
    headers["Accept"] = ACCEPT_EXCEL

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
        try:
            response = await retry_on_status(
                lambda: client.get(url),
                source="conab_serie",
            )
            response.raise_for_status()
            content = response.content

            logger.info(
                "conab_serie_historica_download_ok",
                produto=produto,
                url=url,
                size_bytes=len(content),
            )

            categoria, _, _ = _PRODUCT_REGISTRY.get(produto.lower().strip(), ("unknown", "", ""))
            metadata: dict[str, Any] = {
                "url": str(response.url),
                "produto": produto,
                "categoria": categoria,
                "size_bytes": len(content),
                "content_type": response.headers.get("content-type", ""),
            }

            return BytesIO(content), metadata

        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                source="conab_serie_historica",
                url=url,
                last_error=str(e),
            ) from e


async def fetch_series_page(categoria: str = "graos") -> str:
    url = f"{SERIES_HISTORICAS_URL}/{categoria}"

    headers = UserAgentRotator.get_headers(source="conab_serie")
    headers["Accept"] = "text/html,*/*;q=0.8"

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=headers,
        follow_redirects=True,
    ) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            logger.info(
                "conab_serie_historica_page_ok",
                categoria=categoria,
                content_length=len(response.text),
            )
            return response.text
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                source="conab_serie_historica",
                url=url,
                last_error=str(e),
            ) from e


def parse_xls_links_from_html(html: str) -> list[dict[str, str]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    links: list[dict[str, str]] = []
    seen: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag["href"])
        if ".xls" not in href.lower():
            continue

        if href in seen:
            continue
        seen.add(href)

        titulo = a_tag.get_text(strip=True)
        if not titulo:
            titulo = href.split("/")[-1].replace(".xls", "")

        full_url = href
        if full_url.startswith("/"):
            full_url = f"{BASE_URL}{full_url}"

        links.append({"url": full_url, "titulo": titulo})

    logger.info("conab_serie_historica_links_parsed", count=len(links))
    return links
