from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urldefrag, urljoin, urlsplit

import bs4
import httpx
import pydantic
import structlog

from agrobr import constants
from agrobr.conab import models
from agrobr.constants import MIN_HTML_PAGE_SIZE, MIN_XLSX_SIZE
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize import encoding
from agrobr.utils import io as io_utils
from agrobr.utils.warnings import warn_once

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None  # type: ignore[assignment,misc]

logger = structlog.get_logger()


async def _fetch_http(url: str) -> bytes:
    headers = UserAgentRotator.get_headers(source="conab")
    async with httpx.AsyncClient(
        timeout=get_timeout(read=60), headers=headers, follow_redirects=True
    ) as client:
        response = await retry_on_status(lambda: client.get(url), source="conab")
        responses.raise_for_status(response, source="conab")
        return response.content


def _sem_fallback(
    exc: Exception, fallback: SourceUnavailableError, url: str
) -> SourceUnavailableError:
    """A falha HTTP é a causa; a do navegador é só o motivo de o fallback não ter rodado."""
    status = f" {exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else ""
    detalhe = getattr(exc, "last_error", None) or exc
    return SourceUnavailableError(
        source="conab",
        url=url,
        last_error=(
            f"HTTP falhou ({type(exc).__name__}{status} em {url}: {detalhe}); "
            f"o fallback por navegador também falhou: {fallback.last_error}"
        ),
    )


async def fetch_boletim_page() -> str:
    url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]
    try:
        content = await _fetch_http(url)
        html, _ = encoding.decode_content(content, source="conab")
        if len(html) < MIN_HTML_PAGE_SIZE or "levantamento" not in html.lower():
            raise SourceUnavailableError(
                source="conab", url=url, last_error="Boletim sem conteúdo esperado"
            )
        return html
    except (httpx.HTTPError, SourceUnavailableError) as exc:
        logger.warning("conab_http_fallback", error=str(exc))
        try:
            return await _fetch_boletim_page_browser()
        except SourceUnavailableError as fallback:
            raise _sem_fallback(exc, fallback, url) from exc


async def _fetch_boletim_page_browser() -> str:
    url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]

    logger.debug("conab_fetch_boletim_page", url=url)
    logger.info("conab_fetch_boletim_page", source="conab")

    from agrobr.http.browser import is_available

    if not is_available():
        raise SourceUnavailableError(
            source="conab",
            url=url,
            last_error=(
                "Playwright not available for CONAB page fetch. Install with "
                "pip install agrobr[browser] and python -m playwright install chromium"
            ),
        )

    settings = constants.HTTPSettings()
    last_error: Exception | None = None

    for attempt in range(settings.max_retries):
        try:
            async with RateLimiter.acquire(constants.Fonte.CONAB), async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent=UserAgentRotator.get_random(),
                    viewport={"width": 1920, "height": 1080},
                )

                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(3000)

                html: str = await page.content()
                await browser.close()

                if len(html) < MIN_HTML_PAGE_SIZE or "levantamento" not in html.lower():
                    raise SourceUnavailableError(
                        source="conab",
                        url=url,
                        last_error=(
                            f"Response too small or missing expected content "
                            f"({len(html)} bytes, no 'levantamento' marker)"
                        ),
                    )

                logger.info(
                    "conab_fetch_boletim_success",
                    content_length=len(html),
                )

                return html

        except Exception as e:
            last_error = e
            if "executable doesn't exist" in str(e).lower():
                raise SourceUnavailableError(
                    source="conab",
                    url=url,
                    last_error=f"{e}. Install Chromium with: python -m playwright install chromium",
                ) from e
            if attempt < settings.max_retries - 1:
                delay = settings.retry_base_delay * (settings.retry_exponential_base**attempt)
                logger.warning(
                    "conab_boletim_retry", attempt=attempt + 1, error=str(e), delay=delay
                )
                await asyncio.sleep(delay)

    raise SourceUnavailableError(
        source="conab",
        url=url,
        last_error=str(last_error),
    )


async def list_levantamentos(html: str | None = None) -> list[dict[str, Any]]:
    if html is not None:
        levantamentos = _parse_levantamentos(html)
    else:
        levantamentos = await _fetch_catalog()

    levantamentos.sort(key=lambda x: (x["ano_inicio"], x["levantamento"]), reverse=True)

    logger.info(
        "conab_levantamentos_found",
        count=len(levantamentos),
    )

    return levantamentos


async def _fetch_catalog() -> list[dict[str, Any]]:
    url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]
    html = await fetch_boletim_page()
    visited = {url}
    found: dict[str, dict[str, Any]] = {}
    while True:
        entries = _parse_levantamentos(html)
        if not entries:
            raise ParseError(
                source="conab", parser_version=1, reason=f"Página de catálogo sem tabelas: {url}"
            )
        found.update((entry["url"], entry) for entry in entries)
        next_url = _next_catalog_url(html, url)
        if next_url is None:
            return list(found.values())
        if next_url in visited:
            raise ParseError(
                source="conab", parser_version=1, reason="Ciclo na paginação do catálogo"
            )
        visited.add(next_url)
        url = next_url
        content = await _fetch_http(url)
        html, _ = encoding.decode_content(content, source="conab")


def _next_catalog_url(html: str, current_url: str) -> str | None:
    soup = bs4.BeautifulSoup(html, "lxml")
    anchor = soup.select_one("a.proximo[href], a[rel~=next][href]")
    if anchor is None:
        return None
    url = urldefrag(urljoin(current_url, str(anchor["href"])))[0]
    expected = urlsplit(constants.URLS[constants.Fonte.CONAB]["boletim_graos"])
    actual = urlsplit(url)
    if (actual.scheme, actual.netloc, actual.path.rstrip("/")) != (
        expected.scheme,
        expected.netloc,
        expected.path.rstrip("/"),
    ):
        raise ParseError(
            source="conab", parser_version=1, reason="Paginação fora do catálogo CONAB"
        )
    return url


def _parse_levantamentos(html: str) -> list[dict[str, Any]]:
    base_url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"] + "/"
    soup = bs4.BeautifulSoup(html, "lxml")
    pattern = re.compile(r"/(\d+)o-levantamento-safra-(\d{4})-(\d{4}|\d{2})/", re.I)
    found: dict[str, dict[str, Any]] = {}
    for anchor in soup.select("a[href]"):
        if not re.match(r"^Tabela\b", anchor.get_text(" ", strip=True), re.I):
            continue
        url = urldefrag(urljoin(base_url, str(anchor["href"])))[0]
        parsed = urlsplit(url)
        match = pattern.search(parsed.path)
        if parsed.scheme not in ("http", "https") or match is None:
            continue
        hostname = parsed.hostname or ""
        if hostname != "www.gov.br" and not (
            hostname == "conab.gov.br" or hostname.endswith(".conab.gov.br")
        ):
            continue
        if PurePosixPath(parsed.path).suffix.lower() not in ("", ".xlsx", ".xls"):
            continue
        publication = _publication_date(anchor)
        try:
            item = models.ConabLevantamento(
                url=url,
                levantamento=int(match[1]),
                safra=f"{match[2]}/{match[3]}",
                ano_inicio=int(match[2]),
                ano_fim=int(match[3][-2:]),
                data_publicacao=publication,
            )
        except pydantic.ValidationError as exc:
            raise ParseError(source="conab", parser_version=1, reason=str(exc)) from exc
        record = item.model_dump()
        record["safra"] = f"{item.ano_inicio}/{item.ano_fim:02d}"
        found[url] = record
    return list(found.values())


def _publication_date(anchor: bs4.Tag) -> date | None:
    published = None
    for block in anchor.find_parents("div", class_="item"):
        published = block.select_one(".documentPublished .value")
        if published is not None:
            break
    if published is None:
        return None
    parts = published.get_text(" ", strip=True).split()
    if not parts:
        return None
    text = parts[0]
    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return None


async def _download_xlsx_once(url: str) -> BytesIO:
    async with RateLimiter.acquire(constants.Fonte.CONAB), async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()
            async with page.expect_download(timeout=60000) as download_info:
                safe_url = json.dumps(url)
                await page.evaluate(f"() => {{ window.location.href = {safe_url} }}")

            download = await download_info.value

            path = await download.path()
            if not path:
                raise SourceUnavailableError(
                    source="conab",
                    url=url,
                    last_error="Download path not available",
                )

            with open(path, "rb") as file:
                content = file.read()

            io_utils.validate_download(
                content,
                kinds=("xlsx", "xls"),
                source="conab",
                url=url,
                min_size=MIN_XLSX_SIZE,
            )

            logger.info(
                "conab_download_success",
                source="conab",
                size_bytes=len(content),
            )
            return BytesIO(content)

        finally:
            await browser.close()


async def download_xlsx(url: str, *, metadata: dict[str, Any] | None = None) -> BytesIO:
    try:
        content = await _fetch_http(url)
        io_utils.validate_download(
            content,
            kinds=("xlsx", "xls"),
            source="conab",
            url=url,
            min_size=MIN_XLSX_SIZE,
        )
        if metadata is not None:
            metadata["source_method"] = "httpx"
        return BytesIO(content)
    except (httpx.HTTPError, SourceUnavailableError) as exc:
        logger.warning("conab_http_download_fallback", error=str(exc))
        try:
            result = await _download_xlsx_browser(url)
        except SourceUnavailableError as fallback:
            raise _sem_fallback(exc, fallback, url) from exc
        if metadata is not None:
            metadata["source_method"] = "playwright"
        return result


async def _download_xlsx_browser(url: str) -> BytesIO:
    logger.debug("conab_download_xlsx", url=url)
    logger.info("conab_download_xlsx", source="conab")

    from agrobr.http.browser import is_available

    if not is_available():
        raise SourceUnavailableError(
            source="conab",
            url=url,
            last_error=(
                "Playwright not available for CONAB download. Install with "
                "pip install agrobr[browser] and python -m playwright install chromium"
            ),
        )

    settings = constants.HTTPSettings()
    last_error: Exception | None = None
    for attempt in range(settings.max_retries):
        try:
            return await _download_xlsx_once(url)
        except Exception as exc:
            last_error = exc
            if "executable doesn't exist" in str(exc).lower():
                raise SourceUnavailableError(
                    source="conab",
                    url=url,
                    last_error=(
                        f"{exc}. Install Chromium with: python -m playwright install chromium"
                    ),
                ) from exc
            if attempt < settings.max_retries - 1:
                delay = settings.retry_base_delay * (settings.retry_exponential_base**attempt)
                logger.warning(
                    "conab_download_retry",
                    attempt=attempt + 1,
                    error=str(exc),
                    delay=delay,
                )
                await asyncio.sleep(delay)

    logger.debug("conab_download_failed_detail", url=url)
    logger.error("conab_download_failed", source="conab", error=str(last_error))
    raise SourceUnavailableError(source="conab", url=url, last_error=str(last_error))


def _publica(levantamento: dict[str, Any], safra: str) -> bool:
    inicio = int(safra[:4])
    return int(levantamento["safra"][:4]) in (inicio, inicio + 1)


def fora_do_boletim(levantamentos: list[dict[str, Any]], safra: str) -> bool:
    return bool(levantamentos) and int(safra[:4]) <= levantamentos[0]["ano_inicio"] - 2


def edicao_da_safra(levantamentos: list[dict[str, Any]], safra: str) -> dict[str, Any] | None:
    return next((lev for lev in levantamentos if _publica(lev, safra)), None)


def _avisar_revisao(levantamentos: list[dict[str, Any]], alvo: dict[str, Any], safra: str) -> None:
    recente = next(lev for lev in levantamentos if _publica(lev, safra))
    if recente["url"] == alvo["url"]:
        return
    data = f" ({recente['data_publicacao']})" if recente.get("data_publicacao") else ""
    warn_once(
        f"conab_revisao:{safra}:{alvo['url']}",
        f"CONAB: depois do {alvo['levantamento']}º levantamento de {alvo['safra']}, a safra {safra} "
        f"foi republicada no {recente['levantamento']}º levantamento de {recente['safra']}{data}, "
        "que pode trazer números revisados; sem `levantamento`, o agrobr entrega essa publicação",
    )


async def fetch_safra_xlsx(
    safra: str | None = None,
    levantamento: int | None = None,
    *,
    da_propria_safra: bool = False,
) -> tuple[BytesIO, dict[str, Any]]:
    """Sem `levantamento`, a safra vem da publicação mais recente que a contém: a CONAB
    republica a safra anterior, revisada, nos levantamentos da safra seguinte.
    `da_propria_safra` restringe a busca às edições da própria safra."""
    safra = models.validate_selection(safra, levantamento)
    levantamentos = await list_levantamentos()

    if not levantamentos:
        raise SourceUnavailableError(
            source="conab",
            url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
            last_error="No levantamentos found",
        )

    if safra and levantamento is None and not da_propria_safra:
        filtered = [lev for lev in levantamentos if _publica(lev, safra)]
    else:
        filtered = [
            lev
            for lev in levantamentos
            if (not safra or lev["safra"] == safra)
            and (levantamento is None or lev["levantamento"] == levantamento)
        ]

    if not filtered:
        raise SourceUnavailableError(
            source="conab",
            url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
            last_error=f"No levantamento found for safra={safra}, levantamento={levantamento}",
        )

    target = filtered[0]
    if levantamento is not None:
        _avisar_revisao(levantamentos, target, safra or target["safra"])
    metadata = dict(target)
    xlsx = await download_xlsx(target["url"], metadata=metadata)

    return xlsx, metadata
