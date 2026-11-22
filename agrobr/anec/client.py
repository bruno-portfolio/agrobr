from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urljoin, urlsplit
from weakref import WeakValueDictionary

import httpx
from bs4 import BeautifulSoup
from lxml import etree

from agrobr import _log
from agrobr.anec import models
from agrobr.anec.models import MIN_YEAR, ANECArticle
from agrobr.constants import MIN_PDF_SIZE, URLS, CacheSettings, Fonte, env_flag
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import atomic
from agrobr.utils import io as io_utils
from agrobr.utils.time import hoje
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)


def _warn_license() -> None:
    warn_once(
        "anec_license",
        (
            "ANEC publica os dados sem termos de uso explícitos (zona_cinza). "
            "Uso comercial pode requerer autorização da associação."
        ),
        category=UserWarning,
    )


_BASE_URL = URLS[Fonte.ANEC]["base"]
_SITEMAP_URL = URLS[Fonte.ANEC]["sitemap"]

TIMEOUT = get_timeout(read=60.0)

_HTML_PARSER_VERSION = 2

_SITEMAP_NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_BOLETIM_URL_RE = re.compile(r"/biblioteca/estatisticas-semana-(\d{1,2})-(\d{4})(?:-\d+)?/?$")
_SHORTLINK_POST_RE = re.compile(r"[?&]p=(\d+)$")
_SUFIXO_DO_TITULO = " – Anec"


def _erro_de_layout(reason: str) -> ParseError:
    return ParseError(source="anec", parser_version=_HTML_PARSER_VERSION, reason=reason)


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _parse_sitemap(xml: bytes) -> list[tuple[str, datetime]]:
    leitor = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
    try:
        raiz = etree.fromstring(xml, parser=leitor)
    except etree.XMLSyntaxError as exc:
        raise _erro_de_layout("Sitemap da biblioteca ANEC não é XML válido") from exc
    entradas: list[tuple[str, datetime]] = []
    for url in raiz.iterfind("s:url", _SITEMAP_NS):
        loc = url.findtext("s:loc", namespaces=_SITEMAP_NS)
        lastmod = url.findtext("s:lastmod", namespaces=_SITEMAP_NS)
        if not loc or not lastmod:
            raise _erro_de_layout("Entrada do sitemap da biblioteca ANEC sem loc ou lastmod")
        entradas.append((loc.strip(), _parse_iso(lastmod.strip())))
    if not entradas:
        raise _erro_de_layout("Sitemap da biblioteca ANEC sem entradas")
    return entradas


def _parse_boletim(html: bytes, *, url: str, lastmod: datetime) -> ANECArticle | None:
    soup = BeautifulSoup(html, "lxml")
    titulo = soup.title.get_text(strip=True) if soup.title else ""
    shortlink = soup.find("link", rel="shortlink", href=True)
    post = _SHORTLINK_POST_RE.search(str(shortlink["href"])) if shortlink else None
    if not titulo or post is None:
        raise _erro_de_layout("Página de boletim ANEC sem título ou sem shortlink ?p=<id>")
    pdfs = list(
        dict.fromkeys(
            urljoin(url, str(link["href"]))
            for link in soup.find_all("a", href=True)
            if str(link["href"]).lower().endswith(".pdf")
        )
    )
    if not pdfs:
        logger.debug("anec_article_sem_pdf", url=url)
        return None
    if len(pdfs) > 1:
        raise _erro_de_layout(f"Página de boletim ANEC com {len(pdfs)} PDFs distintos")
    slug = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    return ANECArticle(
        id=int(post[1]),
        cuid=slug,
        title_en=titulo.removesuffix(_SUFIXO_DO_TITULO),
        slug_en=slug,
        created_at=lastmod,
        pdf_url=pdfs[0],
        media_updated_at=lastmod,
    )


async def _fetch_body(client: httpx.AsyncClient, url: str) -> bytes:
    response = await retry_on_status(lambda: client.get(url), source="anec")
    if response.status_code == 404:
        raise SourceUnavailableError(source="anec", url=url, last_error="HTTP 404")
    responses.raise_for_status(response, source="anec")
    return response.content


_LIST_CACHE: dict[int, tuple[float, list[ANECArticle]]] = {}

_LIST_TTL_PADRAO = 300.0


def _list_ttl_seconds() -> float:
    raw = os.environ.get("AGROBR_ANEC_LIST_TTL")
    if raw is None:
        return _LIST_TTL_PADRAO
    try:
        ttl = float(raw)
    except ValueError:
        ttl = math.nan
    if math.isfinite(ttl) and ttl >= 0:
        return ttl
    warn_once(
        f"anec_list_ttl_invalido:{raw}",
        f"AGROBR_ANEC_LIST_TTL inválido ({raw!r}): use segundos, número finito maior ou igual a 0 "
        f"(0 desliga o cache da listagem); usando o padrão de {_LIST_TTL_PADRAO:.0f} s.",
    )
    return _LIST_TTL_PADRAO


def _list_cache_clear() -> None:
    _LIST_CACHE.clear()


def _boletim_semanal(article: ANECArticle) -> bool:
    try:
        return bool(article.week_year)
    except ValueError:
        warn_once(
            f"anec_artigo_fora_do_padrao:{article.cuid}",
            f"ANEC: artigo '{article.title_en}' fora do padrão de boletim semanal ignorado na "
            "listagem.",
        )
        return False


def _dedupe_articles(articles: list[ANECArticle]) -> list[ANECArticle]:
    seen: set[str] = set()
    out: list[ANECArticle] = []
    for a in articles:
        if a.cuid in seen:
            continue
        seen.add(a.cuid)
        out.append(a)
    return out


async def list_articles(year: int) -> list[ANECArticle]:
    _warn_license()
    models.validate_year(year)

    ttl = _list_ttl_seconds()
    if ttl > 0 and year in _LIST_CACHE:
        ts, cached_articles = _LIST_CACHE[year]
        age = time.monotonic() - ts
        if age < ttl:
            logger.debug("anec_list_memcache_hit", year=year, age_s=age)
            return [article.model_copy() for article in cached_articles]

    all_articles: list[ANECArticle] = []
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source=Fonte.ANEC),
        follow_redirects=True,
    ) as client:
        for url, lastmod in _parse_sitemap(await _fetch_body(client, _SITEMAP_URL)):
            boletim = _BOLETIM_URL_RE.search(url)
            if boletim is None or int(boletim[2]) != year:
                continue
            io_utils.validate_download_url(url, base_url=_BASE_URL, source="anec")
            logger.debug("anec_list_boletim", year=year, url=url)
            article = _parse_boletim(await _fetch_body(client, url), url=url, lastmod=lastmod)
            if article is not None:
                all_articles.append(article)

    deduped = _dedupe_articles(all_articles)
    if len(deduped) != len(all_articles):
        logger.info(
            "anec_list_deduped",
            before=len(all_articles),
            after=len(deduped),
            duplicates=len(all_articles) - len(deduped),
        )

    semanais = [
        article for article in deduped if _boletim_semanal(article) and article.week_year[1] == year
    ]
    logger.info("anec_list_done", year=year, count=len(semanais))
    if ttl > 0:
        _LIST_CACHE[year] = (time.monotonic(), semanais)
    return [article.model_copy() for article in semanais]


_FETCH_LOCKS: WeakValueDictionary[tuple[asyncio.AbstractEventLoop, str], asyncio.Lock] = (
    WeakValueDictionary()
)
_LOCKS_GUARD = threading.Lock()


async def _get_fetch_lock(key: str) -> asyncio.Lock:
    loop_key = (asyncio.get_running_loop(), key)
    with _LOCKS_GUARD:
        lock = _FETCH_LOCKS.get(loop_key)
        if lock is None:
            lock = asyncio.Lock()
            _FETCH_LOCKS[loop_key] = lock
        return lock


class Aquisicao(NamedTuple):
    content: bytes
    url: str
    from_cache: bool
    fetched_at: datetime
    source_details: dict[str, str]


async def fetch_pdf_bytes(article: ANECArticle, *, use_cache: bool = True) -> tuple[bytes, str]:
    _warn_license()
    aquisicao = await _acquire_pdf(article, use_cache=use_cache)
    return aquisicao.content, aquisicao.url


async def _acquire_pdf(article: ANECArticle, *, use_cache: bool) -> Aquisicao:
    io_utils.validate_download_url(article.pdf_url, base_url=_BASE_URL, source="anec")
    lock_key = article.cuid
    lock = await _get_fetch_lock(lock_key)
    async with lock:
        if use_cache:
            cached = _load_cached(article)
            if cached is not None:
                logger.debug(
                    "anec_cache_hit",
                    article_id=article.id,
                    week=article.week_year[0],
                    year=article.week_year[1],
                )
                content, fetched_at = cached
                return Aquisicao(
                    content,
                    article.pdf_url,
                    True,
                    fetched_at,
                    {"media_updated_at": article.media_updated_at.isoformat()},
                )

        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            headers=UserAgentRotator.get_headers(source=Fonte.ANEC),
            follow_redirects=True,
            event_hooks={
                "request": [io_utils.download_url_hook(base_url=_BASE_URL, source="anec")]
            },
        ) as client:
            logger.debug("anec_pdf_request", url=article.pdf_url)
            response = await retry_on_status(lambda: client.get(article.pdf_url), source="anec")
            if response.status_code == 404:
                raise SourceUnavailableError(
                    source="anec", url=article.pdf_url, last_error="HTTP 404"
                )
            responses.raise_for_status(response, source="anec")

            content = response.content
            if len(content) < MIN_PDF_SIZE:
                raise SourceUnavailableError(
                    source="anec",
                    url=article.pdf_url,
                    last_error=(
                        f"PDF muito pequeno ({len(content)} bytes), esperado ≥{MIN_PDF_SIZE}"
                    ),
                )
            if not content.startswith(b"%PDF"):
                raise SourceUnavailableError(
                    source="anec",
                    url=article.pdf_url,
                    last_error=f"Resposta não é PDF (magic bytes: {content[:8]!r})",
                )

            fetched_at = datetime.now(UTC)
            if use_cache:
                _save_cache(article, content, fetched_at)

            logger.info("anec_pdf_ok", url=article.pdf_url, size=len(content))
            return Aquisicao(
                content,
                article.pdf_url,
                False,
                fetched_at,
                {"media_updated_at": article.media_updated_at.isoformat()},
            )


async def fetch_latest_pdf(
    year: int | None = None, *, use_cache: bool = True
) -> tuple[bytes, str, ANECArticle]:
    aquisicao, latest = await _acquire_latest(year, use_cache=use_cache)
    return aquisicao.content, aquisicao.url, latest


async def _acquire_latest(year: int | None, *, use_cache: bool) -> tuple[Aquisicao, ANECArticle]:
    allow_previous_year = year is None
    if year is None:
        year = hoje().year

    articles = await list_articles(year)
    if not articles and allow_previous_year:
        for prev_year in range(year - 1, MIN_YEAR - 1, -1):
            logger.warning("anec_year_empty_fallback", year=year, fallback=prev_year)
            articles = await list_articles(prev_year)
            if articles:
                break

    if not articles:
        raise SourceUnavailableError(
            source="anec",
            url=_SITEMAP_URL,
            last_error=f"Nenhum artigo disponível para ano {year}",
        )

    latest = max(articles, key=lambda a: a.created_at)
    return await _acquire_pdf(latest, use_cache=use_cache), latest


def _cache_disabled() -> bool:
    return env_flag("AGROBR_ANEC_CACHE_DISABLED")


def _validate_cache_key(year: int, week: int) -> None:
    if not isinstance(year, int) or not 2000 <= year <= 2100:
        raise ValueError(f"year inválido para cache: {year!r} (esperado int 2000-2100)")
    if not isinstance(week, int) or not 1 <= week <= 53:
        raise ValueError(f"week inválido para cache: {week!r} (esperado int 1-53)")


def _cache_dir(year: int, week: int) -> Path:
    _validate_cache_key(year, week)
    settings = CacheSettings()
    return settings.cache_dir / "anec" / str(year) / f"week_{week:02d}"


def _cached_pdf_path(year: int, week: int) -> Path:
    return _cache_dir(year, week) / "shipment.pdf"


def _cached_meta_path(year: int, week: int) -> Path:
    return _cache_dir(year, week) / "meta.json"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_cached(article: ANECArticle) -> tuple[bytes, datetime] | None:
    if _cache_disabled():
        return None
    try:
        week, year = article.week_year
    except ValueError:
        return None

    pdf_path = _cached_pdf_path(year, week)
    meta_path = _cached_meta_path(year, week)
    if not pdf_path.exists() or not meta_path.exists():
        return None

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        cached_updated = _parse_iso(str(meta["media_updated_at"]))
        fetched_at = _parse_iso(str(meta["fetched_at"]))
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning("anec_cache_meta_invalid", path=str(meta_path), error=str(exc))
        return None

    if meta.get("pdf_url") != article.pdf_url or meta.get("cuid", article.cuid) != article.cuid:
        logger.debug(
            "anec_cache_outro_artigo",
            cached_url=meta.get("pdf_url"),
            remote_url=article.pdf_url,
            cached_cuid=meta.get("cuid"),
            remote_cuid=article.cuid,
        )
        return None

    if cached_updated < article.media_updated_at:
        logger.debug(
            "anec_cache_stale",
            cached=cached_updated.isoformat(),
            remote=article.media_updated_at.isoformat(),
        )
        return None

    try:
        content = pdf_path.read_bytes()
    except OSError as exc:
        logger.warning("anec_cache_read_failed", path=str(pdf_path), error=str(exc))
        return None
    if not content.startswith(b"%PDF"):
        logger.warning("anec_cache_pdf_invalid", path=str(pdf_path), size=len(content))
        return None

    expected_sha = meta.get("pdf_sha256")
    if expected_sha:
        actual_sha = _sha256(content)
        if actual_sha != expected_sha:
            logger.warning(
                "anec_cache_sha_mismatch",
                path=str(pdf_path),
                expected=expected_sha,
                actual=actual_sha,
            )
            return None
    return content, fetched_at


def _atomic_write_bytes(target: Path, data: bytes) -> None:
    with atomic.atomic_output(target) as tmp:
        tmp.write_bytes(data)


def _atomic_write_text(target: Path, text: str) -> None:
    with atomic.atomic_output(target) as tmp:
        tmp.write_text(text, encoding="utf-8")


def _save_cache(article: ANECArticle, pdf_bytes: bytes, fetched_at: datetime | None = None) -> None:
    if _cache_disabled():
        return
    try:
        week, year = article.week_year
    except ValueError:
        return

    cdir = _cache_dir(year, week)
    try:
        cdir.mkdir(parents=True, exist_ok=True)
        meta = {
            "article_id": article.id,
            "cuid": article.cuid,
            "title_en": article.title_en,
            "pdf_url": article.pdf_url,
            "media_updated_at": article.media_updated_at.isoformat(),
            "fetched_at": (fetched_at or datetime.now(UTC)).isoformat(),
            "size_bytes": len(pdf_bytes),
            "pdf_sha256": _sha256(pdf_bytes),
        }
        _atomic_write_bytes(_cached_pdf_path(year, week), pdf_bytes)
        _atomic_write_text(
            _cached_meta_path(year, week),
            json.dumps(meta, ensure_ascii=False, indent=2),
        )
    except OSError as exc:
        logger.warning("anec_cache_write_failed", path=str(cdir), error=str(exc))
