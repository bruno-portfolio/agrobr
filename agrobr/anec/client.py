from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, cast
from weakref import WeakValueDictionary

import httpx

from agrobr import _log
from agrobr.anec import models
from agrobr.anec.models import CATEGORIES_BY_YEAR, MIN_YEAR, ANECArticle
from agrobr.constants import MIN_PDF_SIZE, URLS, CacheSettings, Fonte, env_flag
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import atomic
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
_SEARCH_URL = URLS[Fonte.ANEC]["search"]

TIMEOUT = get_timeout(read=60.0)

_HTML_PARSER_VERSION = 1

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
    re.DOTALL,
)


def _extract_next_data(html: str) -> dict[str, Any]:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        raise ParseError(
            source="anec",
            parser_version=_HTML_PARSER_VERSION,
            reason="__NEXT_DATA__ script não encontrado no HTML",
            html_snippet=html[:500],
        )
    try:
        return cast(dict[str, Any], json.loads(m.group(1)))
    except json.JSONDecodeError as exc:
        raise ParseError(
            source="anec",
            parser_version=_HTML_PARSER_VERSION,
            reason=f"Erro decodificando __NEXT_DATA__: {exc}",
        ) from exc


def _resolve_pdf_url(rel_or_abs: str) -> str:
    if rel_or_abs.startswith("http"):
        return rel_or_abs
    if not rel_or_abs.startswith("/"):
        rel_or_abs = "/" + rel_or_abs
    return f"{_BASE_URL}{rel_or_abs}"


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


_ATTACHMENT_TYPES = {"ATTACHMENT", "ATTTACHMENT"}
"""ANEC backend retorna o type "ATTTACHMENT" (3 T's) em alguns artigos — typo conhecido."""


def _pick_pdf_attachment(article_json: dict[str, Any]) -> tuple[str, datetime] | None:
    media_files = article_json.get("articleMediaFiles") or []
    for mf in media_files:
        if mf.get("type") not in _ATTACHMENT_TYPES:
            continue
        media = mf.get("mediaFile") or {}
        url = media.get("url")
        if not url or not url.lower().endswith(".pdf"):
            continue
        updated_at_raw = media.get("updatedAt") or mf.get("updatedAt")
        if not updated_at_raw:
            continue
        return _resolve_pdf_url(url), _parse_iso(updated_at_raw)
    return None


def _parse_articles(payload: dict[str, Any]) -> list[ANECArticle]:
    pp = payload.get("props", {}).get("pageProps", {})
    pa = pp.get("paginatedArticles") or {}
    articles_raw = pa.get("articles") or []

    articles: list[ANECArticle] = []
    for art in articles_raw:
        pdf = _pick_pdf_attachment(art)
        if pdf is None:
            logger.debug("anec_article_sem_pdf", id=art.get("id"))
            continue
        pdf_url, media_updated_at = pdf
        try:
            articles.append(
                ANECArticle(
                    id=int(art["id"]),
                    cuid=str(art["cuid"]),
                    title_en=str(art.get("titleEN") or art.get("titleBR") or ""),
                    slug_en=str(art.get("slugEN") or art.get("slugBR") or ""),
                    created_at=_parse_iso(str(art["createdAt"])),
                    pdf_url=pdf_url,
                    media_updated_at=media_updated_at,
                )
            )
        except (KeyError, ValueError) as exc:
            logger.warning("anec_article_invalid", id=art.get("id"), error=str(exc))
            continue
    return articles


def _articles_total(payload: dict[str, Any]) -> int:
    pp = payload.get("props", {}).get("pageProps", {})
    pa = pp.get("paginatedArticles") or {}
    return int(pa.get("total") or 0)


def _raw_article_count(payload: dict[str, Any]) -> int:
    pp = payload.get("props", {}).get("pageProps", {})
    pa = pp.get("paginatedArticles") or {}
    return len(pa.get("articles") or [])


async def _fetch_html(client: httpx.AsyncClient, url: str) -> str:
    response = await retry_on_status(lambda: client.get(url), source="anec")
    if response.status_code == 404:
        raise SourceUnavailableError(source="anec", url=url, last_error="HTTP 404")
    responses.raise_for_status(response, source="anec")
    return response.text


_MAX_PAGES = 20

_LIST_CACHE: dict[int, tuple[float, list[ANECArticle]]] = {}


def _list_ttl_seconds() -> float:
    raw = os.environ.get("AGROBR_ANEC_LIST_TTL")
    if raw is None:
        return 300.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 300.0


def _list_cache_clear() -> None:
    _LIST_CACHE.clear()


def _boletim_semanal(article: ANECArticle) -> bool:
    try:
        return bool(article.week_year)
    except ValueError:
        warn_once(
            f"anec_artigo_fora_do_padrao:{article.cuid}",
            f"ANEC: artigo '{article.title_en}' fora do padrão 'ANEC - NN.AAAA' ignorado na listagem.",
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


def _parse_categories(payload: dict[str, Any]) -> dict[int, str]:
    try:
        raw = payload["props"]["pageProps"]["homeLayoutData"]["navbarCategories"]
        categories = [models.ANECCategory.model_validate(item) for item in raw]
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(
            source="anec",
            parser_version=_HTML_PARSER_VERSION,
            reason="Catálogo de categorias ANEC inválido",
        ) from exc
    annual: dict[int, str] = {}
    for category in categories:
        if not category.public or category.nameEN.strip().casefold() != "statistics":
            continue
        for child in category.children:
            if not child.public:
                continue
            match = re.fullmatch(r"(20\d{2})\s+all\s+products", child.nameEN.strip(), re.I)
            if match is None:
                match = re.fullmatch(
                    r"(20\d{2})\s+todos\s+os\s+produtos", child.nameBR.strip(), re.I
                )
            if match is None:
                continue
            year = int(match[1])
            if year in annual and annual[year] != child.cuid:
                raise ParseError(
                    source="anec",
                    parser_version=_HTML_PARSER_VERSION,
                    reason=f"Categorias anuais ambíguas para {year}",
                )
            annual[year] = child.cuid
    if not annual:
        raise ParseError(
            source="anec",
            parser_version=_HTML_PARSER_VERSION,
            reason="Categorias anuais ausentes no catálogo ANEC",
        )
    return annual


async def list_articles(year: int) -> list[ANECArticle]:
    _warn_license()
    models.validate_year(year)

    ttl = _list_ttl_seconds()
    if ttl > 0 and year in _LIST_CACHE:
        ts, cached_articles = _LIST_CACHE[year]
        age = time.monotonic() - ts
        if age < ttl:
            logger.debug("anec_list_memcache_hit", year=year, age_s=age)
            return list(cached_articles)

    all_articles: list[ANECArticle] = []
    page = 1
    total: int | None = None

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source=Fonte.ANEC),
        follow_redirects=True,
    ) as client:
        cuid = CATEGORIES_BY_YEAR.get(year)
        if cuid is None:
            catalog = _parse_categories(_extract_next_data(await _fetch_html(client, _SEARCH_URL)))
            cuid = catalog.get(year)
            if cuid is None:
                if ttl > 0:
                    _LIST_CACHE[year] = (time.monotonic(), [])
                return []
        while page <= _MAX_PAGES:
            url = f"{_SEARCH_URL}?category={cuid}&page={page}"
            logger.debug("anec_list_page", year=year, page=page, url=url)
            html = await _fetch_html(client, url)
            payload = _extract_next_data(html)
            articles = _parse_articles(payload)
            raw_count = _raw_article_count(payload)
            if total is None:
                total = _articles_total(payload)
            all_articles.extend(articles)
            if raw_count == 0 or len(all_articles) >= total:
                break
            page += 1
        else:
            logger.warning("anec_list_max_pages", year=year, max_pages=_MAX_PAGES)

    deduped = _dedupe_articles(all_articles)
    if len(deduped) != len(all_articles):
        logger.info(
            "anec_list_deduped",
            before=len(all_articles),
            after=len(deduped),
            duplicates=len(all_articles) - len(deduped),
        )

    semanais = [article for article in deduped if _boletim_semanal(article)]
    logger.info("anec_list_done", year=year, count=len(semanais), total=total)
    if _list_ttl_seconds() > 0:
        _LIST_CACHE[year] = (time.monotonic(), list(semanais))
    return semanais


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
        year = datetime.now(UTC).year

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
            url=_SEARCH_URL,
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
