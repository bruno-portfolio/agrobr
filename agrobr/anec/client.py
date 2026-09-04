from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import structlog

from agrobr.anec.models import CATEGORIES_BY_YEAR, MIN_YEAR, ANECArticle
from agrobr.constants import MIN_PDF_SIZE, URLS, CacheSettings, Fonte
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

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
    response.raise_for_status()
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
    if year < MIN_YEAR:
        raise InvalidParameterError(
            f"Suporte a anos anteriores a {MIN_YEAR} não implementado (recebido: {year}). "
            f"Layout dos PDFs antigos não foi validado. "
            f"Para suportar, adicione cuid em CATEGORIES_BY_YEAR e ajuste MIN_YEAR."
        )
    if year not in CATEGORIES_BY_YEAR:
        mapped = sorted(CATEGORIES_BY_YEAR.keys())
        raise InvalidParameterError(
            f"Ano {year} não mapeado em CATEGORIES_BY_YEAR (mapeados: {mapped}). "
            "Atualize agrobr/anec/models.py::CATEGORIES_BY_YEAR com o cuid do ano."
        )

    ttl = _list_ttl_seconds()
    if ttl > 0 and year in _LIST_CACHE:
        ts, cached_articles = _LIST_CACHE[year]
        age = time.monotonic() - ts
        if age < ttl:
            logger.debug("anec_list_memcache_hit", year=year, age_s=age)
            return list(cached_articles)

    cuid = CATEGORIES_BY_YEAR[year]
    all_articles: list[ANECArticle] = []
    page = 1
    total: int | None = None

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source=Fonte.ANEC),
        follow_redirects=True,
    ) as client:
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

    logger.info("anec_list_done", year=year, count=len(deduped), total=total)
    if _list_ttl_seconds() > 0:
        _LIST_CACHE[year] = (time.monotonic(), list(deduped))
    return deduped


_FETCH_LOCKS: dict[str, asyncio.Lock] = {}
_LOCKS_GUARD = asyncio.Lock()


async def _get_fetch_lock(key: str) -> asyncio.Lock:
    async with _LOCKS_GUARD:
        lock = _FETCH_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _FETCH_LOCKS[key] = lock
        return lock


async def fetch_pdf_bytes(article: ANECArticle, *, use_cache: bool = True) -> tuple[bytes, str]:
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
                return cached, article.pdf_url

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
            response.raise_for_status()

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

            if use_cache:
                _save_cache(article, content)

            logger.info("anec_pdf_ok", url=article.pdf_url, size=len(content))
            return content, article.pdf_url


async def fetch_latest_pdf(
    year: int | None = None, *, use_cache: bool = True
) -> tuple[bytes, str, ANECArticle]:
    if year is None:
        year = datetime.now(UTC).year

    articles = await list_articles(year)
    if not articles:
        for prev_year in range(year - 1, MIN_YEAR - 1, -1):
            if prev_year in CATEGORIES_BY_YEAR:
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
    pdf_bytes, url = await fetch_pdf_bytes(latest, use_cache=use_cache)
    return pdf_bytes, url, latest


def _cache_disabled() -> bool:
    return os.environ.get("AGROBR_ANEC_CACHE_DISABLED") == "1"


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


def _load_cached(article: ANECArticle) -> bytes | None:
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
    return content


def _atomic_write_bytes(target: Path, data: bytes) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, target)


def _atomic_write_text(target: Path, text: str) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, target)


def _cleanup_tmp_files(cdir: Path) -> None:
    for tmp in cdir.glob("*.tmp"):
        try:
            tmp.unlink()
        except OSError as exc:
            logger.debug("anec_cache_tmp_cleanup_failed", path=str(tmp), error=str(exc))


def _save_cache(article: ANECArticle, pdf_bytes: bytes) -> None:
    if _cache_disabled():
        return
    try:
        week, year = article.week_year
    except ValueError:
        return

    cdir = _cache_dir(year, week)
    try:
        cdir.mkdir(parents=True, exist_ok=True)
        _cleanup_tmp_files(cdir)
        meta = {
            "article_id": article.id,
            "cuid": article.cuid,
            "title_en": article.title_en,
            "pdf_url": article.pdf_url,
            "media_updated_at": article.media_updated_at.isoformat(),
            "fetched_at": datetime.now(UTC).isoformat(),
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
