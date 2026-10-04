from __future__ import annotations

import json
import warnings
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.anec import client
from agrobr.anec.models import ANECArticle
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    levanta_exatamente,
    make_mock_async_client,
    make_mock_response,
)


def _make_article(
    *,
    id_: int = 999,
    week: int = 5,
    year: int = 2026,
    media_updated: str = "2026-02-05T10:00:00Z",
    pdf_url: str = "https://www.anec.com.br/uploads/test.pdf",
) -> ANECArticle:
    return ANECArticle(
        id=id_,
        cuid=f"cuid-{id_}",
        title_en=f"ANEC - {week:02d}.{year} Accumulated Exports",
        slug_en=f"anec-{week:02d}{year}-accumulated-exports",
        created_at=datetime(year, 1, 1, tzinfo=UTC),
        pdf_url=pdf_url,
        media_updated_at=datetime.fromisoformat(media_updated.replace("Z", "+00:00")),
    )


class TestExtractNextData:
    def test_no_next_data_raises_parse_error(self, no_next_data_html):
        with pytest.raises(ParseError, match="__NEXT_DATA__"):
            client._extract_next_data(no_next_data_html)

    def test_malformed_json_raises_parse_error(self, malformed_json_html):
        with pytest.raises(ParseError, match="decodificando"):
            client._extract_next_data(malformed_json_html)


class TestPickPdfAttachment:
    def test_correct_attachment_accepted(self):
        art = {
            "articleMediaFiles": [
                {
                    "type": "ATTACHMENT",
                    "mediaFile": {"url": "/uploads/y.pdf", "updatedAt": "2026-01-01T00:00:00Z"},
                }
            ]
        }
        result = client._pick_pdf_attachment(art)
        assert result is not None

    def test_non_pdf_extension_skipped(self):
        art = {
            "articleMediaFiles": [
                {
                    "type": "ATTTACHMENT",
                    "mediaFile": {"url": "/uploads/x.png", "updatedAt": "2026-01-01T00:00:00Z"},
                }
            ]
        }
        assert client._pick_pdf_attachment(art) is None

    def test_missing_updated_at_skipped(self):
        art = {
            "articleMediaFiles": [{"type": "ATTTACHMENT", "mediaFile": {"url": "/uploads/x.pdf"}}]
        }
        assert client._pick_pdf_attachment(art) is None


class TestResolvePdfUrl:
    def test_absolute_url_unchanged(self):
        url = "https://www.anec.com.br/uploads/test.pdf"
        assert client._resolve_pdf_url(url) == url

    def test_relative_without_slash(self):
        assert (
            client._resolve_pdf_url("uploads/test.pdf")
            == "https://www.anec.com.br/uploads/test.pdf"
        )


class TestParseIso:
    def test_naive_assumes_utc(self):
        dt = client._parse_iso("2026-04-29T19:16:31")
        assert dt.tzinfo is not None


class TestListArticles:
    @pytest.mark.asyncio
    async def test_404_raises_source_unavailable(self):
        resp = make_mock_response(404, text="not found")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="HTTP 404"),
        ):
            await client.list_articles(2026)


@pytest.mark.usefixtures("isolated_cache")
class TestFetchPdfBytes:
    @pytest.mark.asyncio
    async def test_success_returns_bytes(self):
        article = _make_article()
        pdf_content = b"%PDF-1.7" + b"x" * 20_000
        resp = make_mock_response(200, content=pdf_content, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, url = await client.fetch_pdf_bytes(article, use_cache=False)

        assert content == pdf_content
        assert url == article.pdf_url
        assert not client._cached_pdf_path(2026, 5).exists()
        assert not client._cached_meta_path(2026, 5).exists()

    @pytest.mark.asyncio
    async def test_404_raises_source_unavailable(self):
        article = _make_article()
        resp = make_mock_response(404, content=b"", url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="HTTP 404"),
        ):
            await client.fetch_pdf_bytes(article, use_cache=False)

    @pytest.mark.asyncio
    async def test_pdf_too_small_raises(self):
        article = _make_article()
        resp = make_mock_response(200, content=b"%PDF-tiny", url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="muito pequeno"),
        ):
            await client.fetch_pdf_bytes(article, use_cache=False)

    @pytest.mark.asyncio
    async def test_html_disguised_as_pdf_raises(self):
        article = _make_article()
        html_body = b"<html><body>Site em manutencao</body></html>" + b"x" * 20_000
        resp = make_mock_response(200, content=html_body, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="não é PDF"),
        ):
            await client.fetch_pdf_bytes(article, use_cache=False)

    @pytest.mark.asyncio
    async def test_timeout_retries_then_fails(self):
        article = _make_article()
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_pdf_bytes(article, use_cache=False)

        assert mock_client.get.call_count == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pdf_url",
        [
            "http://10.0.0.5/interno.pdf",
            "http://www.anec.com.br/uploads/test.pdf",
            "https://www.anec.com.br.exemplo.net/uploads/test.pdf",
        ],
    )
    async def test_url_fora_da_origem_https_recusada_antes_do_pedido(self, pdf_url):
        article = _make_article(pdf_url=pdf_url)
        pdf_content = b"%PDF-1.7" + b"x" * 20_000
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(
            return_value=make_mock_response(200, content=pdf_content, url=pdf_url)
        )

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client) as sessao,
            levanta_exatamente(SourceUnavailableError, "fora da origem HTTPS oficial da fonte"),
        ):
            await client.fetch_pdf_bytes(article, use_cache=False)
        sessao.assert_not_called()


class TestValidateCacheKey:
    def test_cache_dir_rejects_invalid(self):
        with pytest.raises(ValueError):
            client._cache_dir(1999, 5)
        with pytest.raises(ValueError):
            client._cache_dir(2026, 54)


@pytest.mark.usefixtures("isolated_cache")
class TestSha256Mismatch:
    @pytest.mark.asyncio
    async def test_legacy_meta_without_sha_loads(self):
        article = _make_article(week=24, year=2026)
        pdf = b"%PDF-1.7" + b"x" * 20_000

        cache_dir = client._cache_dir(2026, 24)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 24).write_bytes(pdf)
        client._cached_meta_path(2026, 24).write_text(
            json.dumps(
                {
                    "cuid": article.cuid,
                    "pdf_url": article.pdf_url,
                    "media_updated_at": article.media_updated_at.isoformat(),
                    "fetched_at": article.media_updated_at.isoformat(),
                }
            ),
            encoding="utf-8",
        )

        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(
            return_value=make_mock_response(
                200, content=b"%PDF-1.7" + b"y" * 20_000, url=article.pdf_url
            )
        )
        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, _ = await client.fetch_pdf_bytes(article)

        assert content == pdf
        assert mock_client.get.call_count == 0

    @pytest.mark.asyncio
    async def test_sha_mismatch_invalidates(self):
        article = _make_article(week=21, year=2026)
        good_pdf = b"%PDF-1.7" + b"x" * 20_000

        cache_dir = client._cache_dir(2026, 21)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 21).write_bytes(good_pdf)
        client._cached_meta_path(2026, 21).write_text(
            json.dumps(
                {
                    "media_updated_at": article.media_updated_at.isoformat(),
                    "fetched_at": article.media_updated_at.isoformat(),
                    "pdf_sha256": "0" * 64,
                }
            ),
            encoding="utf-8",
        )

        new_pdf = b"%PDF-1.7" + b"y" * 20_000
        resp = make_mock_response(200, content=new_pdf, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, _ = await client.fetch_pdf_bytes(article)

        assert content == new_pdf

    @pytest.mark.asyncio
    async def test_save_includes_sha256(self):
        article = _make_article(week=23, year=2026)
        pdf = b"%PDF-1.7" + b"x" * 20_000
        client._save_cache(article, pdf)

        meta = json.loads(client._cached_meta_path(2026, 23).read_text(encoding="utf-8"))
        assert meta["pdf_sha256"] == client._sha256(pdf)


class TestListMemCache:
    @pytest.mark.asyncio
    async def test_second_call_uses_cache(self, category_2026_p1_payload, html_factory):
        from copy import deepcopy

        payload = deepcopy(category_2026_p1_payload)
        articles_in = payload["props"]["pageProps"]["paginatedArticles"]["articles"]
        payload["props"]["pageProps"]["paginatedArticles"]["total"] = len(articles_in)
        html = html_factory(payload)
        resp = make_mock_response(200, text=html)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            articles_a = await client.list_articles(2026)
            articles_b = await client.list_articles(2026)

        assert len(articles_a) == len(articles_b)
        assert mock_client.get.call_count == 1

    @pytest.mark.parametrize("bruto", ["garbage", "-5", "nan", "inf", "-inf"])
    def test_ttl_invalido_avisa_uma_vez_e_usa_o_padrao(self, monkeypatch, bruto):
        from agrobr.utils.warnings import warn_once_reset

        warn_once_reset()
        monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", bruto)
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            valores = [client._list_ttl_seconds() for _ in range(3)]
        assert valores == [300.0, 300.0, 300.0]
        assert [str(aviso.message) for aviso in avisos] == [
            f"AGROBR_ANEC_LIST_TTL inválido ({bruto!r}): use segundos, número finito maior ou "
            "igual a 0 (0 desliga o cache da listagem); usando o padrão de 300 s."
        ]

    @pytest.mark.parametrize(("bruto", "esperado"), [("0", 0.0), ("60", 60.0), ("1.5", 1.5)])
    def test_ttl_valido_nao_avisa(self, monkeypatch, bruto, esperado):
        monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", bruto)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert client._list_ttl_seconds() == esperado

    @pytest.mark.asyncio
    async def test_cache_returns_defensive_copy(self, category_2026_p1_payload, html_factory):
        from copy import deepcopy

        payload = deepcopy(category_2026_p1_payload)
        articles_in = payload["props"]["pageProps"]["paginatedArticles"]["articles"]
        payload["props"]["pageProps"]["paginatedArticles"]["total"] = len(articles_in)
        html = html_factory(payload)
        resp = make_mock_response(200, text=html)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            first = await client.list_articles(2026)
            first.clear()
            second = await client.list_articles(2026)
            second.clear()
            third = await client.list_articles(2026)

        assert len(third) == len(articles_in)
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_artigo_alterado_pelo_usuario_nao_muda_o_cache(
        self, category_2026_p1_payload, html_factory
    ):
        from copy import deepcopy

        payload = deepcopy(category_2026_p1_payload)
        articles_in = payload["props"]["pageProps"]["paginatedArticles"]["articles"]
        payload["props"]["pageProps"]["paginatedArticles"]["total"] = len(articles_in)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(
            return_value=make_mock_response(200, text=html_factory(payload))
        )

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            primeiro = await client.list_articles(2026)
            original = primeiro[0].slug_en
            primeiro[0].slug_en = "alterado"
            segundo = await client.list_articles(2026)
            segundo[0].slug_en = "alterado de novo"
            terceiro = await client.list_articles(2026)

        assert terceiro[0].slug_en == original
        assert mock_client.get.call_count == 1


class TestPaginaSemArtigos:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [{}, {"props": {"pageProps": {}}}, {"props": {"pageProps": {"paginatedArticles": {}}}}],
    )
    async def test_pagina_sem_lista_de_artigos_e_layout(self, payload, html_factory):
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(
            return_value=make_mock_response(200, text=html_factory(payload))
        )

        with (
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ParseError, match="sem paginatedArticles.articles"),
        ):
            await client.list_articles(2026)

    @pytest.mark.asyncio
    async def test_lista_de_artigos_vazia_continua_vazia(self, html_factory):
        payload = {"props": {"pageProps": {"paginatedArticles": {"articles": [], "total": 0}}}}
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(
            return_value=make_mock_response(200, text=html_factory(payload))
        )

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            assert await client.list_articles(2026) == []


class TestAnoDeBrasilia:
    def test_virada_do_ano_segue_brasilia(self, monkeypatch):
        from agrobr.anec import models
        from agrobr.exceptions import InvalidParameterError
        from agrobr.utils import time as time_utils

        monkeypatch.setattr(
            time_utils, "utcnow_aware", lambda: datetime(2027, 1, 1, 1, 0, tzinfo=UTC)
        )

        with pytest.raises(InvalidParameterError, match="entre 2026 e 2026"):
            models.validate_year(2027)

    @pytest.mark.asyncio
    async def test_ano_padrao_do_ultimo_boletim_segue_brasilia(self, monkeypatch):
        from agrobr.utils import time as time_utils

        monkeypatch.setattr(
            time_utils, "utcnow_aware", lambda: datetime(2027, 1, 1, 1, 0, tzinfo=UTC)
        )
        artigo = _make_article(week=52, year=2026)
        listagem = AsyncMock(return_value=[artigo])
        monkeypatch.setattr(client, "list_articles", listagem)
        monkeypatch.setattr(
            client,
            "_acquire_pdf",
            AsyncMock(return_value=client.Aquisicao(b"%PDF", artigo.pdf_url, False, None, {})),
        )

        await client.fetch_latest_pdf()

        listagem.assert_awaited_once_with(2026)


@pytest.mark.usefixtures("isolated_cache")
class TestConcurrentFetch:
    @pytest.mark.asyncio
    async def test_parallel_fetch_same_article_one_download(self):
        import asyncio

        article = _make_article(week=20, year=2026)
        pdf_content = b"%PDF-1.7" + b"x" * 20_000

        call_count = {"n": 0}

        async def slow_get(*_args, **_kwargs):
            call_count["n"] += 1
            await asyncio.sleep(0.05)
            return make_mock_response(200, content=pdf_content, url=article.pdf_url)

        mock_client = make_mock_async_client()
        mock_client.get = slow_get

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            results = await asyncio.gather(
                client.fetch_pdf_bytes(article),
                client.fetch_pdf_bytes(article),
                client.fetch_pdf_bytes(article),
            )

        assert all(r[0] == pdf_content for r in results)
        assert call_count["n"] == 1


class TestArticleDedup:
    @pytest.mark.asyncio
    async def test_duplicate_articles_deduped(self, category_2026_p1_payload, html_factory):
        from copy import deepcopy

        payload = deepcopy(category_2026_p1_payload)
        articles_in = payload["props"]["pageProps"]["paginatedArticles"]["articles"]
        duplicated = articles_in + deepcopy(articles_in[:3])
        payload["props"]["pageProps"]["paginatedArticles"]["articles"] = duplicated
        payload["props"]["pageProps"]["paginatedArticles"]["total"] = len(duplicated)

        html = html_factory(payload)
        resp = make_mock_response(200, text=html)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            articles = await client.list_articles(2026)

        cuids = [a.cuid for a in articles]
        assert len(cuids) == len(set(cuids))


@pytest.mark.usefixtures("isolated_cache")
class TestCacheFilesystem:
    @pytest.mark.asyncio
    async def test_stale_refetches(self):
        article = _make_article(week=7, year=2026, media_updated="2026-02-15T10:00:00Z")
        old_content = b"%PDF-old" + b"o" * 20_000
        new_content = b"%PDF-new" + b"n" * 20_000

        cache_dir = client._cache_dir(2026, 7)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 7).write_bytes(old_content)
        client._cached_meta_path(2026, 7).write_text(
            json.dumps(
                {
                    "media_updated_at": "2026-02-10T10:00:00+00:00",
                    "fetched_at": "2026-02-10T10:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

        resp = make_mock_response(200, content=new_content, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, _ = await client.fetch_pdf_bytes(article)

        assert content == new_content
        assert client._cached_pdf_path(2026, 7).read_bytes() == new_content

    @pytest.mark.asyncio
    async def test_corrupt_meta_invalidates(self):
        article = _make_article(week=8, year=2026)
        pdf_content = b"%PDF-1.7" + b"x" * 20_000

        cache_dir = client._cache_dir(2026, 8)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 8).write_bytes(b"old")
        client._cached_meta_path(2026, 8).write_text("{not json", encoding="utf-8")

        resp = make_mock_response(200, content=pdf_content, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, _ = await client.fetch_pdf_bytes(article)

        assert content == pdf_content

    @pytest.mark.asyncio
    async def test_corrupt_cached_pdf_invalidates(self):
        article = _make_article(week=10, year=2026)
        good_pdf = b"%PDF-1.7" + b"x" * 20_000

        cache_dir = client._cache_dir(2026, 10)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 10).write_bytes(b"<html>corrupted</html>" + b"x" * 20_000)
        client._cached_meta_path(2026, 10).write_text(
            json.dumps(
                {
                    "media_updated_at": article.media_updated_at.isoformat(),
                    "fetched_at": article.media_updated_at.isoformat(),
                }
            ),
            encoding="utf-8",
        )

        resp = make_mock_response(200, content=good_pdf, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            content, _ = await client.fetch_pdf_bytes(article)

        assert content == good_pdf

    @pytest.mark.asyncio
    async def test_cache_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("AGROBR_ANEC_CACHE_DISABLED", "1")
        article = _make_article(week=9, year=2026)
        pdf_content = b"%PDF-1.7" + b"x" * 20_000

        cache_dir = client._cache_dir(2026, 9)
        cache_dir.mkdir(parents=True, exist_ok=True)
        client._cached_pdf_path(2026, 9).write_bytes(pdf_content)
        client._cached_meta_path(2026, 9).write_text(
            json.dumps(
                {
                    "media_updated_at": article.media_updated_at.isoformat(),
                    "fetched_at": article.media_updated_at.isoformat(),
                }
            ),
            encoding="utf-8",
        )

        resp = make_mock_response(200, content=pdf_content, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client):
            await client.fetch_pdf_bytes(article)

        assert mock_client.get.call_count == 1


@pytest.mark.parametrize(("valor", "desligado"), [("true", True), ("YES", True), ("0", False)])
def test_cache_disabled_aceita_booleanos(monkeypatch, valor, desligado):
    monkeypatch.setenv("AGROBR_ANEC_CACHE_DISABLED", valor)
    assert client._cache_disabled() is desligado
