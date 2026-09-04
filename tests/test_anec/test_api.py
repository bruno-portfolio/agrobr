from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.anec import api, parser
from agrobr.anec.models import ANECArticle
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.utils.warnings import warn_once_reset

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "anec"


@pytest.fixture
def w04_pdf_bytes() -> bytes:
    return (GOLDEN_DIR / "weekly_w04_2026" / "response.pdf").read_bytes()


@pytest.fixture
def w04_article() -> ANECArticle:
    return ANECArticle(
        id=999,
        cuid="cuid-w04",
        title_en="ANEC - 04.2026 Accumulated Exports",
        slug_en="anec-042026-accumulated-exports",
        created_at=datetime(2026, 2, 5, tzinfo=UTC),
        pdf_url="https://www.anec.com.br/uploads/test-w04.pdf",
        media_updated_at=datetime(2026, 2, 5, tzinfo=UTC),
    )


@pytest.fixture(autouse=True)
def _reset_license_warning():
    warn_once_reset("anec_license")
    yield
    warn_once_reset("anec_license")


def _patch_fetch_latest(pdf_bytes: bytes, article: ANECArticle):
    return patch.object(
        api.client,
        "fetch_latest_pdf",
        new_callable=AsyncMock,
        return_value=(pdf_bytes, article.pdf_url, article),
    )


def _patch_fetch_specific(pdf_bytes: bytes, article: ANECArticle):
    return (
        patch.object(
            api.client,
            "list_articles",
            new_callable=AsyncMock,
            return_value=[article],
        ),
        patch.object(
            api.client,
            "fetch_pdf_bytes",
            new_callable=AsyncMock,
            return_value=(pdf_bytes, article.pdf_url),
        ),
    )


class TestEmbarques:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026)
        assert len(df) == 228
        assert set(df.columns) == {"porto", "produto", "periodo", "valor_ton"}

    @pytest.mark.asyncio
    async def test_specific_week(self, w04_pdf_bytes, w04_article):
        list_p, fetch_p = _patch_fetch_specific(w04_pdf_bytes, w04_article)
        with list_p, fetch_p:
            df = await api.embarques(ano=2026, semana=4)
        assert len(df) == 228

    @pytest.mark.asyncio
    async def test_filter_porto(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, porto="SANTOS")
        assert df["porto"].nunique() == 1
        assert df["porto"].iloc[0] == "SANTOS"

    @pytest.mark.asyncio
    async def test_filter_porto_sem_acento(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, porto="paranagua")
        assert df["porto"].nunique() == 1
        assert df["porto"].iloc[0] == "PARANAGUÁ"

    @pytest.mark.asyncio
    async def test_filter_porto_lowercase_with_accent(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, porto="vitória")
        assert df["porto"].nunique() == 1
        assert df["porto"].iloc[0] == "VITÓRIA"

    @pytest.mark.asyncio
    async def test_filter_produto_pt_alias(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, produto="soja")
        assert df["produto"].nunique() == 1
        assert df["produto"].iloc[0] == "soybean"

    @pytest.mark.asyncio
    async def test_filter_tipo_efetivado(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, tipo="efetivado")
        assert df["periodo"].nunique() == 1
        assert df["periodo"].iloc[0] == "last_week"

    @pytest.mark.asyncio
    async def test_filter_tipo_programado(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, tipo="programado")
        assert df["periodo"].nunique() == 1
        assert df["periodo"].iloc[0] == "current_week"

    @pytest.mark.asyncio
    async def test_invalid_tipo_raises(self, w04_pdf_bytes, w04_article):
        with (
            _patch_fetch_latest(w04_pdf_bytes, w04_article),
            pytest.raises(ValueError, match="tipo inválido"),
        ):
            await api.embarques(ano=2026, tipo="invalido")

    @pytest.mark.asyncio
    async def test_return_meta(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df, meta = await api.embarques(ano=2026, return_meta=True)
        assert isinstance(df, pd.DataFrame)
        assert meta.source == "anec"
        assert meta.parser_version == parser.PARSER_VERSION
        assert meta.records_count == 228
        assert meta.raw_content_hash == "730ae402f5f92337"

    @pytest.mark.asyncio
    async def test_emits_license_warning(self, w04_pdf_bytes, w04_article):
        with (
            _patch_fetch_latest(w04_pdf_bytes, w04_article),
            pytest.warns(UserWarning, match="zona_cinza"),
        ):
            await api.embarques(ano=2026)

    @pytest.mark.asyncio
    async def test_year_too_old(self):
        with pytest.raises(InvalidParameterError, match="2026"):
            await api.embarques(ano=2025)

    @pytest.mark.asyncio
    async def test_specific_week_unavailable(self, w04_pdf_bytes, w04_article):
        list_p, fetch_p = _patch_fetch_specific(w04_pdf_bytes, w04_article)
        with (
            list_p,
            fetch_p,
            pytest.raises(SourceUnavailableError, match="Semana"),
        ):
            await api.embarques(ano=2026, semana=99)


class TestEmbarquesMensais:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques_mensais(ano=2026)
        assert len(df) == 72
        assert set(df.columns) == {"ano", "mes", "produto", "valor_ton", "eh_estimativa"}

    @pytest.mark.asyncio
    async def test_filter_produto(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques_mensais(ano=2026, produto="soybean")
        assert df["produto"].nunique() == 1


class TestComparacaoAnual:
    @pytest.mark.asyncio
    async def test_returns_yoy_dataframe(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.comparacao_anual(ano=2026)
        assert "valor_2025" in df.columns
        assert "valor_2026" in df.columns

    @pytest.mark.asyncio
    async def test_filter_produto(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.comparacao_anual(ano=2026, produto="maize")
        assert df["produto"].nunique() == 1
        assert df["produto"].iloc[0] == "maize"


class TestDestinos:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.destinos(ano=2026)
        assert set(df.columns) == {"produto", "destino", "share_pct"}
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_filter_produto(self, w04_pdf_bytes, w04_article):
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.destinos(ano=2026, produto="soybean")
        assert df["produto"].nunique() == 1


class TestArticlesDisponiveis:
    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(self, w04_article):
        with patch.object(
            api.client,
            "list_articles",
            new_callable=AsyncMock,
            return_value=[w04_article],
        ):
            result = await api.articles_disponiveis(2026)
        assert len(result) == 1
        item = result[0]
        assert item["id"] == 999
        assert item["week"] == 4
        assert item["year"] == 2026
        assert "pdf_url" in item


class TestAsPolars:
    @pytest.mark.asyncio
    async def test_returns_polars_dataframe(self, w04_pdf_bytes, w04_article):
        polars = pytest.importorskip("polars")
        with _patch_fetch_latest(w04_pdf_bytes, w04_article):
            df = await api.embarques(ano=2026, as_polars=True)
        assert isinstance(df, polars.DataFrame)


class TestEndToEndWithRealParser:
    @pytest.mark.asyncio
    async def test_real_parser_with_http_mock(self, w04_pdf_bytes):
        from tests.helpers import make_mock_async_client, make_mock_response

        article = ANECArticle(
            id=1,
            cuid="cuid-realparse",
            title_en="ANEC - 04.2026 Accumulated Exports",
            slug_en="anec-042026-accumulated-exports",
            created_at=datetime(2026, 2, 5, tzinfo=UTC),
            pdf_url="https://www.anec.com.br/uploads/realparse.pdf",
            media_updated_at=datetime(2026, 2, 5, tzinfo=UTC),
        )
        resp_pdf = make_mock_response(200, content=w04_pdf_bytes, url=article.pdf_url)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_pdf)

        with (
            patch.object(
                api.client, "list_articles", new_callable=AsyncMock, return_value=[article]
            ),
            patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
        ):
            df, meta = await api.embarques(ano=2026, semana=4, return_meta=True, use_cache=False)

        assert len(df) == 228
        assert df["porto"].nunique() == 19
        assert meta.parser_version == parser.PARSER_VERSION
        assert meta.records_count == 228
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_articles_disponiveis_year_too_old_raises(self):
        with pytest.raises(InvalidParameterError, match="2026"):
            await api.articles_disponiveis(2025)
