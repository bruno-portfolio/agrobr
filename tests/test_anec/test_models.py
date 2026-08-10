from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agrobr.anec.models import CATEGORIES_BY_YEAR, ANECArticle, normalize_produto, resolve_produto


class TestNormalizeProduto:
    @pytest.mark.parametrize(
        "alias,canonical",
        [
            ("soja", "soybean"),
            ("Soja", "soybean"),
            ("  SOJA  ", "soybean"),
            ("farelo de soja", "soybean_meal"),
            ("milho", "maize"),
            ("trigo", "wheat"),
            ("sorgo", "sorghum"),
            ("ddgs", "ddgs"),
            ("soybean", "soybean"),
            ("corn", "maize"),
        ],
    )
    def test_known_aliases(self, alias, canonical):
        assert normalize_produto(alias) == canonical

    def test_unknown_returns_lowercase(self):
        assert normalize_produto("ProdutoDesconhecido") == "produtodesconhecido"


class TestResolveProduto:
    def test_valid_returns_canonical(self):
        assert resolve_produto("soja") == "soybean"
        assert resolve_produto("corn") == "maize"
        assert resolve_produto("trigo") == "wheat"

    def test_invalid_raises_valueerror(self):
        with pytest.raises(ValueError, match="produto desconhecido"):
            resolve_produto("banana")


class TestANECArticleValidation:
    def _base_kwargs(self):
        return {
            "id": 1,
            "cuid": "abc",
            "title_en": "ANEC - 05.2026 Accumulated Exports",
            "slug_en": "anec-052026-accumulated-exports",
            "created_at": datetime(2026, 2, 1, tzinfo=UTC),
            "pdf_url": "https://www.anec.com.br/uploads/x.pdf",
            "media_updated_at": datetime(2026, 2, 1, tzinfo=UTC),
        }

    def test_valid(self):
        a = ANECArticle(**self._base_kwargs())
        assert a.id == 1

    def test_pdf_url_must_be_absolute(self):
        kwargs = self._base_kwargs()
        kwargs["pdf_url"] = "/uploads/x.pdf"
        with pytest.raises(ValidationError, match="absoluta"):
            ANECArticle(**kwargs)

    def test_pdf_url_must_end_pdf(self):
        kwargs = self._base_kwargs()
        kwargs["pdf_url"] = "https://www.anec.com.br/uploads/x.png"
        with pytest.raises(ValidationError, match=r"\.pdf"):
            ANECArticle(**kwargs)


class TestWeekYear:
    def _make(self, title: str) -> ANECArticle:
        return ANECArticle(
            id=1,
            cuid="x",
            title_en=title,
            slug_en="x",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            pdf_url="https://www.anec.com.br/x.pdf",
            media_updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

    def test_extracts_week_year(self):
        a = self._make("ANEC - 13.2026 Accumulated Exports")
        assert a.week_year == (13, 2026)

    def test_single_digit_week(self):
        a = self._make("ANEC - 5.2026 Accumulated Exports")
        assert a.week_year == (5, 2026)

    def test_invalid_title_raises(self):
        a = self._make("ANEC Annual Report")
        with pytest.raises(ValueError, match="extrair"):
            _ = a.week_year

    def test_week_out_of_range(self):
        a = self._make("ANEC - 60.2026 Accumulated Exports")
        with pytest.raises(ValueError, match="intervalo"):
            _ = a.week_year


class TestConstants:
    def test_categories_include_2026(self):
        assert 2026 in CATEGORIES_BY_YEAR
