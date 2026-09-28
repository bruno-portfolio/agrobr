from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agrobr.anec.models import ANECArticle
from tests.helpers import collect_failures


def _article(**overrides: str) -> ANECArticle:
    return ANECArticle(
        **{
            "id": 1,
            "cuid": "abc",
            "title_en": "ANEC - 05.2026 Accumulated Exports",
            "slug_en": "anec-052026-accumulated-exports",
            "created_at": datetime(2026, 2, 1, tzinfo=UTC),
            "pdf_url": "https://www.anec.com.br/uploads/x.pdf",
            "media_updated_at": datetime(2026, 2, 1, tzinfo=UTC),
        }
        | overrides
    )


def test_artigo_recusa_url_e_titulo_invalidos():
    with collect_failures() as check:
        for url, mensagem in [
            ("/uploads/x.pdf", "absoluta"),
            ("https://www.anec.com.br/uploads/x.png", r"\.pdf"),
        ]:
            with check(url), pytest.raises(ValidationError, match=mensagem):
                _article(pdf_url=url)
        for titulo, mensagem in [
            ("ANEC Annual Report", "extrair"),
            ("ANEC - 60.2026 Accumulated Exports", "intervalo"),
        ]:
            with check(titulo), pytest.raises(ValueError, match=mensagem):
                _ = _article(title_en=titulo).week_year
