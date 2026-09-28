from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.anec import api, client, parser
from agrobr.anec.models import ANECArticle
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, conferir_corpo

_APIS = ["embarques_mensais", "comparacao_anual", "destinos"]
_INVALIDOS = [
    {"ano": True},
    {"ano": 2026.0},
    {"ano": "2026"},
    {"ano": 2025},
    {"ano": 2027},
    {"ano": 2026, "semana": True},
    {"ano": 2026, "semana": 1.0},
    {"ano": 2026, "semana": 0},
    {"ano": 2026, "semana": 54},
    {"ano": 2026, "produto": "cafe"},
    {"ano": 2026, "produto": ""},
    {"ano": 2026, "produto": 123},
]


@pytest.fixture
def additional_article() -> ANECArticle:
    return ANECArticle(
        id=812,
        cuid="edition-week-12",
        title_en="ANEC - 12.2026 Accumulated Exports",
        slug_en="anec-122026",
        created_at=datetime.fromisoformat("2026-03-24T09:00:00-03:00"),
        pdf_url="https://www.anec.com.br/uploads/edition-week-12.pdf",
        media_updated_at=datetime.fromisoformat("2026-03-25T10:30:00-03:00"),
    )


@pytest.fixture
def additional_report() -> parser.ParsedReport:
    monthly = pd.DataFrame(
        {
            "ano": pd.Series([2025, 2026, 2026], dtype="Int64"),
            "mes": pd.Series([12, 3, 3], dtype="Int64"),
            "produto": ["soybean", "soybean", "soybean_meal"],
            "valor_ton": pd.Series([100.0, pd.NA, 0.0], dtype="Float64"),
            "valor_min_ton": pd.Series([pd.NA, 200.0, pd.NA], dtype="Float64"),
            "valor_max_ton": pd.Series([pd.NA, 300.0, pd.NA], dtype="Float64"),
            "eh_estimativa": [False, True, True],
        }
    )
    comparison = pd.DataFrame(
        {
            "mes": pd.Series([3] * 3, dtype="Int64"),
            "produto": ["soybean", "soybean_meal", "total_products"],
            "valor_2025": pd.Series([100.0, 0.0, 100.0], dtype="Float64"),
            "valor_2026": pd.Series([pd.NA, 0.0, pd.NA], dtype="Float64"),
            "ano_base": pd.Series([2025] * 3, dtype="Int64"),
            "ano_comparacao": pd.Series([2026] * 3, dtype="Int64"),
            "eh_estimativa": [True] * 3,
        }
    )
    destinations = pd.DataFrame(
        {
            "produto": ["soybean", "soybean_meal"],
            "destino": ["CHINA", "OTHERS"],
            "share_pct": pd.Series([75.0, 100.0], dtype="Float64"),
            "ano": pd.Series([2025, pd.NA], dtype="Int64"),
            "mes_inicio": pd.Series([1, pd.NA], dtype="Int64"),
            "mes_fim": pd.Series([12, pd.NA], dtype="Int64"),
        }
    )
    return parser.ParsedReport(pd.DataFrame(), monthly, comparison, destinations, "abc123")


async def test_parametros_invalidos_recusados_antes_da_rede(additional_report, additional_article):
    casos = [(name, kwargs, None) for name in _APIS for kwargs in _INVALIDOS]
    casos += [(name, {"ano": 2026, "produto": "total_products"}, None) for name in _APIS[::2]]
    casos += [
        ("destinos", {"ano": 2026, "produto": produto}, "tabela de destinos")
        for produto in ("ddgs", "sorgo", "sorghum", " DDGS ")
    ]
    with collect_failures() as check:
        for name, kwargs, mensagem in casos:
            with check(f"{name}{kwargs}"):
                with (
                    patch.object(
                        api,
                        "_fetch_and_parse",
                        new_callable=AsyncMock,
                        return_value=(
                            additional_report,
                            client.Aquisicao(
                                b"%PDF-1.7",
                                additional_article.pdf_url,
                                False,
                                datetime(2026, 3, 25, tzinfo=UTC),
                                {},
                            ),
                            additional_article,
                        ),
                    ) as fetch,
                    pytest.raises(InvalidParameterError, match=mensagem) as error,
                ):
                    await getattr(api, name)(**kwargs)
                fetch.assert_not_awaited()
                if mensagem:
                    assert repr(kwargs["produto"]) in str(error.value)
                    assert "['soybean', 'soybean_meal', 'maize', 'wheat']" in str(error.value)


async def test_additional_edition_provenance_preserved(additional_report, additional_article):
    with patch.object(
        api,
        "_fetch_and_parse",
        new_callable=AsyncMock,
        return_value=(
            additional_report,
            client.Aquisicao(
                b"%PDF-1.7",
                additional_article.pdf_url,
                False,
                datetime(2026, 3, 25, tzinfo=UTC),
                {},
            ),
            additional_article,
        ),
    ) as fetch:
        frame, meta = await api.comparacao_anual(ano=2026, use_cache=False, return_meta=True)
    fetch.assert_awaited_once_with(ano=2026, semana=None, use_cache=False)
    assert set(frame["ano_relatorio"]) == {2026}
    assert set(frame["semana_relatorio"]) == {12}
    assert set(frame["edicao_id"]) == {"edition-week-12"}
    assert (frame["publicado_em"] == pd.Timestamp("2026-03-24T12:00:00Z")).all()
    assert (frame["revisado_em"] == pd.Timestamp("2026-03-25T13:30:00Z")).all()
    assert str(frame["publicado_em"].dt.tz) == "UTC"
    assert str(frame["revisado_em"].dt.tz) == "UTC"
    assert meta.source_url == additional_article.pdf_url
    conferir_corpo(meta, b"%PDF-1.7")
    assert meta.source_details.get("layout_fingerprint") == "abc123"
    assert meta.schema_version == "1.2"
    assert meta.records_count == len(frame)
    assert meta.selected_source == "anec"
    assert "edicao_id" not in additional_report.yoy_comparison


async def test_additional_empty_filter_keeps_typed_schema(additional_report, additional_article):
    with patch.object(
        api,
        "_fetch_and_parse",
        new_callable=AsyncMock,
        return_value=(
            additional_report,
            client.Aquisicao(
                b"%PDF-1.7",
                additional_article.pdf_url,
                False,
                datetime(2026, 3, 25, tzinfo=UTC),
                {},
            ),
            additional_article,
        ),
    ):
        full = await api.destinos(ano=2026)
        empty, meta = await api.destinos(ano=2026, produto="milho", return_meta=True)
    assert empty.empty
    assert empty.dtypes.to_dict() == full.dtypes.to_dict()
    assert meta.records_count == 0
    assert str(empty["publicado_em"].dt.tz) == "UTC"
    assert not meta.validation_warnings
