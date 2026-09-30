from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from agrobr import anec
from agrobr.anec import client, models, parser
from agrobr.datasets.comparacao_anual_anec import ComparacaoAnualANECDataset
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.helpers import collect_failures

FIXTURE = Path(__file__).parent / "fixtures/json/annual_categories.json"


def test_official_category_projection():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    categorias = payload["props"]["pageProps"]["homeLayoutData"]["navbarCategories"]
    categorias[0]["children"] += [
        {"cuid": "hidden-2028", "nameEN": "2028 All products", "public": False},
        {"cuid": "br-2029", "nameEN": "", "nameBR": "2029 Todos os produtos"},
    ]
    categorias.append(
        {
            "cuid": "news",
            "nameEN": "News",
            "children": [{"cuid": "news-2027", "nameEN": "2027 All products"}],
        }
    )
    assert client._parse_categories(payload) == {
        2021: "ckjlwb2hw2763049mtx4pgd003i",
        2022: "ckjlwaoon2762899mtxbzopsplz",
        2023: "cld0gqflr65339ibtx0nr26ztx",
        2024: "clr7rie04736497vatxx9dx29xo",
        2025: "cm5pn0s37134558patx544qm9vc",
        2026: models.CATEGORIES_BY_YEAR[2026],
        2029: "br-2029",
    }


def _catalogo_sem_2027(monkeypatch, category_2026_p1_payload):
    monkeypatch.setattr(
        models.time_utils, "utcnow_aware", lambda: datetime(2027, 2, 1, 12, tzinfo=UTC)
    )
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    articles = copy.deepcopy(category_2026_p1_payload)
    articles["props"]["pageProps"]["paginatedArticles"]["total"] = 1
    requests = []

    def handler(request):
        requests.append(request)
        data = payload if "category" not in request.url.params else articles
        html = (
            '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(data) + "</script>"
        )
        return httpx.Response(200, text=html)

    factory = httpx.AsyncClient
    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: factory(**kwargs, transport=httpx.MockTransport(handler)),
    )
    filhos = payload["props"]["pageProps"]["homeLayoutData"]["navbarCategories"][0]["children"]
    return filhos, requests


async def test_ano_sem_categoria_usa_cache_negativo_e_expira(monkeypatch, category_2026_p1_payload):
    monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", "60")
    now = [0.0]
    monkeypatch.setattr(client, "time", Mock(wraps=client.time, monotonic=lambda: now[0]))
    filhos, requests = _catalogo_sem_2027(monkeypatch, category_2026_p1_payload)
    assert await client.list_articles(2027) == []
    assert await client.list_articles(2027) == []
    assert len(requests) == 1
    filhos.append({"cuid": "published-2027", "nameEN": "2027 All products"})
    now[0] = 61.0
    assert await client.list_articles(2027)
    assert len(requests) == 3
    assert requests[-1].url.params["category"] == "published-2027"


async def test_ttl_zero_nao_grava_cache_negativo(monkeypatch, category_2026_p1_payload):
    monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", "0")
    filhos, requests = _catalogo_sem_2027(monkeypatch, category_2026_p1_payload)
    assert await client.list_articles(2027) == []
    filhos.append({"cuid": "published-2027", "nameEN": "2027 All products"})
    monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", "60")
    assert await client.list_articles(2027)
    assert requests[-1].url.params["category"] == "published-2027"


async def test_latest_without_year_falls_back_when_new_year_has_no_publication(
    monkeypatch, category_2026_p1_payload
):
    article = client._parse_articles(category_2026_p1_payload)[0]
    monkeypatch.setattr(
        models.time_utils, "utcnow_aware", lambda: datetime(2027, 1, 5, 12, tzinfo=UTC)
    )
    listing = AsyncMock(side_effect=[[], [article]])
    monkeypatch.setattr(client, "list_articles", listing)
    monkeypatch.setattr(
        client,
        "_acquire_pdf",
        AsyncMock(
            return_value=client.Aquisicao(b"%PDF", article.pdf_url, False, datetime(2027, 1, 5), {})
        ),
    )
    result = await client.fetch_latest_pdf()
    assert result[2] == article
    assert [call.args for call in listing.await_args_list] == [(2027,), (2026,)]


async def test_explicit_year_without_publication_never_returns_previous_edition(monkeypatch):
    monkeypatch.setattr(
        models.time_utils, "utcnow_aware", lambda: datetime(2027, 1, 5, 12, tzinfo=UTC)
    )
    golden = Path(__file__).parents[1] / "golden_data/anec/weekly_w34_2026"
    article = models.ANECArticle.model_validate_json(
        (golden / "article.json").read_text(encoding="utf-8")
    )
    listing = AsyncMock(side_effect=[[], [article]])
    download = AsyncMock()
    monkeypatch.setattr(client, "list_articles", listing)
    monkeypatch.setattr(client, "_acquire_pdf", download)
    with pytest.raises(SourceUnavailableError, match="2027"):
        await anec.comparacao_anual(ano=2027, use_cache=False)
    listing.assert_awaited_once_with(2027)
    download.assert_not_awaited()


def test_conflicting_annual_categories_fail():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    children = payload["props"]["pageProps"]["homeLayoutData"]["navbarCategories"][0]["children"]
    children.append({"cuid": "duplicate", "nameEN": "2026 All products"})
    with pytest.raises(ParseError, match="ambíguas"):
        client._parse_categories(payload)


def test_comparison_next_year_keeps_values_under_the_correct_year():
    with collect_failures() as check:
        for base_year in (2026, 2027, 2028):
            with check(base_year):
                words = [
                    {"text": text, "x0": x, "x1": x + 40, "top": y, "bottom": y + 8}
                    for text, x, y in [
                        ("Soybeans", 225, 0),
                        (str(base_year), 200, 10),
                        (f"{base_year + 1}*", 300, 10),
                        ("January", 50, 20),
                        ("100", 220, 20),
                        ("200", 320, 20),
                    ]
                ]
                frame = parser._parse_yoy_comparison([words], [0])
                row = frame.iloc[0]
                assert (row["ano_base"], row["ano_comparacao"]) == (base_year, base_year + 1)
                assert (row["valor_base_ton"], row["valor_comparacao_ton"]) == (100.0, 200.0)
                legado = frame[["valor_2025", "valor_2026"]].iloc[0]
                assert legado.isna().tolist() == [True, base_year != 2026]
                assert legado.fillna(-1).tolist() == [-1, 100.0 if base_year == 2026 else -1]
                normalized = ComparacaoAnualANECDataset()._normalize(frame)
                assert normalized["valor_base_ton"].tolist() == [100.0]
                assert normalized["valor_comparacao_ton"].tolist() == [200.0]
                assert "valor_2026" not in normalized
                assert normalized.columns.is_unique
