from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr.alt.anp_diesel import _catalog, api, client, models
from agrobr.exceptions import ParseError, SourceUnavailableError

FIXTURE = Path(__file__).parent / "fixtures/municipal_catalog.json"


def _html():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return "".join(f'<a href="{link["url"]}">{link["text"]}</a>' for link in payload["links"])


def test_official_municipal_catalog_projection():
    assert _catalog.parse_municipal_catalog(_html()) == models.PRECOS_MUNICIPIOS_URLS


@pytest.mark.parametrize("bounded", [False, True])
async def test_new_municipal_year_discovers_actual_link(bounded, monkeypatch):
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date(2027, 2, 1))
    url = f"{models.SHLP_BASE}/semanal/semanal-municipios-2026-2027.xlsx"
    html = _html().replace(models.PRECOS_MUNICIPIOS_URLS["2026"], url)
    catalog = _catalog.parse_municipal_catalog(html)
    fetch = AsyncMock(return_value=catalog)
    monkeypatch.setattr(client, "fetch_precos_catalog", fetch)
    query = {
        "nivel": "municipio",
        "inicio": date(2027, 1, 1),
        "fim": date(2027, 1, 31) if bounded else None,
    }
    assert await api._resolve_price_urls(query) == [url]
    fetch.assert_awaited_once()


async def test_absent_municipal_year_does_not_invent_url(monkeypatch):
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date(2027, 2, 1))
    monkeypatch.setattr(
        client, "fetch_precos_catalog", AsyncMock(return_value=models.PRECOS_MUNICIPIOS_URLS)
    )
    with pytest.raises(SourceUnavailableError, match="catálogo"):
        await api._resolve_price_urls(
            {"nivel": "municipio", "inicio": date(2027, 1, 1), "fim": None}
        )


def test_lacuna_no_catalogo_nao_e_parametro_invalido():
    catalog = {"2022": "https://example.test/2022.xlsx", "2024": "https://example.test/2024.xlsx"}
    with pytest.raises(SourceUnavailableError, match="Ano 2023"):
        api._periodos_municipios(date(2022, 1, 1), date(2024, 12, 31), catalog)


def test_overlapping_workbooks_are_rejected():
    url = f"{models.SHLP_BASE}/semanal/semanal-municipios-2025-2026.xlsx"
    with pytest.raises(ParseError, match="sobrepostas"):
        _catalog.parse_municipal_catalog(_html() + f'<a href="{url}">Municípios</a>')


@pytest.mark.parametrize("html", ["<html>Indisponível</html>", '<a href="/mensal.xlsx">Mensal</a>'])
def test_catalog_layout_failure_is_visible(html):
    with pytest.raises(ParseError, match="ausentes"):
        _catalog.parse_municipal_catalog(html)
