from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from agrobr.cepea import client as cepea_client
from scripts import update_golden, update_landing_data

FIXTURES = Path(__file__).parent / "golden_data"


async def test_ticker_official_cotton_prices_are_rendered_in_reais_per_pound(monkeypatch):
    async def fetch_page(produto: str, **_kwargs):
        filename = {"boi": "boi-gordo"}.get(produto, produto)
        html = (FIXTURES / "cepea/pages_20260905" / f"{filename}.html").read_text(encoding="utf-8")
        return cepea_client.FetchResult(html=html, source="cepea")

    monkeypatch.setattr(cepea_client, "fetch_indicador_page", fetch_page)
    ticker, _ = await update_landing_data.coletar()
    cotton = next(row for row in ticker if row["key"] == "algodao")
    assert cotton["valor"] == pytest.approx(439.98)
    assert cotton["unidade"] == "cBRL/lb"
    pt = update_landing_data.render_ticker(ticker, update_landing_data.LOCALES["pt"])
    en = update_landing_data.render_ticker(ticker, update_landing_data.LOCALES["en"])
    assert "R$ 4,40 / lb" in pt
    assert "R$ 4.40 / lb" in en
    assert "R$ 439,98" not in pt
    assert "R$ 439.98" not in en


@pytest.mark.parametrize(
    ("unidade", "valor", "expected"),
    [("BRL/sc60kg", 159.76, "R$ 159,76 / sc 60kg"), ("BRL/ton", 1452.55, "R$ 1.452,55 / ton")],
)
def test_ticker_preserves_reais_and_physical_unit(unidade, valor, expected):
    assert (
        update_landing_data.fmt_preco(valor, unidade, update_landing_data.LOCALES["pt"]) == expected
    )


def test_ticker_rejects_unknown_unit_before_publishing():
    with pytest.raises(ValueError, match="Unidade"):
        update_landing_data.fmt_preco(10, "desconhecida", update_landing_data.LOCALES["pt"])


async def test_ibge_golden_capture_preserves_first_official_row_without_sidrapy(
    monkeypatch, tmp_path
):
    raw = pd.read_csv(FIXTURES / "ibge/pam_soja_sample/response.csv", dtype=str)
    fetch = AsyncMock(return_value=raw.copy())
    monkeypatch.setattr(update_golden.ibge_client, "fetch_sidra", fetch)
    monkeypatch.setattr(update_golden, "GOLDEN_DIR", tmp_path)

    await update_golden.capture_ibge()

    saved = pd.read_csv(tmp_path / "ibge/pam_soja_sample/response.csv", dtype=str)
    pd.testing.assert_frame_equal(saved, raw)
    fetch.assert_awaited_once()
    assert fetch.call_args.kwargs["classifications"] == {"782": "40124"}


@pytest.mark.parametrize("language", ["pt", "en"])
def test_landing_regeneration_preserves_layout_and_renders_real_sample_length(language):
    loc = update_landing_data.LOCALES[language]
    html = Path(loc["index"]).read_text(encoding="utf-8")
    dates = list(pd.date_range("2026-08-01", periods=12))
    data = {"valores": list(range(150, 162)), "datas": dates, "unidade": "BRL/sc60kg"}
    ticker = [
        {"key": "soja", "data": dates[-1], "valor": 161, "unidade": "BRL/sc60kg", "var_pct": 0.625}
    ]
    result = update_landing_data.render_page(html, loc, ticker, data)
    original, updated = html, result
    for anchor in ("ticker", "proof"):
        original = update_landing_data.substituir(original, anchor, "")
        updated = update_landing_data.substituir(updated, anchor, "")
    assert original == updated
    page = BeautifulSoup(result, "html.parser")
    assert page.select_one("time.output-date")["datetime"] == "2026-08-12"
    assert len(page.select(".proof-table tbody tr")) == 4
    assert len(page.select_one(".spark-path")["points"].split()) == 12
    assert "12" in page.select_one(".chart-caption").text
    assert "/" in page.select_one(".tk-price").text


@pytest.mark.parametrize("broken", ["", "<!-- agrobr:ticker --><!-- /agrobr:ticker -->" * 2])
def test_landing_invalid_second_page_leaves_both_files_unchanged(broken, tmp_path, monkeypatch):
    complete = "<!-- agrobr:ticker -->original<!-- /agrobr:ticker --><!-- agrobr:proof -->original<!-- /agrobr:proof -->"
    first, second = tmp_path / "index.html", tmp_path / "en/index.html"
    second.parent.mkdir()
    first.write_text(complete, encoding="utf-8")
    second.write_text(broken, encoding="utf-8")
    fetch = AsyncMock()
    monkeypatch.setattr(update_landing_data, "coletar", fetch)
    monkeypatch.setattr(sys, "argv", ["update", "--root", str(tmp_path)])
    with pytest.raises(RuntimeError, match="âncora"):
        update_landing_data.main()
    fetch.assert_not_called()
    assert first.read_text(encoding="utf-8") == complete
    assert second.read_text(encoding="utf-8") == broken
