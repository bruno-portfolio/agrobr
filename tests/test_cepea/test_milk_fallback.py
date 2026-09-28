from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import duckdb
import pytest
from bs4 import BeautifulSoup

from agrobr import constants, noticias_agricolas
from agrobr.cache import duckdb_store
from agrobr.cepea import api, client
from agrobr.cepea.parsers import v1
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.helpers import insert_cache_indicator, seed_cache_schema

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
MILK = GOLDEN / "na" / "leite_publicacao_20260906"


def test_na_milk_official_publication_and_reference_are_distinct():
    raw = (MILK / "response.html").read_bytes()
    metadata = json.loads((MILK / "metadata.json").read_text(encoding="utf-8"))
    expected = json.loads((MILK / "expected.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(raw).hexdigest() == metadata["sha256"]
    soup = BeautifulSoup(raw.decode("utf-8"), "lxml")
    blocks = soup.select("div.cotacao")
    assert len(blocks) == 10
    for block, period in zip(blocks, expected["periods"], strict=True):
        text = block.get_text(" ", strip=True)
        assert f"Referência: {period['reference_note']}" in text
        publication = date.fromisoformat(period["publication"]).strftime("%d/%m/%Y")
        assert f"Fechamento: {publication}" in text
    records = noticias_agricolas.parse_indicador(raw.decode("utf-8"), "leite")
    assert [
        {"data": str(row.data), "praca": row.praca, "valor": str(row.valor), "unidade": row.unidade}
        for row in records
    ] == expected["observations"]
    cepea_html = (GOLDEN / "cepea" / "pages_20260905" / "leite.html").read_text(encoding="utf-8")
    cepea_table = BeautifulSoup(cepea_html, "lxml").select_one("#imagenet-indicador1")
    assert cepea_table is not None
    cells = [
        [cell.get_text(strip=True) for cell in row.select("td")] for row in cepea_table.select("tr")
    ]
    assert ["jul/26", "BRASIL", "2,8761"] in cells
    assert ["jul/26", "SP", "2,9695"] in cells
    cepea = v1.CepeaParserV1().parse(cepea_html, "leite")
    assert next(
        row for row in cepea if row.praca == "BRASIL" and row.data == date(2026, 7, 1)
    ).valor == Decimal("2.8761")
    assert next(
        row for row in records if row.praca == "Brasil" and row.data == date(2026, 9, 1)
    ).valor == Decimal("2.8761")
    assert next(
        row for row in records if row.praca == "SP" and row.data == date(2026, 7, 1)
    ).valor == Decimal("2.6999")
    assert expected["periods"][0]["reference_note"] == "Julho/26"
    assert expected["periods"][2]["reference_note"] == "Maio/26"


@pytest.mark.parametrize("produto", ["leite", "LEITE"])
async def test_cepea_milk_network_failure_does_not_fetch_na(produto, monkeypatch):
    monkeypatch.setattr(client, "_use_alternative_source", True)
    monkeypatch.setattr(client, "_use_browser", False)
    monkeypatch.setattr(client, "_try_endpoints", AsyncMock(return_value=None))
    na_fetch = AsyncMock()
    monkeypatch.setattr(client, "_fetch_with_alternative_source", na_fetch)
    with pytest.raises(SourceUnavailableError) as exc:
        await client.fetch_indicador_page(produto)
    assert exc.value.attempted_sources == ["cepea"]
    na_fetch.assert_not_awaited()


@pytest.mark.parametrize("enabled", [False, True])
async def test_cepea_forced_milk_alternative_is_rejected(monkeypatch, enabled):
    monkeypatch.setattr(client, "_use_alternative_source", enabled)
    na_fetch = AsyncMock()
    monkeypatch.setattr(client, "_fetch_with_alternative_source", na_fetch)
    with pytest.raises(SourceUnavailableError, match="data de publicação"):
        await client.fetch_indicador_page("leite", force_alternative=True)
    na_fetch.assert_not_awaited()


async def test_cepea_milk_offline_after_v8_upgrade_excludes_na_publications(tmp_path, monkeypatch):
    settings = constants.CacheSettings(cache_dir=tmp_path)
    with duckdb.connect(str(tmp_path / settings.db_name)) as conn:
        seed_cache_schema(conn, 8)
        insert_cache_indicator(
            conn,
            produto="leite",
            fonte="noticias_agricolas",
            praca="Brasil",
            data=date(2026, 9, 1),
            valor="2.8761",
            unidade="BRL/L",
            parser_version=3,
        )
        insert_cache_indicator(
            conn,
            produto="leite",
            fonte="cepea",
            praca="BRASIL",
            data=date(2026, 7, 1),
            valor="2.8761",
            unidade="BRL/L",
            parser_version=2,
        )
    store = duckdb_store.DuckDBStore(settings)
    monkeypatch.setattr(api, "get_store", lambda: store)
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_indicador_page", fetch)
    try:
        frame = await api.indicador("leite", inicio="2026-07-01", fim="2026-09-01", offline=True)
        assert len(frame) == 1
        assert frame.iloc[0].data.date() == date(2026, 7, 1)
        assert frame.iloc[0].valor == 2.8761
        assert frame.iloc[0].fonte == "cepea"
        fetch.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.parametrize("entrypoint", ["indicador", "ultimo"])
async def test_cepea_milk_content_failure_does_not_fetch_na(entrypoint, monkeypatch):
    store = MagicMock()
    store.indicadores_query.return_value = []
    store.indicadores_ultima_coleta.return_value = None
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(client, "_use_alternative_source", True)
    fetch = AsyncMock(return_value=client.FetchResult("<html>Sem dados</html>", "cepea"))
    monkeypatch.setattr(client, "fetch_indicador_page", fetch)
    mensagem = {"indicador": "All parsers failed", "ultimo": "No indicators found for leite"}
    with pytest.raises(ParseError, match=mensagem[entrypoint]):
        await getattr(api, entrypoint)("leite")
    fetch.assert_awaited_once_with("leite")
    store.indicadores_upsert.assert_not_called()
