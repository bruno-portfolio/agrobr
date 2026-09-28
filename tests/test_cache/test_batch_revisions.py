from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.models import Indicador


def test_invalid_last_revision_preserves_valid_values_and_distinct_sources(tmp_path):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    key = {"produto": "soja", "data": date(2026, 9, 1), "unidade": "BRL/sc60kg"}
    rows = [
        dict(key, valor=100, fonte="cepea"),
        dict(key, valor=105, fonte="noticias_agricolas"),
        dict(key, valor=110, fonte="cepea"),
        dict(key, valor=float("inf"), fonte="cepea"),
    ]
    try:
        assert store.indicadores_upsert(rows) == 3
        saved = store.indicadores_query("soja")
        assert {row["fonte"]: float(row["valor"]) for row in saved} == {
            "cepea": 110,
            "noticias_agricolas": 105,
        }
    finally:
        store.close()


@pytest.mark.parametrize("reverse", [False, True])
async def test_public_refresh_and_offline_agree_on_revisions(tmp_path, monkeypatch, reverse):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    reference = date.today()
    first = Indicador(
        produto="soja",
        praca="Paranaguá/PR",
        data=reference,
        valor=100,
        unidade="BRL/sc60kg",
        fonte=constants.Fonte.CEPEA,
        parsed_at=datetime(2026, 9, 1),
    )
    latest = Indicador.model_validate(
        first.model_dump() | {"valor": 110, "parsed_at": first.parsed_at + timedelta(seconds=1)}
    )
    records = [latest, first] if reverse else [first, latest]
    fetch = AsyncMock(
        return_value=api._FetchResult(records, "cepea", "https://example.org", 2, "test", 1, 0)
    )
    monkeypatch.setattr(api, "_fetch_and_parse", fetch)
    try:
        refreshed = await datasets.preco_diario(
            "soja", inicio=reference, fim=reference, force_refresh=True
        )
        offline = await datasets.preco_diario("soja", inicio=reference, fim=reference, offline=True)
        assert refreshed["valor"].tolist() == offline["valor"].tolist() == [110]
        fetch.assert_awaited_once()
    finally:
        store.close()
