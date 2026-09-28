from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import constants, contracts, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.cepea.parsers import v1
from agrobr.models import Indicador

PAGES = Path(__file__).resolve().parents[1] / "golden_data" / "cepea" / "pages_20260905"


def _parse(page: str, produto: str) -> list[Indicador]:
    return v1.CepeaParserV1().parse((PAGES / f"{page}.html").read_text(encoding="utf-8"), produto)


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[duckdb_store.DuckDBStore]:
    database = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: database)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: database)
    try:
        yield database
    finally:
        database.close()


def test_dataframe_expoe_colunas_opcionais_como_float():
    frame = api._to_dataframe(_parse("bezerro", "bezerro") + _parse("leite", "leite"))
    assert frame["valor_usd"].dtype == "float64"
    assert frame["peso_medio_kg"].dtype == "float64"
    assert frame.loc[frame["produto"] == "bezerro", "peso_medio_kg"].notna().all()
    leite = frame.loc[frame["produto"] == "leite", ["valor_usd", "peso_medio_kg"]]
    assert leite.isna().all().all()
    assert api._to_dataframe([]).columns.tolist()[-2:] == ["valor_usd", "peso_medio_kg"]


async def test_cache_preserva_dolar_e_peso_no_dataset_e_offline(store, monkeypatch):
    records = _parse("bezerro", "bezerro")
    fetched = api._FetchResult(records, "cepea", "https://example.invalid/bezerro", 2, "", 0, 0)
    monkeypatch.setattr(api, "_fetch_and_parse", AsyncMock(return_value=fetched))
    inicio, fim = date(2026, 8, 17), date(2026, 9, 4)

    frame, meta = await datasets.preco_diario(
        "bezerro", inicio=inicio, fim=fim, force_refresh=True, return_meta=True
    )
    offline = await api.indicador("bezerro", inicio=inicio, fim=fim, offline=True)

    assert meta.contract_version == "1.1"
    assert contracts.get_contract("preco_diario").validate(frame) == (True, [])
    latest = frame.loc[frame["data"] == "2026-09-04"].iloc[0]
    assert (latest["valor"], latest["valor_usd"], latest["peso_medio_kg"]) == (
        3397.99,
        662.38,
        211.69,
    )
    assert offline[["valor_usd", "peso_medio_kg"]].notna().all().all()
    assert offline.loc[offline["data"] == "2026-09-04", "peso_medio_kg"].item() == 211.69
    cached = {row["data"]: row for row in store.indicadores_query("bezerro")}
    assert float(cached[date(2026, 9, 4)]["valor_usd"]) == 662.38
