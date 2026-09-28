from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import cepea
from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.cepea import client
from agrobr.constants import CacheSettings, Fonte
from agrobr.models import Indicador
from agrobr.validators.sanity import validate_batch, validate_indicador

PAGES = Path(__file__).parents[1] / "golden_data/cepea/pages_20260905"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("product", "unit", "low", "high"),
    [("trigo", "BRL/ton", 1419.11, 1457.92), ("algodao", "cBRL/lb", 420.68, 444.82)],
)
async def test_official_cepea_native_units_pass_sanity(tmp_path, product, unit, low, high):
    store = DuckDBStore(CacheSettings(cache_dir=tmp_path))
    page = (PAGES / f"{product}.html").read_text(encoding="utf-8")
    try:
        with (
            patch("agrobr.cepea.api.get_store", return_value=store),
            patch.object(
                client,
                "fetch_indicador_page",
                AsyncMock(return_value=client.FetchResult(html=page, source="cepea")),
            ),
        ):
            df = await cepea.indicador(
                product,
                praca="parana" if product == "trigo" else None,
                inicio="2026-08-01",
                fim="2026-09-05",
                force_refresh=True,
                validate_sanity=True,
            )
    finally:
        store.close()
    assert len(df) == 15
    assert set(df["unidade"]) == {unit}
    assert df["valor"].min() == pytest.approx(low)
    assert df["valor"].max() == pytest.approx(high)
    assert not df["anomalies"].map(bool).any()


@pytest.mark.parametrize(
    ("product", "unit", "value"),
    [
        ("trigo", "BRL/ton", "100"),
        ("trigo", "BRL/ton", "10000"),
        ("algodao", "cBRL/lb", "10"),
        ("algodao", "cBRL/lb", "10000"),
    ],
)
def test_native_unit_outliers_are_still_rejected(product, unit, value):
    indicador = Indicador(
        fonte=Fonte.CEPEA,
        produto=product,
        unidade=unit,
        data=date(2026, 9, 1),
        valor=Decimal(value),
    )
    anomalies = validate_indicador(indicador)
    assert [a.anomaly_type for a in anomalies] == ["out_of_range"]


def _indicador(product, place, unit, value, day):
    return Indicador(
        fonte=Fonte.CEPEA,
        produto=product,
        praca=place,
        unidade=unit,
        valor=Decimal(value),
        data=date(2026, 9, day),
    )


@pytest.mark.asyncio
async def test_temporal_change_tracks_interleaved_product():
    soja_first = _indicador("soja", "A", "BRL/sc60kg", "100", 1)
    milho = _indicador("milho", "A", "BRL/sc60kg", "50", 2)
    soja_last = _indicador("soja", "A", "BRL/sc60kg", "150", 3)
    ordered, anomalies = await validate_batch([soja_last, milho, soja_first])
    assert ordered == [soja_first, milho, soja_last]
    assert [a.anomaly_type for a in anomalies] == ["excessive_change"]
    assert anomalies[0].details["valor_anterior"] == 100.0
    assert soja_last.anomalies == ["excessive_change: valor"]
