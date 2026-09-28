from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import cepea
from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.cepea import client
from agrobr.constants import CEPEA_PRODUTOS, CacheSettings, Fonte
from agrobr.exceptions import ValidationError
from agrobr.models import Indicador
from agrobr.validators import sanity

FIXTURES = Path(__file__).parents[1] / "golden_data/cepea/sanity_20260906"
OBSERVATIONS = json.loads((FIXTURES / "observations.json").read_text(encoding="utf-8"))
HISTORICAL_EXTREMES = json.loads(
    (FIXTURES / "historical_extremes.json").read_text(encoding="utf-8")
)
NEW_RANGES = [
    ("soja_parana", "30", "300"),
    ("cafe_arabica", "200", "3000"),
    ("arroz", "8", "300"),
    ("acucar", "8", "400"),
    ("acucar_refinado", "0.2", "8"),
    ("frango_congelado", "0.6", "20"),
    ("frango_resfriado", "0.6", "20"),
    ("suino", "0.8", "30"),
    ("etanol_hidratado", "0.1", "8"),
    ("etanol_anidro", "0.1", "10"),
    ("leite", "0.1", "8"),
    ("laranja_industria", "4", "300"),
    ("laranja_in_natura", "4", "300"),
]


def _observation(product: str) -> Indicador:
    source = OBSERVATIONS[product]
    row = source["records"][-1]
    return Indicador(
        fonte=Fonte.CEPEA,
        produto=product,
        data=date.fromisoformat(row["date"]),
        valor=Decimal(row["value"]),
        unidade=source["unit"],
        praca=row["place"],
    )


def test_every_catalog_product_has_unit_and_price_coverage():
    assert set(OBSERVATIONS) == set(CEPEA_PRODUTOS) == set(sanity.PRICE_RULES)
    assert sum(len(source["records"]) for source in OBSERVATIONS.values()) == 337


@pytest.mark.asyncio
@pytest.mark.parametrize("product", OBSERVATIONS)
async def test_official_price_cells_pass_public_api_with_sanity(tmp_path, product):
    source = OBSERVATIONS[product]
    raw = (FIXTURES / source["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == source["sha256"]
    store = DuckDBStore(CacheSettings(cache_dir=tmp_path))
    try:
        with (
            patch("agrobr.cepea.api.get_store", return_value=store),
            patch.object(
                client,
                "fetch_indicador_page",
                AsyncMock(return_value=client.FetchResult(html=raw.decode(), source="cepea")),
            ),
        ):
            frame = await cepea.indicador(
                product,
                praca="parana" if product == "trigo" else None,
                inicio="2026-01-01",
                fim="2026-09-05",
                force_refresh=True,
                validate_sanity=True,
            )
    finally:
        store.close()
    regional = product in {"suino", "leite"}
    actual = {
        (str(row.data)[:10], row.praca if regional else None): Decimal(str(row.valor))
        for row in frame.itertuples()
    }
    expected = {(row["date"], row["place"]): Decimal(row["value"]) for row in source["records"]}
    assert actual == expected
    assert len(frame) == len(expected)
    assert set(frame["unidade"]) == {source["unit"]}
    assert not frame["anomalies"].map(bool).any()


@pytest.mark.parametrize(
    "cell", HISTORICAL_EXTREMES, ids=lambda cell: f"{cell['product']}-{cell['extreme']}"
)
def test_published_historical_extreme_passes_value_range(cell):
    indicator = Indicador(
        fonte=Fonte.CEPEA,
        produto=cell["product"],
        data=date.fromisoformat(cell["date"]),
        valor=Decimal(cell["value"]),
        unidade=cell["unit"],
        praca=cell["place"],
    )
    assert sanity.validate_indicador(indicator) == []


@pytest.mark.parametrize(
    "product,value",
    [
        ("laranja_industria", "8.45"),
        ("laranja_industria", "14.16"),
        ("laranja_in_natura", "9.62"),
        ("laranja_in_natura", "111.39"),
    ],
)
def test_citrus_documentary_references_remain_in_plausible_range(product, value):
    indicator = _observation(product).model_copy(update={"valor": Decimal(value)})
    assert sanity.validate_indicador(indicator) == []


@pytest.mark.parametrize("product", OBSERVATIONS)
@pytest.mark.parametrize("factor", ["0.001", "0.01", "100", "1000"])
def test_official_price_with_large_scale_error_is_flagged(product, factor):
    indicator = _observation(product)
    indicator.valor *= Decimal(factor)
    anomalies = sanity.validate_indicador(indicator)
    assert [anomaly.anomaly_type for anomaly in anomalies] == ["out_of_range"]
    assert anomalies[0].severity == "critical"


@pytest.mark.parametrize("product", OBSERVATIONS)
@pytest.mark.parametrize("mismatch", ["currency", "measure"])
def test_wrong_unit_is_reported_before_value_or_temporal_range(product, mismatch):
    indicator = _observation(product)
    unit = indicator.unidade
    indicator.unidade = (
        unit.replace("BRL", "USD")
        if mismatch == "currency"
        else "BRL/kg"
        if unit == "BRL/ton"
        else "BRL/ton"
    )
    indicator.valor = Decimal("1000000")
    anomalies = sanity.validate_indicador(indicator, Decimal("1"))
    assert [anomaly.anomaly_type for anomaly in anomalies] == ["unit_mismatch"]
    assert anomalies[0].field == "unidade"
    assert anomalies[0].severity == "critical"
    assert anomalies[0].value == indicator.unidade
    assert anomalies[0].expected_range == unit


@pytest.mark.asyncio
async def test_strict_batch_rejects_unit_mismatch():
    indicator = _observation("acucar_refinado")
    indicator.unidade = "BRL/sc50kg"
    with pytest.raises(ValidationError, match="unit_mismatch"):
        await sanity.validate_batch([indicator], strict=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("product", ["etanol_hidratado", "etanol_anidro", "leite"])
async def test_weekly_and_monthly_prices_do_not_receive_daily_change_limit(product):
    first = _observation(product).model_copy(
        update={"data": date(2026, 7, 1), "valor": Decimal("1")}
    )
    last = first.model_copy(
        update={"data": date(2026, 8, 1), "valor": Decimal("3"), "anomalies": []}
    )
    _, anomalies = await sanity.validate_batch([last, first], strict=True)
    assert anomalies == []
    assert sanity.PRICE_RULES[product].max_daily_change_pct is None


@pytest.mark.parametrize("product", [row[0] for row in NEW_RANGES][2:])
def test_new_markets_have_no_uncalibrated_daily_limit(product):
    indicator = _observation(product)
    assert sanity.validate_indicador(indicator, indicator.valor / 3) == []


@pytest.mark.parametrize("product,original", [("cafe_arabica", "cafe"), ("soja_parana", "soja")])
def test_same_measure_rules_preserve_existing_bounds_without_shared_mutability(product, original):
    alias_rule = sanity.PRICE_RULES[product]
    original_rule = sanity.PRICE_RULES[original]
    assert alias_rule is not original_rule
    assert alias_rule.min_value == original_rule.min_value
    assert alias_rule.max_value == original_rule.max_value
    assert alias_rule.max_daily_change_pct == original_rule.max_daily_change_pct
    assert alias_rule.expected_unit == original_rule.expected_unit


def test_sanity_rule_original_positional_arguments_remain_compatible(monkeypatch):
    rule = sanity.SanityRule("valor", Decimal("1"), Decimal("5"), None, "Custom measure")
    monkeypatch.setitem(sanity.PRICE_RULES, "custom", rule)
    indicator = _observation("leite").model_copy(
        update={"produto": "custom", "unidade": "custom/unit", "valor": Decimal("2")}
    )
    assert rule.expected_unit is None
    assert sanity.validate_indicador(indicator) == []


def test_crystal_and_refined_sugar_use_different_measures():
    crystal = _observation("acucar")
    refined = _observation("acucar_refinado")
    assert crystal.unidade == "BRL/sc50kg"
    assert refined.unidade == "BRL/kg"
    refined.valor = crystal.valor
    assert [anomaly.anomaly_type for anomaly in sanity.validate_indicador(refined)] == [
        "out_of_range"
    ]
