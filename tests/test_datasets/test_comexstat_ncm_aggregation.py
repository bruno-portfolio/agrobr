from __future__ import annotations

from fractions import Fraction
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.datasets import _comercio_exterior
from tests.helpers import (
    collect_failures,
    comexstat_csv,
    install_comexstat_http,
    isolated_dataset_case,
)


@pytest.mark.parametrize(
    "weights",
    [
        [9007199254740995],
        [4503599627370497, 4503599627370498],
        [9223372036854775807, 9223372036854775806],
    ],
)
def test_tonnes_exact_group_weight_unaffected_by_unrelated_null(weights):
    frame = pd.DataFrame(
        {
            "ano": pd.Series([2026] * len(weights), dtype="Int64"),
            "mes": pd.Series([1] * len(weights), dtype="Int64"),
            "ncm": pd.Series(["15071000", "15079019"][: len(weights)], dtype="string[python]"),
            "uf": pd.Series(["SP"] * len(weights), dtype="string[python]"),
            "kg_liquido": pd.Series(weights, dtype="Int64"),
            "valor_fob_usd": [1.0] * len(weights),
            "volume_ton": [float(Fraction(value, 1000)) for value in weights],
        }
    )
    unrelated = frame.iloc[:1].copy()
    unrelated["mes"] = pd.Series([2], dtype="Int64")
    unrelated["kg_liquido"] = pd.Series([None], dtype="Int64")
    unrelated["volume_ton"] = float("nan")
    alone, _ = _comercio_exterior.adapt_comexstat(frame, None, combine_ncms=True)
    combined, _ = _comercio_exterior.adapt_comexstat(
        pd.concat([frame, unrelated], ignore_index=True), None, combine_ncms=True
    )
    expected_weight = sum(weights)
    expected_tonnes = float(Fraction(expected_weight, 1000))
    assert alone["volume_ton"].iloc[0].hex() == expected_tonnes.hex()
    assert combined["volume_ton"].iloc[0].hex() == expected_tonnes.hex()
    assert combined["kg_liquido"].iloc[0].hex() == float(expected_weight).hex()
    assert pd.isna(combined["kg_liquido"].iloc[1])
    assert pd.isna(combined["volume_ton"].iloc[1])
    assert list(combined.columns) == [name for name in frame if name != "ncm"]
    assert str(combined["kg_liquido"].dtype) == "float64"


def test_nullable_ncm_weight_does_not_zero_fill_or_affect_other_measure():
    frame = pd.DataFrame(
        {
            "ano": pd.Series([2026, 2026], dtype="Int64"),
            "mes": pd.Series([1, 1], dtype="Int64"),
            "ncm": pd.Series(["15071000", "15079019"], dtype="string[python]"),
            "uf": pd.Series(["SP", "SP"], dtype="string[python]"),
            "kg_liquido": pd.Series([9007199254740995, None], dtype="Int64"),
            "valor_fob_usd": [0.1, 0.2],
            "volume_ton": [9007199254740.994, float("nan")],
        }
    )
    result, _ = _comercio_exterior.adapt_comexstat(frame, None, combine_ncms=True)
    assert len(result) == 1
    assert pd.isna(result["kg_liquido"].iloc[0])
    assert pd.isna(result["volume_ton"].iloc[0])
    assert result["valor_fob_usd"].iloc[0].hex() == (0.1 + 0.2).hex()


@pytest.mark.parametrize("produto", ["soja", "algodao"])
@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
async def test_dataset_empty_preserves_source_extras(monkeypatch, produto, fluxo):
    ncm = "12019000" if produto == "soja" else "52010020"
    row = ["2024", "01", ncm, "10", "160", "MT", "01", "0817800", "1", "1000", "50"]
    if fluxo == "importacao":
        row += ["1", "0"]
    install_comexstat_http(monkeypatch, comexstat_csv(fluxo=fluxo, rows=[row]))
    function = getattr(datasets, fluxo)
    full = await function(produto, ano=2024, uf="MT")
    empty, meta = await function(produto, ano=2024, uf="SP", return_meta=True)
    assert empty.empty
    pd.testing.assert_series_equal(full.dtypes, empty.dtypes)
    assert list(full.columns) == list(empty.columns)
    assert full["produto"].dtype == pd.Series([""]).dtype
    assert "ncm" not in empty
    assert "volume_ton" in empty
    assert meta.source_details["coverage"]["source_rows"] == 1


def test_comexstat_ncm_aggregation_casos_1():
    with collect_failures() as check:
        case = "test_ncm_aggregation_preserves_null_uf_and_separates_products"
        with check(case), isolated_dataset_case(case):
            frame = pd.DataFrame(
                [
                    {
                        "ano": 2024,
                        "mes": 1,
                        "produto": produto,
                        "uf": uf,
                        "kg_liquido": kg,
                        "valor_fob_usd": usd,
                        "ncm": ncm,
                    }
                    for produto, uf, kg, usd, ncm in [
                        ("oleo_soja", None, 10, 5, "15071000"),
                        ("oleo_soja", None, 20, 7, "15079019"),
                        ("oleo_soja", "SP", 40, 9, "15071000"),
                        ("outro", None, 8, 2, "00000000"),
                    ]
                ]
            )
            result = _comercio_exterior.agregar_ncms(frame)
            assert len(result) == 3
            missing_uf = result.loc[(result["produto"] == "oleo_soja") & result["uf"].isna()].iloc[
                0
            ]
            assert missing_uf["kg_liquido"] == 30
            assert missing_uf["valor_fob_usd"] == 12
            assert result["kg_liquido"].sum() == frame["kg_liquido"].sum()
            assert "ncm" not in result.columns
        case = "test_aggregation_propagates_missing_measure"
        with check(case), isolated_dataset_case(case):
            frame = pd.DataFrame(
                {
                    "ano": [2024, 2024],
                    "mes": [1, 1],
                    "ncm": ["15071000", "15079019"],
                    "uf": ["SP", "SP"],
                    "kg_liquido": pd.Series([1, None], dtype="Int64"),
                    "valor_fob_usd": [1.0, 2.0],
                    "valor_frete_usd": [None, 2.0],
                    "valor_seguro_usd": [0.0, 0.0],
                    "volume_ton": [0.001, None],
                }
            )
            result = _comercio_exterior.agregar_ncms(frame)
            assert pd.isna(result.iloc[0]["kg_liquido"])
            assert pd.isna(result.iloc[0]["volume_ton"])
            assert pd.isna(result.iloc[0]["valor_frete_usd"])
            assert result.iloc[0]["valor_fob_usd"] == 3.0
            assert result.iloc[0]["valor_seguro_usd"] == 0.0


@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
@pytest.mark.parametrize("kwargs", [{"pais": 160}, {"via": "01"}, {"urf": "0817800"}, {"mes": 1}])
async def test_unsupported_dataset_filter_before_fallback(monkeypatch, fluxo, kwargs):
    calls = install_comexstat_http(monkeypatch, b"unreached", status=503)
    with (
        patch("agrobr.abiove.exportacao", new_callable=AsyncMock) as fallback,
        pytest.raises(TypeError, match="unexpected keyword argument"),
    ):
        await getattr(datasets, fluxo)("soja", ano=2024, **kwargs)
    assert calls == []
    fallback.assert_not_awaited()
