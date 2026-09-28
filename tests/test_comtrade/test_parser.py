from __future__ import annotations

import pandas as pd
import pytest

from agrobr.comtrade import models, parser
from agrobr.exceptions import ParseError
from tests.test_comtrade.replay import record


@pytest.mark.parametrize(
    "name,classification,year", [("soy_br_cn_2021", "H5", 2021), ("soy_br_cn_2023", "H6", 2023)]
)
def test_official_record_preserves_identity_classification_units_and_nullable_values(
    name, classification, year, captures
):
    raw = record(captures, name)
    frame = parser.parse_trade_data([raw])
    assert frame.columns.tolist() == models.COLUNAS_SAIDA
    assert len(frame.columns) == 27
    row = frame.iloc[0]
    assert row["classificacao"] == classification
    assert bool(row["classificacao_original"]) == raw["isOriginalClassification"]
    assert row["reporter_code"] == 76 and row["partner_code"] == 156
    assert row["ano"] == year and pd.isna(row["mes"])
    assert row["peso_liquido_kg"] == raw["netWgt"]
    assert row["volume_ton"] == pytest.approx(raw["netWgt"] / 1000)
    assert pd.isna(row["valor_cif_usd"])
    for column in ["ano", "mes", "reporter_code", "partner_code", "nivel_hs"]:
        assert str(frame[column].dtype) == "Int64"
    assert str(frame["classificacao_original"].dtype) == "boolean"


@pytest.mark.parametrize(
    "field,value",
    [
        ("netWgt", "bad"),
        ("netWgt", float("nan")),
        ("netWgt", float("inf")),
        ("netWgt", -1),
        ("netWgt", True),
        ("primaryValue", float("-inf")),
        ("fobvalue", -1),
        ("qty", "bad"),
        ("period", "202313"),
        ("refYear", 2022),
        ("refMonth", 13),
        ("typeCode", "S"),
        ("freqCode", "Q"),
        ("flowCode", "RX"),
        ("partner2Code", 156),
        ("motCode", 1000),
        ("customsCode", "C01"),
        ("reporterCode", True),
        ("partnerCode", -1),
        ("cmdCode", 1201),
        ("classificationCode", ""),
    ],
)
def test_malformed_external_record_is_not_coerced_or_deduplicated(field, value, captures):
    with pytest.raises(ParseError):
        parser.parse_trade_data([record(captures, **{field: value})])


def test_null_zero_and_descriptive_absence_remain_distinct(captures):
    frame = parser.parse_trade_data(
        [record(captures, netWgt=None, fobvalue=0.0, reporterISO=None, partnerISO=None, qty=0.0)]
    )
    row = frame.iloc[0]
    assert pd.isna(row["peso_liquido_kg"]) and pd.isna(row["volume_ton"])
    assert row["valor_fob_usd"] == row["quantidade"] == 0
    assert pd.isna(row["reporter_iso"]) and pd.isna(row["partner_iso"])


def mirror_frames(captures):
    return (
        parser.parse_trade_data([record(captures)]),
        parser.parse_trade_data([record(captures, "soy_cn_br_2023_mirror")]),
    )


@pytest.mark.parametrize("side", ["reporter", "partner"])
def test_mirror_rejects_many_to_many_or_duplicate_leg(side, captures):
    left, right = mirror_frames(captures)
    if side == "reporter":
        left = pd.concat([left, left], ignore_index=True)
    else:
        right = pd.concat([right, right], ignore_index=True)
    with pytest.raises(ParseError):
        parser.parse_mirror(left, right, "BRA", "CHN")


def test_mirror_rejects_unharmonized_hs_revisions(captures):
    left, right = mirror_frames(captures)
    right["classificacao"] = "H5"
    with pytest.raises(ParseError):
        parser.parse_mirror(left, right, "BRA", "CHN")
