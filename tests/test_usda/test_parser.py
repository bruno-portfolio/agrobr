"""Testes para o parser USDA PSD."""

import pandas as pd

from agrobr.usda.parser import (
    PARSER_VERSION,
    filter_attributes,
    parse_psd_response,
    pivot_attributes,
)


def _mock_psd_records():
    """Registros PSD mockados (formato API response)."""
    return [
        {
            "CommodityCode": "2222000",
            "CommodityDescription": "Soybeans",
            "CountryCode": "BR",
            "CountryName": "Brazil",
            "MarketYear": 2024,
            "CalendarYear": 2024,
            "Month": "Mar",
            "AttributeId": 125,
            "AttributeDescription": "Production",
            "UnitId": 21,
            "UnitDescription": "(1000 MT)",
            "Value": 169000.0,
        },
        {
            "CommodityCode": "2222000",
            "CommodityDescription": "Soybeans",
            "CountryCode": "BR",
            "CountryName": "Brazil",
            "MarketYear": 2024,
            "CalendarYear": 2024,
            "Month": "Mar",
            "AttributeId": 88,
            "AttributeDescription": "Exports",
            "UnitId": 21,
            "UnitDescription": "(1000 MT)",
            "Value": 105000.0,
        },
        {
            "CommodityCode": "2222000",
            "CommodityDescription": "Soybeans",
            "CountryCode": "BR",
            "CountryName": "Brazil",
            "MarketYear": 2024,
            "CalendarYear": 2024,
            "Month": "Mar",
            "AttributeId": 57,
            "AttributeDescription": "Domestic Consumption",
            "UnitId": 21,
            "UnitDescription": "(1000 MT)",
            "Value": 56500.0,
        },
        {
            "CommodityCode": "2222000",
            "CommodityDescription": "Soybeans",
            "CountryCode": "BR",
            "CountryName": "Brazil",
            "MarketYear": 2024,
            "CalendarYear": 2024,
            "Month": "Mar",
            "AttributeId": 84,
            "AttributeDescription": "Ending Stocks",
            "UnitId": 21,
            "UnitDescription": "(1000 MT)",
            "Value": 38200.0,
        },
    ]


class TestParsePsdResponse:
    def test_returns_dataframe(self):
        df = parse_psd_response(_mock_psd_records())

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4
        assert "commodity_code" in df.columns
        assert "commodity" in df.columns
        assert "country_code" in df.columns
        assert "attribute" in df.columns
        assert "value" in df.columns
        assert "unit" in df.columns

    def test_column_rename(self):
        df = parse_psd_response(_mock_psd_records())

        # CommodityCode -> commodity_code
        assert "CommodityCode" not in df.columns
        assert "commodity_code" in df.columns

    def test_commodity_name_normalized(self):
        df = parse_psd_response(_mock_psd_records())

        assert all(df["commodity"] == "soja")

    def test_attribute_br_added(self):
        df = parse_psd_response(_mock_psd_records())

        assert "attribute_br" in df.columns
        producao_row = df[df["attribute"] == "Production"]
        assert producao_row.iloc[0]["attribute_br"] == "producao"

    def test_auxiliary_columns_removed(self):
        df = parse_psd_response(_mock_psd_records())

        assert "calendar_year" not in df.columns
        assert "month" not in df.columns
        assert "attribute_id" not in df.columns

    def test_empty_records(self):
        df = parse_psd_response([])

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "commodity_code" in df.columns

    def test_sorted_by_market_year(self):
        records = _mock_psd_records()
        df = parse_psd_response(records)

        assert df["market_year"].is_monotonic_increasing or len(df["market_year"].unique()) == 1


class TestFilterAttributes:
    def test_filter_single(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, ["Production"])

        assert len(filtered) == 1
        assert filtered.iloc[0]["attribute"] == "Production"

    def test_filter_multiple(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, ["Production", "Exports"])

        assert len(filtered) == 2

    def test_filter_case_insensitive(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, ["production", "exports"])

        assert len(filtered) == 2

    def test_filter_by_brazilian_name(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, ["producao"])

        assert len(filtered) == 1

    def test_none_returns_all(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, None)

        assert len(filtered) == 4

    def test_empty_list_returns_all(self):
        df = parse_psd_response(_mock_psd_records())
        filtered = filter_attributes(df, [])

        assert len(filtered) == 4

    def test_empty_df(self):
        df = parse_psd_response([])
        filtered = filter_attributes(df, ["Production"])

        assert len(filtered) == 0


class TestPivotAttributes:
    def test_basic_pivot(self):
        df = parse_psd_response(_mock_psd_records())
        pivoted = pivot_attributes(df)

        assert len(pivoted) == 1  # 1 commodity/country/year
        assert "producao" in pivoted.columns
        assert "exportacao" in pivoted.columns

    def test_pivot_values(self):
        df = parse_psd_response(_mock_psd_records())
        pivoted = pivot_attributes(df)

        assert pivoted.iloc[0]["producao"] == 169000.0
        assert pivoted.iloc[0]["exportacao"] == 105000.0

    def test_empty_df(self):
        df = parse_psd_response([])
        pivoted = pivot_attributes(df)

        assert len(pivoted) == 0


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
