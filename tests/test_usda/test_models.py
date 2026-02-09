"""Testes para os modelos USDA PSD."""

import pytest

from agrobr.usda.models import (
    PSD_ATTRIBUTES,
    PSD_COLUMNS_MAP,
    PSD_COMMODITIES,
    PSD_COUNTRIES,
    PSDRecord,
    commodity_name,
    resolve_commodity_code,
    resolve_country_code,
)


class TestResolveCommodityCode:
    def test_soja(self):
        assert resolve_commodity_code("soja") == "2222000"

    def test_soybeans_english(self):
        assert resolve_commodity_code("soybeans") == "2222000"

    def test_milho(self):
        assert resolve_commodity_code("milho") == "0440000"

    def test_trigo(self):
        assert resolve_commodity_code("trigo") == "0410000"

    def test_arroz(self):
        assert resolve_commodity_code("arroz") == "0422110"

    def test_algodao(self):
        assert resolve_commodity_code("algodao") == "2631000"

    def test_acucar(self):
        assert resolve_commodity_code("acucar") == "0612000"

    def test_direct_code(self):
        assert resolve_commodity_code("2222000") == "2222000"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="desconhecida"):
            resolve_commodity_code("banana")

    def test_case_insensitive(self):
        assert resolve_commodity_code("Soja") == "2222000"
        assert resolve_commodity_code("MILHO") == "0440000"


class TestResolveCountryCode:
    def test_brasil(self):
        assert resolve_country_code("brasil") == "BR"
        assert resolve_country_code("brazil") == "BR"
        assert resolve_country_code("BR") == "BR"

    def test_eua(self):
        assert resolve_country_code("eua") == "US"
        assert resolve_country_code("usa") == "US"
        assert resolve_country_code("US") == "US"

    def test_china(self):
        assert resolve_country_code("china") == "CH"

    def test_short_code_passthrough(self):
        assert resolve_country_code("AR") == "AR"
        assert resolve_country_code("IN") == "IN"

    def test_unknown_long_name_raises(self):
        with pytest.raises(ValueError, match="desconhecido"):
            resolve_country_code("pais_inventado")


class TestCommodityName:
    def test_known_codes(self):
        assert commodity_name("2222000") == "soja"
        assert commodity_name("0440000") == "milho"
        assert commodity_name("0410000") == "trigo"

    def test_unknown_returns_code(self):
        assert commodity_name("9999999") == "9999999"


class TestPSDRecord:
    def test_basic_creation(self):
        rec = PSDRecord(
            commodity_code="2222000",
            commodity="Soybeans",
            country_code="BR",
            country="Brazil",
            market_year=2024,
            attribute="Production",
            value=169000.0,
            unit="(1000 MT)",
        )

        assert rec.commodity_code == "2222000"
        assert rec.commodity == "soybeans"  # normalized
        assert rec.country == "Brazil"
        assert rec.market_year == 2024
        assert rec.attribute == "production"  # normalized
        assert rec.value == 169000.0

    def test_value_optional(self):
        rec = PSDRecord(
            commodity_code="2222000",
            commodity="Soybeans",
            country_code="BR",
            country="Brazil",
            market_year=2024,
            attribute="Production",
        )
        assert rec.value is None


class TestConstants:
    def test_commodities_has_main_products(self):
        assert "soja" in PSD_COMMODITIES
        assert "milho" in PSD_COMMODITIES
        assert "trigo" in PSD_COMMODITIES
        assert "arroz" in PSD_COMMODITIES
        assert "algodao" in PSD_COMMODITIES
        assert "acucar" in PSD_COMMODITIES

    def test_attributes_has_main_ids(self):
        assert 125 in PSD_ATTRIBUTES  # Production
        assert 88 in PSD_ATTRIBUTES  # Exports
        assert 130 in PSD_ATTRIBUTES  # Imports
        assert 84 in PSD_ATTRIBUTES  # Ending Stocks
        assert 57 in PSD_ATTRIBUTES  # Domestic Consumption

    def test_countries_has_brazil(self):
        assert PSD_COUNTRIES["brasil"] == "BR"
        assert PSD_COUNTRIES["brazil"] == "BR"

    def test_columns_map_has_key_fields(self):
        assert "CommodityCode" in PSD_COLUMNS_MAP
        assert "CountryCode" in PSD_COLUMNS_MAP
        assert "MarketYear" in PSD_COLUMNS_MAP
        assert "Value" in PSD_COLUMNS_MAP
        assert "AttributeDescription" in PSD_COLUMNS_MAP
