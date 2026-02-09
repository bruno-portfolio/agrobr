"""Testes para a API pública USDA PSD."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.usda import api


def _mock_psd_records():
    """Registros PSD mockados."""
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
    ]


class TestPsd:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        with patch.object(
            api.client,
            "fetch_psd_country",
            new_callable=AsyncMock,
            return_value=_mock_psd_records(),
        ):
            df = await api.psd("soja", country="BR", market_year=2024, api_key="test")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "commodity" in df.columns
        assert "value" in df.columns

    @pytest.mark.asyncio
    async def test_filter_attributes(self):
        with patch.object(
            api.client,
            "fetch_psd_country",
            new_callable=AsyncMock,
            return_value=_mock_psd_records(),
        ):
            df = await api.psd(
                "soja",
                country="BR",
                market_year=2024,
                attributes=["Production"],
                api_key="test",
            )

        assert len(df) == 1
        assert df.iloc[0]["attribute"] == "Production"

    @pytest.mark.asyncio
    async def test_pivot(self):
        with patch.object(
            api.client,
            "fetch_psd_country",
            new_callable=AsyncMock,
            return_value=_mock_psd_records(),
        ):
            df = await api.psd(
                "soja",
                country="BR",
                market_year=2024,
                pivot=True,
                api_key="test",
            )

        assert len(df) == 1
        assert "producao" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(
            api.client,
            "fetch_psd_country",
            new_callable=AsyncMock,
            return_value=_mock_psd_records(),
        ):
            df, meta = await api.psd(
                "soja",
                country="BR",
                market_year=2024,
                return_meta=True,
                api_key="test",
            )

        assert meta.source == "usda"
        assert meta.attempted_sources == ["usda_psd"]
        assert meta.selected_source == "usda_psd"
        assert meta.source_method == "httpx"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_world_query(self):
        mock_fn = AsyncMock(return_value=_mock_psd_records())
        with patch.object(api.client, "fetch_psd_world", mock_fn):
            df = await api.psd("soja", country="world", market_year=2024, api_key="test")

        assert len(df) == 3
        mock_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_all_countries_query(self):
        mock_fn = AsyncMock(return_value=_mock_psd_records())
        with patch.object(api.client, "fetch_psd_all_countries", mock_fn):
            df = await api.psd("soja", country="all", market_year=2024, api_key="test")

        assert len(df) == 3
        mock_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_commodity_code_resolution(self):
        with patch.object(
            api.client,
            "fetch_psd_country",
            new_callable=AsyncMock,
            return_value=_mock_psd_records(),
        ) as mock_fetch:
            await api.psd("milho", country="BR", market_year=2024, api_key="test")

        # Deve ter passado o código correto
        mock_fetch.assert_called_once_with("0440000", "BR", 2024, "test")

    @pytest.mark.asyncio
    async def test_invalid_commodity_raises(self):
        with pytest.raises(ValueError, match="desconhecida"):
            await api.psd("banana", market_year=2024, api_key="test")
