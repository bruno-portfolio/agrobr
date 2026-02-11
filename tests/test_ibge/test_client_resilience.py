"""Testes de resiliência HTTP para agrobr.ibge.client (via sidrapy)."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.ibge import client


class TestIbgeSidraTimeout:
    @pytest.mark.asyncio
    async def test_sidrapy_timeout_propagates(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = Exception("Connection timed out")
            with pytest.raises(Exception, match="Connection timed out"):
                await client.fetch_sidra(table_code="5457")

    @pytest.mark.asyncio
    async def test_sidrapy_timeout_retried(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = [
                Exception("timeout"),
                pd.DataFrame({"V": ["100"]}),
            ]
            result = await client.fetch_sidra(table_code="5457")
            assert len(result) > 0


class TestIbgeSidraHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_propagates(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = Exception("Internal Server Error")
            with pytest.raises(Exception, match="Internal Server Error"):
                await client.fetch_sidra(table_code="5457")

    @pytest.mark.asyncio
    async def test_http_403_propagates(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = Exception("403 Forbidden")
            with pytest.raises(Exception, match="403"):
                await client.fetch_sidra(table_code="5457")


class TestIbgeSidraEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_dataframe(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame()
            result = await client.fetch_sidra(table_code="5457", header="y")
            assert len(result) == 0

    @pytest.mark.asyncio
    async def test_header_n_removes_first_row(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame({"V": ["header", "100", "200"]})
            result = await client.fetch_sidra(table_code="5457", header="n")
            assert len(result) == 2
            assert result["V"].iloc[0] == "100"


class TestIbgeSidraVariableHandling:
    @pytest.mark.asyncio
    async def test_variable_as_list(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame({"V": ["header", "100"]})
            await client.fetch_sidra(table_code="5457", variable=["214", "215"])
            kwargs = mock_sidra.call_args[1]
            assert kwargs["variable"] == "214,215"

    @pytest.mark.asyncio
    async def test_period_as_list(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame({"V": ["header", "100"]})
            await client.fetch_sidra(table_code="5457", period=["2022", "2023"])
            kwargs = mock_sidra.call_args[1]
            assert kwargs["period"] == "2022,2023"
