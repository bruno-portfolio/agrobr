"""Testes de resiliência HTTP para agrobr.ibge.client (via sidrapy)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import client
from tests.helpers import RETRY_SLEEP, make_sleep_tracker


class TestIbgeSidraTimeout:
    @pytest.mark.asyncio
    async def test_sidrapy_timeout_propagates(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = Exception("Connection timed out")
            with pytest.raises(Exception, match="Connection timed out"):
                await client.fetch_sidra(table_code="5457")

    @pytest.mark.asyncio
    async def test_sidrapy_timeout_retried(self):
        import requests

        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.side_effect = [
                requests.exceptions.ConnectionError("timeout"),
                pd.DataFrame({"V": ["100"]}),
            ]
            result = await client.fetch_sidra(table_code="5457")
            assert len(result) > 0


class TestIbgeSidraHTTPErrors:
    @pytest.mark.asyncio
    async def test_sidrapy_value_error_retries_as_source_unavailable(self):
        max_retries = constants.HTTPSettings().max_retries
        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra,
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError, match="Service Unavailable") as exc_info,
        ):
            mock_sidra.side_effect = ValueError("<html>Service Unavailable")
            await client.fetch_sidra(table_code="5457")

        assert mock_sidra.call_count == max_retries
        assert len(sleep_calls) == max_retries - 1
        assert exc_info.value.url.endswith("/values/t/5457")

    @pytest.mark.asyncio
    async def test_sidrapy_value_error_then_success(self):
        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra,
            patch(RETRY_SLEEP, side_effect=track_sleep),
        ):
            mock_sidra.side_effect = [
                ValueError("<html>Service Unavailable"),
                pd.DataFrame({"V": ["100"]}),
            ]
            result = await client.fetch_sidra(table_code="5457")

        assert result["V"].tolist() == ["100"]
        assert mock_sidra.call_count == 2
        assert len(sleep_calls) == 1

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
    async def test_header_n_preserves_all_rows(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame({"V": ["100", "200"]})
            result = await client.fetch_sidra(table_code="5457", header="n")
            assert len(result) == 2
            assert result["V"].iloc[0] == "100"
            assert result["V"].iloc[1] == "200"

    @pytest.mark.asyncio
    async def test_header_y_preserves_all_rows(self):
        with patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra:
            mock_sidra.return_value = pd.DataFrame({"V": ["Valor", "100", "200"]})
            result = await client.fetch_sidra(table_code="5457", header="y")
            assert len(result) == 3


class TestSidraAsyncNonBlocking:
    @pytest.mark.asyncio
    async def test_sidrapy_runs_in_thread(self):
        with (
            patch("agrobr.ibge.client.sidrapy.get_table") as mock_sidra,
            patch("agrobr.ibge.client.asyncio.to_thread", wraps=asyncio.to_thread) as mock_thread,
        ):
            mock_sidra.return_value = pd.DataFrame({"V": ["header", "100"]})
            await client.fetch_sidra(table_code="1234")
            mock_thread.assert_called_once()


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
