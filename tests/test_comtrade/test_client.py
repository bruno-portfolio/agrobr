from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.comtrade import client
from agrobr.comtrade.client import _chunk_period
from agrobr.exceptions import SourceUnavailableError

RETRY_SLEEP = "agrobr.http.retry.asyncio.sleep"

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "comtrade" / "comercio_sample"


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {"data": []}
    resp.headers = {}
    resp.url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestChunkPeriod:
    def test_simple_year_passes_through(self):
        assert _chunk_period("2024", "A") == ["2024"]

    def test_simple_month_passes_through(self):
        assert _chunk_period("202401", "M") == ["202401"]

    def test_monthly_range_3_years(self):
        chunks = _chunk_period("2022-2024", "M")
        assert len(chunks) == 3
        assert "202201" in chunks[0]
        assert "202212" in chunks[0]
        assert "202301" in chunks[1]
        assert "202401" in chunks[2]
        assert "202412" in chunks[2]

    def test_annual_range_over_12(self):
        chunks = _chunk_period("2010-2024", "A")
        assert len(chunks) == 2
        first_years = chunks[0].split(",")
        assert len(first_years) == 12
        assert first_years[0] == "2010"
        assert first_years[-1] == "2021"
        second_years = chunks[1].split(",")
        assert len(second_years) == 3
        assert second_years[0] == "2022"
        assert second_years[-1] == "2024"

    def test_exact_12_months_single_chunk(self):
        chunks = _chunk_period("2024-2024", "M")
        assert len(chunks) == 1
        periods = chunks[0].split(",")
        assert len(periods) == 12

    def test_annual_range_within_12(self):
        chunks = _chunk_period("2020-2024", "A")
        assert len(chunks) == 1
        years = chunks[0].split(",")
        assert len(years) == 5

    def test_no_dash_passthrough(self):
        assert _chunk_period("202401,202402", "M") == ["202401,202402"]


class TestApiKey:
    def test_no_key_returns_none(self):
        with patch.dict("os.environ", {}, clear=True):
            assert client._get_api_key(None) is None

    def test_explicit_key_used(self):
        assert client._get_api_key("my-key") == "my-key"

    def test_env_var_used(self):
        with patch.dict("os.environ", {"AGROBR_COMTRADE_API_KEY": "env-key"}):
            assert client._get_api_key(None) == "env-key"

    def test_max_records_with_key(self):
        assert client._max_records("key") == 100_000

    def test_max_records_guest(self):
        assert client._max_records(None) == 500


class TestBuildHeaders:
    def test_headers_with_key(self):
        h = client._build_headers("my-key")
        assert h["Ocp-Apim-Subscription-Key"] == "my-key"
        assert "Accept" in h

    def test_headers_without_key(self):
        h = client._build_headers(None)
        assert "Ocp-Apim-Subscription-Key" not in h
        assert "Accept" in h


class TestFetchTradeData:
    @pytest.mark.asyncio
    async def test_success_returns_records(self):
        golden = json.loads(GOLDEN_DIR.joinpath("response.json").read_text())
        resp = _mock_response(200, golden)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client):
            records, url = await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201", "1507", "2304"],
                flow="X",
                period="2024",
                freq="A",
            )

        assert len(records) == 3
        assert records[0]["cmdCode"] == "1201"

    @pytest.mark.asyncio
    async def test_401_raises(self):
        resp = _mock_response(401)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="401"),
        ):
            await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201"],
                flow="X",
                period="2024",
            )

    @pytest.mark.asyncio
    async def test_500_retries(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201"],
                flow="X",
                period="2024",
            )

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_empty_response(self):
        resp = _mock_response(200, {"data": []})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client):
            records, url = await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201"],
                flow="X",
                period="2024",
            )

        assert records == []

    @pytest.mark.asyncio
    async def test_chunked_period_multiple_requests(self):
        golden = json.loads(GOLDEN_DIR.joinpath("response.json").read_text())
        resp = _mock_response(200, golden)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client):
            records, url = await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201"],
                flow="X",
                period="2022-2024",
                freq="M",
            )

        assert mock_client.get.call_count == 3
        assert len(records) == 9

    @pytest.mark.asyncio
    async def test_guest_mode_max_records(self):
        resp = _mock_response(200, {"data": []})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comtrade.client.httpx.AsyncClient", return_value=mock_client),
            patch.dict("os.environ", {}, clear=True),
        ):
            await client.fetch_trade_data(
                reporter=76,
                partner=156,
                hs_codes=["1201"],
                flow="X",
                period="2024",
            )

        call_kwargs = mock_client.get.call_args
        params = call_kwargs.kwargs.get("params", call_kwargs[1].get("params", {}))
        assert params["maxRecords"] == "500"
