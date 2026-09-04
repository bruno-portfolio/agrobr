from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.mapbiomas.client import _build_xlsx_url
from tests.helpers import make_mock_async_client, make_mock_response


class TestBuildXlsxUrl:
    def test_biome_state(self):
        url = _build_xlsx_url("BIOME_STATE")
        assert "format=original" in url

    def test_biome_state_municipality(self):
        url = _build_xlsx_url("BIOME_STATE_MUNICIPALITY")
        assert "format=original" not in url

    def test_case_insensitive_municipality(self):
        url = _build_xlsx_url("biome_state_municipality")
        assert "format=original" not in url


class TestFetchBiomeState:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        from agrobr.mapbiomas.client import fetch_biome_state

        mock_response = make_mock_response(200, content=b"PK\x03\x04" + b"x" * 9996)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch("agrobr.mapbiomas.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.mapbiomas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
        ):
            content, url = await fetch_biome_state()
        assert len(content) >= 10000

    @pytest.mark.asyncio
    async def test_404_raises(self):
        from agrobr.mapbiomas.client import _fetch_url

        mock_response = make_mock_response(404)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch("agrobr.mapbiomas.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.mapbiomas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await _fetch_url("https://example.com/test")

    @pytest.mark.asyncio
    async def test_too_small_raises(self):
        from agrobr.mapbiomas.client import _fetch_url

        mock_response = make_mock_response(200, content=b"tiny")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch("agrobr.mapbiomas.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.mapbiomas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="muito pequeno"),
        ):
            await _fetch_url("https://example.com/test")


class TestFetchBiomeStateMunicipality:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        from agrobr.mapbiomas.client import fetch_biome_state_municipality

        mock_response = make_mock_response(200, content=b"PK\x03\x04" + b"x" * 9996)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch("agrobr.mapbiomas.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.mapbiomas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
        ):
            content, url = await fetch_biome_state_municipality()
        assert len(content) >= 10000
