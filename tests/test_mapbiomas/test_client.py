from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.mapbiomas.client import _build_xlsx_url


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

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.content = b"x" * 10000
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

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

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

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

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.content = b"tiny"
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.mapbiomas.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.mapbiomas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="too small"),
        ):
            await _fetch_url("https://example.com/test")


class TestFetchBiomeStateMunicipality:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        from agrobr.mapbiomas.client import fetch_biome_state_municipality

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.content = b"x" * 10000
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

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
