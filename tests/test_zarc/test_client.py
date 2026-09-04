from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.zarc.client import discover_resources, download_csv, fetch_tabua_risco
from tests.helpers import make_mock_async_client, make_mock_response


class TestDiscoverResources:
    @pytest.mark.asyncio
    async def test_discover_resources_ok(self):
        ckan_json = {
            "success": True,
            "result": {
                "resources": [
                    {
                        "id": "r1",
                        "name": "Safra 2025/2026",
                        "url": "https://x/25.csv",
                        "format": "CSV",
                    },
                    {"id": "r2", "name": "Safra perene", "url": "https://x/p.csv", "format": "CSV"},
                ]
            },
        }
        mock_response = make_mock_response(200, json_data=ckan_json)

        with patch("agrobr.zarc.client.httpx.AsyncClient") as mock_cls:
            mock_client = make_mock_async_client()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("agrobr.zarc.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
                mock_retry.return_value = mock_response
                result = await discover_resources()

        assert len(result) == 2
        assert result[0]["name"] == "Safra 2025/2026"
        assert result[1]["url"] == "https://x/p.csv"

    @pytest.mark.asyncio
    async def test_discover_resources_invalid_response(self):
        mock_response = make_mock_response(200, json_data={"error": "not found"})

        with patch("agrobr.zarc.client.httpx.AsyncClient") as mock_cls:
            mock_client = make_mock_async_client()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("agrobr.zarc.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="missing 'result'"):
                    await discover_resources()


class TestDownloadCsv:
    @pytest.mark.asyncio
    async def test_download_csv_ok(self):
        content = b"col1;col2\n" + b"val1;val2\n" * 15
        mock_response = make_mock_response(200, content=content)

        with patch("agrobr.zarc.client.httpx.AsyncClient") as mock_cls:
            mock_client = make_mock_async_client()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("agrobr.zarc.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
                mock_retry.return_value = mock_response
                result = await download_csv("https://x/test.csv")

        assert result == content

    @pytest.mark.asyncio
    async def test_download_csv_too_small(self):
        mock_response = make_mock_response(200, content=b"tiny")

        with patch("agrobr.zarc.client.httpx.AsyncClient") as mock_cls:
            mock_client = make_mock_async_client()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("agrobr.zarc.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="muito pequeno"):
                    await download_csv("https://x/test.csv")


class TestFetchTabuaRisco:
    @pytest.mark.asyncio
    async def test_fetch_safra_not_found(self):
        resources = [
            {"id": "r1", "name": "Safra 2025/2026", "url": "https://x/25.csv", "format": "CSV"},
        ]

        with patch(
            "agrobr.zarc.client.discover_resources", new_callable=AsyncMock
        ) as mock_discover:
            mock_discover.return_value = resources
            with pytest.raises(InvalidParameterError, match="não encontrada"):
                await fetch_tabua_risco("2020/2021")
