from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from tests.helpers import make_mock_async_client, make_mock_response


class TestFetchBiomeState:
    @pytest.mark.asyncio
    async def test_404_raises(self):
        from agrobr.mapbiomas.client import _fetch_bundle

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
            await _fetch_bundle("https://example.com/test")
