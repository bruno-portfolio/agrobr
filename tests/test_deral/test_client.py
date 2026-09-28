"""Testes de resiliência HTTP para agrobr.deral.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.deral import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    make_mock_async_client,
    make_mock_response,
)


class TestDeralHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_404_raises_immediately(self):
        resp_404 = make_mock_response(404, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_404)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="404"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count == 1
