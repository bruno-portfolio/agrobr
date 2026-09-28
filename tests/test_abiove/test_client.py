"""Testes de resiliência HTTP para agrobr.abiove.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.abiove import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    make_mock_async_client,
    make_mock_response,
)


class TestAbioveEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_body_raises_source_unavailable(self):
        resp = make_mock_response(200, content=b"", url="https://abiove.org.br/test.xlsx")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="too small"),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")
