"""Testes para agrobr.alt.anp_diesel.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.alt.anp_diesel import client
from tests.helpers import make_mock_async_client, make_mock_response


@pytest.fixture
def _mock_retry():
    async def _passthrough(fn, **_kw):
        return await fn()

    with patch("agrobr.alt.anp_diesel.client.retry_on_status", side_effect=_passthrough) as mock:
        yield mock


FAKE_CSV_BYTES = b"ANO;MES;GRANDE REGIAO;UNIDADE DA FEDERACAO;PRODUTO;VENDAS\n" + b"x" * 200


class TestFetchVendasM3:
    @pytest.mark.asyncio
    async def test_fetch_ok(self, _mock_retry):
        resp = make_mock_response(200, content=FAKE_CSV_BYTES)
        with patch("agrobr.alt.anp_diesel.client.httpx.AsyncClient") as mock_client:
            instance = make_mock_async_client()
            instance.get = AsyncMock(return_value=resp)
            mock_client.return_value = instance

            result = await client.fetch_vendas_m3()
            assert result == FAKE_CSV_BYTES
