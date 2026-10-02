from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.cftc import client as cftc_client
from agrobr.exceptions import ParseError
from tests.helpers import (
    make_mock_async_client,
    make_mock_response,
)

COT_SAMPLE = [
    {"report_date_as_yyyy_mm_dd": "2026-06-02T00:00:00.000", "cftc_contract_market_code": "005602"},
]


def _client_with(resp):
    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(return_value=resp)
    return mock_client


class TestFetchCotQuery:
    @pytest.mark.asyncio
    async def test_start_end_no_where(self):
        mock_client = _client_with(make_mock_response(200, json_data=COT_SAMPLE))

        with patch("agrobr.cftc.client.httpx.AsyncClient", return_value=mock_client):
            await cftc_client.fetch_cot(["005602"], start="2026-01-01", end=date(2026, 6, 1))

        where = mock_client.get.call_args[1]["params"]["$where"]
        assert "report_date_as_yyyy_mm_dd >= '2026-01-01T00:00:00.000'" in where
        assert "report_date_as_yyyy_mm_dd <= '2026-06-01T00:00:00.000'" in where


class TestFetchCotErrors:
    @pytest.mark.asyncio
    async def test_resposta_vazia_preserva_corpo_e_consulta(self):
        mock_client = _client_with(make_mock_response(200, json_data=[]))

        with patch("agrobr.cftc.client.httpx.AsyncClient", return_value=mock_client):
            registros, url, corpo = await cftc_client.fetch_cot(["005602"])

        assert registros == []
        assert url == str(mock_client.get.return_value.url)
        assert corpo == mock_client.get.return_value.content

    @pytest.mark.parametrize("resposta", [{}, {"data": []}, {"error": "consulta inválida"}])
    async def test_envelope_invalido_levanta_parse_error(self, resposta):
        mock_client = _client_with(make_mock_response(200, json_data=resposta))

        with (
            patch("agrobr.cftc.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ParseError, match="lista"),
        ):
            await cftc_client.fetch_cot(["005602"])
