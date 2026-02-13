"""Testes de resiliência HTTP para agrobr.bcb.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.bcb import client
from agrobr.exceptions import SourceUnavailableError

RETRY_SLEEP = "agrobr.http.retry.asyncio.sleep"


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {"value": []}
    resp.headers = {}
    resp.url = "https://olinda.bcb.gov.br/test"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestBcbTimeout:
    @pytest.mark.asyncio
    async def test_timeout_propagates_immediately(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("read timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert mock_client.get.call_count == 1


class TestBcbHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert mock_client.get.call_count == client.BCB_MAX_RETRIES

    @pytest.mark.asyncio
    async def test_http_429_retries(self):
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, {"value": [{"id": 1}]})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._fetch_odata("CusteioRegiaoUFProduto")

        assert result["value"] == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")


class TestBcbEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_value_list(self):
        resp = _mock_response(200, {"value": []})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio")

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_value_key(self):
        resp = _mock_response(200, {"odata.metadata": "..."})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio")

        assert result == []


class TestBcbRetry:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        sleep_calls: list[float] = []

        async def track_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert len(sleep_calls) == client.BCB_MAX_RETRIES - 1
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestBcbFetchCreditoRural:
    @pytest.mark.asyncio
    async def test_invalid_finalidade_raises(self):
        with pytest.raises(ValueError, match="Finalidade inválida"):
            await client.fetch_credito_rural(finalidade="invalida")

    @pytest.mark.asyncio
    async def test_client_side_filtering_safra(self):
        records = [
            {"AnoEmissao": "2023", "nomeProduto": "SOJA", "cdEstado": "51"},
            {"AnoEmissao": "2024", "nomeProduto": "SOJA", "cdEstado": "51"},
        ]
        resp = _mock_response(200, {"value": records})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio", safra_sicor="2023/2024")

        assert all(r["AnoEmissao"] == "2023" for r in result)


class TestBcbFallback:
    @pytest.mark.asyncio
    async def test_odata_fails_tries_bigquery(self):
        with (
            patch("agrobr.bcb.client.fetch_credito_rural", new_callable=AsyncMock) as mock_odata,
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                new_callable=AsyncMock,
            ) as mock_bq,
        ):
            mock_odata.side_effect = SourceUnavailableError(
                source="bcb", url="test", last_error="timeout"
            )
            mock_bq.return_value = [{"id": 1}]
            records, source = await client.fetch_credito_rural_with_fallback()

            assert source == "bigquery"
            assert records == [{"id": 1}]
