"""Testes de resiliência HTTP para agrobr.comexstat.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.comexstat import client
from agrobr.exceptions import SourceUnavailableError

RETRY_SLEEP = "agrobr.http.retry.asyncio.sleep"


def _mock_response(status_code: int = 200, text: str = "col1;col2\nval1;val2") -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = {}
    resp.url = "https://test.gov.br/EXP_2024.csv"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestComexstatTimeout:
    @pytest.mark.asyncio
    async def test_timeout_propagates_immediately(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("read timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client.download_csv("https://test.gov.br/EXP_2024.csv")

        assert mock_client.get.call_count == 1


class TestComexstatHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries_then_fails(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_csv("https://test.gov.br/EXP_2024.csv")

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client.download_csv("https://test.gov.br/EXP_2024.csv")

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        ok_text = (
            "CO_ANO;CO_MES;CO_NCM;CO_PAIS;SG_UF;KG_LIQUIDO;VL_FOB\n"
            + "2024;01;12019010;160;SP;1000;5000\n" * 5
        )
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, ok_text)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client.download_csv("https://test.gov.br/EXP_2024.csv")

        assert result == ok_text


class TestComexstatEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_body_raises_source_unavailable(self):
        resp = _mock_response(200, "")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="too small"),
        ):
            await client.download_csv("https://test.gov.br/EXP_2024.csv")


class TestComexstatRetryBackoff:
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
            patch("agrobr.comexstat.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_csv("https://test.gov.br/EXP_2024.csv")

        assert len(sleep_calls) >= 2
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestComexstatFetchHelpers:
    @pytest.mark.asyncio
    async def test_fetch_exportacao_builds_correct_url(self):
        with patch("agrobr.comexstat.client.download_csv", new_callable=AsyncMock) as mock:
            mock.return_value = "data"
            await client.fetch_exportacao_csv(2024)
            mock.assert_called_once()
            url_arg = mock.call_args[0][0]
            assert "EXP_2024.csv" in url_arg

    @pytest.mark.asyncio
    async def test_fetch_importacao_builds_correct_url(self):
        with patch("agrobr.comexstat.client.download_csv", new_callable=AsyncMock) as mock:
            mock.return_value = "data"
            await client.fetch_importacao_csv(2024)
            url_arg = mock.call_args[0][0]
            assert "IMP_2024.csv" in url_arg
