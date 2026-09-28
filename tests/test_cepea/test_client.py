from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.cepea import client
from agrobr.cepea.client import FetchResult
from agrobr.constants import _CEPEA_ENDPOINTS
from agrobr.exceptions import SourceUnavailableError
from tests import helpers
from tests.helpers import make_mock_response


@pytest.fixture(autouse=True)
def _reset_circuit_breaker():
    client._circuit_state.clear()
    client._use_browser = False
    client._use_alternative_source = False
    yield
    client._circuit_state.clear()
    client._use_browser = False
    client._use_alternative_source = True


class TestCepeaCircuitBreaker:
    def test_circuit_opens_on_cloudflare(self):
        endpoint = _CEPEA_ENDPOINTS[0]
        client._open_circuit(endpoint)
        assert client._is_circuit_open(endpoint) is True

    def test_circuit_resets_after_timeout(self):
        endpoint = _CEPEA_ENDPOINTS[0]
        client._open_circuit(endpoint)
        client._circuit_state[endpoint] = time.monotonic() - client._CIRCUIT_RESET_SECONDS - 1
        assert client._is_circuit_open(endpoint) is False

    @pytest.mark.asyncio
    async def test_circuit_open_skips_httpx(self):
        for ep in _CEPEA_ENDPOINTS:
            client._open_circuit(ep)

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

            mock_httpx.assert_not_called()


class TestCepeaFallbackChain:
    @pytest.mark.asyncio
    async def test_browser_falha_sem_fonte_alternativa_informa_o_erro(self):
        client._use_browser = True
        client._use_alternative_source = False
        with (
            patch("agrobr.cepea.client._try_endpoints", new_callable=AsyncMock, return_value=None),
            patch(
                "agrobr.cepea.client._fetch_with_browser",
                new_callable=AsyncMock,
                side_effect=Exception("navegador caiu"),
            ),
            pytest.raises(SourceUnavailableError, match="Last error: navegador caiu"),
        ):
            await client.fetch_indicador_page("soja")

    @pytest.mark.asyncio
    async def test_httpx_fail_falls_to_browser(self):
        client._use_browser = True
        client._use_alternative_source = False

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            mock_httpx.side_effect = httpx.HTTPError("failed")
            with patch(
                "agrobr.cepea.client._fetch_with_browser", new_callable=AsyncMock
            ) as mock_browser:
                mock_browser.return_value = FetchResult("<html>browser</html>", "browser")
                result = await client.fetch_indicador_page("soja")

        assert result.html == "<html>browser</html>"
        assert result.source == "browser"

    @pytest.mark.asyncio
    async def test_all_methods_fail_raises(self):
        client._use_browser = True
        client._use_alternative_source = True

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            mock_httpx.side_effect = httpx.HTTPError("h")
            with patch(
                "agrobr.cepea.client._fetch_with_browser", new_callable=AsyncMock
            ) as mock_browser:
                mock_browser.side_effect = Exception("b")
                with patch(
                    "agrobr.cepea.client._fetch_with_alternative_source", new_callable=AsyncMock
                ) as mock_alt:
                    mock_alt.side_effect = Exception("a")
                    with pytest.raises(SourceUnavailableError, match="All fetch methods"):
                        await client.fetch_indicador_page("soja")

    @pytest.mark.asyncio
    async def test_force_alternative_skips_httpx(self):
        with patch(
            "agrobr.cepea.client._fetch_with_alternative_source", new_callable=AsyncMock
        ) as mock_alt:
            mock_alt.return_value = FetchResult("<html>NA data</html>", "noticias_agricolas")
            result = await client.fetch_indicador_page("soja", force_alternative=True)

        assert result.html == "<html>NA data</html>"
        assert result.source == "noticias_agricolas"


class TestCepeaEncoding:
    @pytest.mark.asyncio
    async def test_encoding_handled_by_decode_content(self):
        iso_content = "Preço médio café".encode("iso-8859-1")
        resp = make_mock_response(200, content=iso_content, charset_encoding="utf-8")

        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.cepea.client.decode_content") as mock_decode:
                mock_decode.return_value = ("Preço médio café", "iso-8859-1")
                result = await client._fetch_with_httpx("https://test.cepea.esalq.usp.br", {})

        assert result.html == "Preço médio café"
        assert result.source == "cepea"
        mock_decode.assert_called_once_with(iso_content, declared_encoding="utf-8", source="cepea")


class TestCepeaEndpointRotation:
    @pytest.mark.asyncio
    async def test_first_fails_second_succeeds(self):
        call_count = 0

        async def _side_effect(url: str, _headers: dict[str, str]) -> FetchResult:
            nonlocal call_count
            call_count += 1
            if url.startswith(_CEPEA_ENDPOINTS[0]):
                raise httpx.HTTPStatusError(
                    "403 Forbidden", request=MagicMock(), response=MagicMock(status_code=403)
                )
            return FetchResult("<html>second</html>", "cepea")

        with helpers.collect_failures() as check:
            with check("resposta"):
                with patch(
                    "agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock
                ) as mock_httpx:
                    mock_httpx.side_effect = _side_effect
                    result = await client.fetch_indicador_page("soja")
                assert result.html == "<html>second</html>"
            with check("rotação"):
                assert call_count == 2
