"""Testes para agrobr.alt.antt_pedagio.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.alt.antt_pedagio.client import (
    _get_ckan_resources,
    _match_pracas_resource,
    _match_trafego_resource,
    download_csv,
    fetch_pracas,
    fetch_trafego,
    fetch_trafego_anos,
)
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import make_mock_async_client, make_mock_response

# ============================================================================
# Resource matching
# ============================================================================


class TestMatchTrafegoResource:
    def test_match_by_name(self):
        resources = [
            {
                "id": "abc",
                "name": "trafego_2023.csv",
                "url": "https://example.com/2023.csv",
                "format": "CSV",
            },
            {
                "id": "def",
                "name": "trafego_2024.csv",
                "url": "https://example.com/2024.csv",
                "format": "CSV",
            },
        ]
        assert _match_trafego_resource(resources, 2023) == "https://example.com/2023.csv"
        assert _match_trafego_resource(resources, 2024) == "https://example.com/2024.csv"

    def test_match_by_url(self):
        resources = [
            {
                "id": "abc",
                "name": "volume data",
                "url": "https://example.com/volume_2023.csv",
                "format": "CSV",
            },
        ]
        assert _match_trafego_resource(resources, 2023) == "https://example.com/volume_2023.csv"

    def test_no_match(self):
        resources = [
            {
                "id": "abc",
                "name": "trafego_2023.csv",
                "url": "https://example.com/2023.csv",
                "format": "CSV",
            },
        ]
        assert _match_trafego_resource(resources, 2020) is None

    def test_empty_resources(self):
        assert _match_trafego_resource([], 2023) is None


class TestMatchPracasResource:
    def test_match_csv(self):
        resources = [
            {
                "id": "abc",
                "name": "cadastro.csv",
                "url": "https://example.com/cadastro.csv",
                "format": "CSV",
            },
        ]
        assert _match_pracas_resource(resources) == "https://example.com/cadastro.csv"

    def test_fallback_first(self):
        resources = [
            {
                "id": "abc",
                "name": "cadastro.xlsx",
                "url": "https://example.com/cadastro.xlsx",
                "format": "XLSX",
            },
        ]
        assert _match_pracas_resource(resources) == "https://example.com/cadastro.xlsx"

    def test_empty_resources(self):
        assert _match_pracas_resource([]) is None


# ============================================================================
# HTTP tests with mocks
# ============================================================================


class TestDownloadCsv:
    @pytest.mark.asyncio
    async def test_download_200(self):
        content = b"col1;col2\n" + b"val1;val2\n" * 15
        mock_response = make_mock_response(200, content=content)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                result = await download_csv("https://example.com/test.csv")

        assert result == content

    @pytest.mark.asyncio
    async def test_download_raises_on_error(self):
        mock_response = make_mock_response(500)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(httpx.HTTPStatusError):
                    await download_csv("https://example.com/test.csv")


class TestFetchTrafego:
    @pytest.mark.asyncio
    async def test_fetch_trafego_success(self):
        resources = [
            {
                "id": "abc",
                "name": "2023.csv",
                "url": "https://example.com/2023.csv",
                "format": "CSV",
            },
        ]
        content = b"concessionaria;praca;mes_ano\nTest;Test;01/01/2023"

        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = resources
            with patch(
                "agrobr.alt.antt_pedagio.client.download_csv", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = content
                result = await fetch_trafego(2023)

        assert result == content

    @pytest.mark.asyncio
    async def test_fetch_trafego_not_found(self):
        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = []
            with pytest.raises(ValueError, match="nao encontrado"):
                await fetch_trafego(2023)


class TestFetchTrafegoAnos:
    @pytest.mark.asyncio
    async def test_fetch_multiple_anos(self):
        resources = [
            {"id": "a", "name": "2022.csv", "url": "https://example.com/2022.csv", "format": "CSV"},
            {"id": "b", "name": "2023.csv", "url": "https://example.com/2023.csv", "format": "CSV"},
        ]
        content = b"data;volume\n01/01/2023;100"

        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = resources
            with patch(
                "agrobr.alt.antt_pedagio.client.download_csv", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = content
                result = await fetch_trafego_anos([2022, 2023])

        assert len(result) == 2
        assert result[0][0] == 2022
        assert result[1][0] == 2023

    @pytest.mark.asyncio
    async def test_skips_missing_anos(self):
        resources = [
            {"id": "a", "name": "2023.csv", "url": "https://example.com/2023.csv", "format": "CSV"},
        ]
        content = b"data;volume\n01/01/2023;100"

        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = resources
            with patch(
                "agrobr.alt.antt_pedagio.client.download_csv", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = content
                result = await fetch_trafego_anos([2020, 2023])

        assert len(result) == 1
        assert result[0][0] == 2023


class TestFetchPracas:
    @pytest.mark.asyncio
    async def test_fetch_pracas_success(self):
        resources = [
            {
                "id": "abc",
                "name": "cadastro.csv",
                "url": "https://example.com/cadastro.csv",
                "format": "CSV",
            },
        ]
        content = b"concessionaria;praca_de_pedagio;rodovia;uf\nTest;Test;BR-101;SP"

        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = resources
            with patch(
                "agrobr.alt.antt_pedagio.client.download_csv", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = content
                result = await fetch_pracas()

        assert result == content

    @pytest.mark.asyncio
    async def test_fetch_pracas_not_found(self):
        with patch(
            "agrobr.alt.antt_pedagio.client._get_ckan_resources", new_callable=AsyncMock
        ) as mock_ckan:
            mock_ckan.return_value = []
            with pytest.raises(ValueError, match="nao encontrado"):
                await fetch_pracas()


# ============================================================================
# _get_ckan_resources (lines 23-67)
# ============================================================================


class TestGetCkanResources:
    @pytest.mark.asyncio
    async def test_valid_response_extracts_resources(self):
        json_data = {
            "result": {
                "resources": [
                    {
                        "id": "r1",
                        "name": "volume_2023.csv",
                        "url": "https://example.com/2023.csv",
                        "format": "CSV",
                    },
                    {
                        "id": "r2",
                        "name": "volume_2024.csv",
                        "url": "https://example.com/2024.csv",
                        "format": "CSV",
                    },
                ]
            }
        }
        mock_response = make_mock_response(200, json_data=json_data)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                resources = await _get_ckan_resources("volume-trafego-praca-pedagio")

        assert len(resources) == 2
        assert resources[0]["id"] == "r1"
        assert resources[0]["name"] == "volume_2023.csv"
        assert resources[1]["url"] == "https://example.com/2024.csv"

    @pytest.mark.asyncio
    async def test_missing_result_key_raises(self):
        json_data = {"error": "not found"}
        mock_response = make_mock_response(200, json_data=json_data)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="missing 'result' key"):
                    await _get_ckan_resources("some-slug")

    @pytest.mark.asyncio
    async def test_empty_resources_list(self):
        json_data = {"result": {"resources": []}}
        mock_response = make_mock_response(200, json_data=json_data)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                resources = await _get_ckan_resources("some-slug")

        assert resources == []

    @pytest.mark.asyncio
    async def test_resource_fields_default_to_empty_string(self):
        json_data = {"result": {"resources": [{"extra_field": "ignored"}]}}
        mock_response = make_mock_response(200, json_data=json_data)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                resources = await _get_ckan_resources("some-slug")

        assert resources[0] == {"id": "", "name": "", "url": "", "format": ""}


class TestDownloadCsvSmall:
    @pytest.mark.asyncio
    async def test_small_csv_raises_source_unavailable(self):
        content = b"tiny"
        mock_response = make_mock_response(200, content=content)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="too small"):
                    await download_csv("https://example.com/test.csv")


# ============================================================================
# WAF / portal outage scenarios (resilience)
# ============================================================================


class TestGetCkanResourcesWafHtml:
    @pytest.mark.asyncio
    async def test_html_response_raises_source_unavailable(self):
        html = (
            "<html><head><title>Request Rejected</title></head><body>"
            "The requested URL was rejected. Please consult with your administrator.<br><br>"
            "Your support ID is: 4431997519019677401<br><br>"
            "<a href='javascript:history.back();'>[Go Back]</a></body></html>"
        )
        mock_response = make_mock_response(
            200,
            text=html,
            headers={"content-type": "text/html; charset=UTF-8"},
        )
        mock_response.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError) as exc_info:
                    await _get_ckan_resources("praca-de-pedagio")

        msg = str(exc_info.value)
        assert "not JSON" in msg
        assert "WAF block" in msg or "outage" in msg
        assert "text/html" in msg
        assert "Request Rejected" in msg

    @pytest.mark.asyncio
    async def test_truncated_json_raises_source_unavailable(self):
        mock_response = make_mock_response(200, text='{"result": {"resources":')
        mock_response.json.side_effect = ValueError("Unterminated string")

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="not JSON"):
                    await _get_ckan_resources("praca-de-pedagio")


class TestDownloadCsvWafHtml:
    @pytest.mark.asyncio
    async def test_html_body_raises_source_unavailable(self):
        html = (
            b"<html><head><title>Request Rejected</title></head><body>"
            b"The requested URL was rejected. Please consult with your administrator.<br><br>"
            b"Your support ID is: 4431997519019677401<br><br>"
            b"<a href='javascript:history.back();'>[Go Back]</a></body></html>"
        )
        mock_response = make_mock_response(200, content=html)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError) as exc_info:
                    await download_csv("https://example.com/test.csv")

        msg = str(exc_info.value)
        assert "HTML instead of CSV" in msg
        assert "Request Rejected" in msg

    @pytest.mark.asyncio
    async def test_doctype_html_raises_source_unavailable(self):
        html = b"<!DOCTYPE html>\n<html><body>error page placeholder body content...</body></html>"
        mock_response = make_mock_response(200, content=html)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="HTML instead of CSV"):
                    await download_csv("https://example.com/test.csv")

    @pytest.mark.asyncio
    async def test_leading_whitespace_before_html_still_detected(self):
        html = b"   \n\t<html><body>blocked page placeholder content padding to bypass size...</body></html>"
        mock_response = make_mock_response(200, content=html)

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = make_mock_async_client()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "agrobr.alt.antt_pedagio.client.retry_on_status", new_callable=AsyncMock
            ) as mock_retry:
                mock_retry.return_value = mock_response
                with pytest.raises(SourceUnavailableError, match="HTML instead of CSV"):
                    await download_csv("https://example.com/test.csv")
