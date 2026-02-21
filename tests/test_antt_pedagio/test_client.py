"""Testes para agrobr.alt.antt_pedagio.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.alt.antt_pedagio.client import (
    _match_pracas_resource,
    _match_trafego_resource,
    download_csv,
    fetch_pracas,
    fetch_trafego,
    fetch_trafego_anos,
)

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
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.content = content
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
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
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError("500", request=MagicMock(), response=mock_response)
        )

        with patch("agrobr.alt.antt_pedagio.client.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
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
