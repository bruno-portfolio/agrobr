from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.conab.ceasa import client
from agrobr.conab.ceasa.models import CDA_PROHORT, PENTAHO_BASE, QUERY_PRECOS
from agrobr.exceptions import SourceUnavailableError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _golden_precos_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("precos_response.json").read_text(encoding="utf-8"))


def _golden_ceasas_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("ceasas_response.json").read_text(encoding="utf-8"))


class TestFetchPrecos:
    @pytest.mark.asyncio
    async def test_returns_dict_and_url(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _golden_precos_json()
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.conab.ceasa.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            data, url = await client.fetch_precos()

        assert isinstance(data, dict)
        assert "resultset" in data
        assert PENTAHO_BASE in url

    @pytest.mark.asyncio
    async def test_404_raises_source_unavailable(self):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.conab.ceasa.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="conab_ceasa"),
        ):
            await client.fetch_precos()

    @pytest.mark.asyncio
    async def test_500_raises_source_unavailable(self):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "Server Error", request=MagicMock(), response=mock_response
            )
        )

        with (
            patch(
                "agrobr.conab.ceasa.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="conab_ceasa"),
        ):
            await client.fetch_precos()


class TestFetchCeasas:
    @pytest.mark.asyncio
    async def test_returns_dict_and_url(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _golden_ceasas_json()
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.conab.ceasa.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            data, url = await client.fetch_ceasas()

        assert isinstance(data, dict)
        assert "resultset" in data
        assert "MDXceasa" in url


class TestBuildUrl:
    def test_contains_base_and_params(self):
        url = client._build_url(CDA_PROHORT, QUERY_PRECOS)
        assert PENTAHO_BASE in url
        assert "dataAccessId=MDXProdutoPreco" in url
        assert "path=%2Fhome%2FPROHORT%2FprecoDia.cda" in url
        assert "userid=pentaho" in url
        assert "password=password" in url

    def test_headers_contain_user_agent(self):
        from agrobr.http.user_agents import UserAgentRotator

        headers = UserAgentRotator.get_headers(source="conab_ceasa")
        assert "User-Agent" in headers
        assert "Mozilla" in headers["User-Agent"]
