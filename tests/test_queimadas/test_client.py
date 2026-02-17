"""Testes para agrobr.queimadas.client — fallback em cascata."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.queimadas import client


def _make_zip_with_csv(csv_content: bytes, csv_name: str = "focos.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


def _mock_response(status: int, content: bytes = b"") -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.content = content
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status}", request=MagicMock(), response=resp
        )
    return resp


class TestExtractCsvFromZip:
    def test_extracts_csv(self):
        csv_data = b"lat,lon\n-15.0,-47.0"
        zip_bytes = _make_zip_with_csv(csv_data)
        result = client._extract_csv_from_zip(zip_bytes)
        assert result == csv_data

    def test_extracts_first_csv_from_multi(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "info")
            zf.writestr("data.csv", "lat,lon\n1,2")
            zf.writestr("extra.csv", "x,y\n3,4")
        result = client._extract_csv_from_zip(buf.getvalue())
        assert result == b"lat,lon\n1,2"

    def test_no_csv_raises(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        with pytest.raises(SourceUnavailableError, match="ZIP nao contem"):
            client._extract_csv_from_zip(buf.getvalue())


class TestFetchFocosMensal:
    @pytest.mark.asyncio
    async def test_csv_hit_returns_directly(self):
        csv_data = b"lat,lon\n-15.0,-47.0"
        resp_ok = _mock_response(200, csv_data)

        with patch(
            "agrobr.queimadas.client.retry_on_status", new_callable=AsyncMock, return_value=resp_ok
        ):
            content, url = await client.fetch_focos_mensal(2025, 1)

        assert content == csv_data
        assert "202501.csv" in url

    @pytest.mark.asyncio
    async def test_csv_404_falls_back_to_zip(self):
        csv_data = b"lat,lon\n-15.0,-47.0"
        zip_bytes = _make_zip_with_csv(csv_data)
        resp_404 = _mock_response(404)
        resp_zip = _mock_response(200, zip_bytes)

        call_count = 0

        async def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp_404
            return resp_zip

        with patch("agrobr.queimadas.client.retry_on_status", side_effect=side_effect):
            content, url = await client.fetch_focos_mensal(2023, 6)

        assert content == csv_data
        assert "202306.zip" in url

    @pytest.mark.asyncio
    async def test_csv_and_zip_404_falls_back_to_anual(self):
        csv_data = b"lat,lon\n-15.0,-47.0"
        zip_bytes = _make_zip_with_csv(csv_data, "focos_br_todos-sats_2020.csv")
        resp_404 = _mock_response(404)
        resp_anual = _mock_response(200, zip_bytes)

        call_count = 0

        async def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return resp_404
            return resp_anual

        with patch("agrobr.queimadas.client.retry_on_status", side_effect=side_effect):
            content, url = await client.fetch_focos_mensal(2020, 9)

        assert content == csv_data
        assert "anual" in url
        assert "2020" in url

    @pytest.mark.asyncio
    async def test_all_404_raises(self):
        resp_404 = _mock_response(404)

        with (
            patch(
                "agrobr.queimadas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=resp_404,
            ),
            pytest.raises(SourceUnavailableError, match="Tentativas"),
        ):
            await client.fetch_focos_mensal(1990, 1)
