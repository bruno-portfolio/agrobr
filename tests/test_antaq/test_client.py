"""Testes de resiliência HTTP para agrobr.antaq.client."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.antaq import client
from agrobr.exceptions import SourceUnavailableError

RETRY_SLEEP = "agrobr.http.retry.asyncio.sleep"


def _make_zip(files: dict[str, str]) -> bytes:
    """Cria ZIP em memória com conteúdo dado."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content.encode("utf-8-sig"))
    return buf.getvalue()


def _mock_response(
    status_code: int = 200,
    content: bytes = b"",
) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.headers = {}
    resp.url = "https://web3.antaq.gov.br/ea/txt/2024.zip"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestDownloadZip:
    @pytest.mark.asyncio
    async def test_success(self):
        zip_bytes = _make_zip({"test.txt": "hello"})
        resp = _mock_response(200, zip_bytes)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.antaq.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._download_zip("https://web3.antaq.gov.br/ea/txt/2024.zip")

        assert result == zip_bytes

    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.antaq.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client._download_zip("https://web3.antaq.gov.br/ea/txt/2024.zip")

    @pytest.mark.asyncio
    async def test_500_retries_then_raises(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.antaq.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._download_zip("https://web3.antaq.gov.br/ea/txt/2024.zip")

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_429_retries_then_succeeds(self):
        zip_bytes = _make_zip({"test.txt": "ok"})
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, zip_bytes)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.antaq.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._download_zip("https://web3.antaq.gov.br/ea/txt/2024.zip")

        assert result == zip_bytes


class TestExtractTxtFromZip:
    def test_extracts_utf8_sig(self):
        content = "IDAtracacao;Porto\n1;Santos"
        zip_bytes = _make_zip({"2024Atracacao.txt": content})

        result = client._extract_txt_from_zip(zip_bytes, "2024Atracacao.txt")

        assert "IDAtracacao" in result
        assert "Santos" in result

    def test_missing_file_raises_keyerror(self):
        zip_bytes = _make_zip({"other.txt": "data"})

        with pytest.raises(KeyError):
            client._extract_txt_from_zip(zip_bytes, "missing.txt")

    def test_strips_bom(self):
        content = "colA;colB\n1;2"
        zip_bytes = _make_zip({"file.txt": content})

        result = client._extract_txt_from_zip(zip_bytes, "file.txt")

        assert not result.startswith("\ufeff")
        assert result.startswith("colA")


class TestListZipContents:
    def test_lists_files(self):
        zip_bytes = _make_zip(
            {
                "2024Atracacao.txt": "a",
                "2024Carga.txt": "b",
            }
        )

        names = client.list_zip_contents(zip_bytes)

        assert "2024Atracacao.txt" in names
        assert "2024Carga.txt" in names
        assert len(names) == 2


class TestFetchAnoZip:
    @pytest.mark.asyncio
    async def test_builds_correct_url(self):
        zip_bytes = _make_zip({"test.txt": "ok"})

        with patch.object(
            client, "_download_zip", new_callable=AsyncMock, return_value=zip_bytes
        ) as mock:
            result = await client.fetch_ano_zip(2024)

        mock.assert_called_once_with("https://web3.antaq.gov.br/ea/txt/2024.zip")
        assert result == zip_bytes


class TestFetchMercadoriaZip:
    @pytest.mark.asyncio
    async def test_builds_correct_url(self):
        zip_bytes = _make_zip({"Mercadoria.txt": "ok"})

        with patch.object(
            client, "_download_zip", new_callable=AsyncMock, return_value=zip_bytes
        ) as mock:
            result = await client.fetch_mercadoria_zip()

        mock.assert_called_once_with("https://web3.antaq.gov.br/ea/txt/Mercadoria.zip")
        assert result == zip_bytes


class TestExtractHelpers:
    def test_extract_atracacao(self):
        zip_bytes = _make_zip({"2024Atracacao.txt": "IDAtracacao;Porto\n1;Santos"})

        result = client.extract_atracacao(zip_bytes, 2024)

        assert "IDAtracacao" in result

    def test_extract_carga(self):
        zip_bytes = _make_zip({"2024Carga.txt": "IDCarga;IDAtracacao\n1;1"})

        result = client.extract_carga(zip_bytes, 2024)

        assert "IDCarga" in result

    def test_extract_mercadoria(self):
        zip_bytes = _make_zip({"Mercadoria.txt": "CDMercadoria;Mercadoria\n0901;Cafe"})

        result = client.extract_mercadoria(zip_bytes)

        assert "CDMercadoria" in result
