"""Testes de resiliência HTTP para agrobr.antaq.client."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.antaq import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import RETRY_SLEEP

_ANTAQ_URL = "https://estatistica.antaq.gov.br/ea/txt/2024.zip"


def _make_zip(files: dict[str, str], *, min_size: int = 0) -> bytes:
    """Cria ZIP em memória com conteúdo dado.

    Se *min_size* > 0 e o ZIP for menor, padding é adicionado como
    arquivo extra para ultrapassar o limite.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content.encode("utf-8-sig"))
    data = buf.getvalue()
    if min_size and len(data) < min_size:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
            for name, content in files.items():
                zf.writestr(name, content.encode("utf-8-sig"))
            zf.writestr("_padding.bin", bytes(range(256)) * ((min_size // 256) + 2))
        data = buf.getvalue()
    return data


def _make_requests_response(
    status: int,
    content: bytes = b"",
    *,
    content_type: str | None = None,
    url: str = _ANTAQ_URL,
):
    import requests

    response = requests.Response()
    response.status_code = status
    response._content = content
    response.url = url
    if content_type:
        response.headers["Content-Type"] = content_type
    return response


class TestDownloadZip:
    @pytest.mark.asyncio
    async def test_success(self):
        zip_bytes = _make_zip({"test.txt": "hello"}, min_size=500)
        resp = _make_requests_response(200, zip_bytes)

        with patch("agrobr.antaq.client.requests.get", return_value=resp):
            result = await client._download_zip(_ANTAQ_URL)

        assert result == zip_bytes

    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        import requests

        with (
            patch(
                "agrobr.antaq.client.requests.get",
                side_effect=requests.exceptions.Timeout("timeout"),
            ) as mock_get,
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._download_zip(_ANTAQ_URL)

        assert mock_get.call_count == 3

    @pytest.mark.asyncio
    async def test_500_retries_then_raises(self):
        resp_500 = _make_requests_response(500)

        with (
            patch("agrobr.antaq.client.requests.get", return_value=resp_500) as mock_get,
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._download_zip(_ANTAQ_URL)

        assert mock_get.call_count > 1

    @pytest.mark.asyncio
    async def test_429_retries_then_succeeds(self):
        zip_bytes = _make_zip({"test.txt": "ok"}, min_size=500)
        resp_429 = _make_requests_response(429)
        resp_ok = _make_requests_response(200, zip_bytes)

        with (
            patch("agrobr.antaq.client.requests.get", side_effect=[resp_429, resp_ok]),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._download_zip(_ANTAQ_URL)

        assert result == zip_bytes

    @pytest.mark.asyncio
    async def test_403_nao_retried(self):
        resp_403 = _make_requests_response(403)

        with (
            patch("agrobr.antaq.client.requests.get", return_value=resp_403) as mock_get,
            pytest.raises(SourceUnavailableError, match="HTTPError"),
        ):
            await client._download_zip(_ANTAQ_URL)

        assert mock_get.call_count == 1

    @pytest.mark.asyncio
    async def test_html_challenge_reports_size_type_and_final_url(self):
        html = b"<!DOCTYPE html>\n" + b"x" * 600
        resp = _make_requests_response(
            200,
            html,
            content_type="text/html;charset=utf-8",
            url="https://estatistica.antaq.gov.br/redirected",
        )

        with (
            patch("agrobr.antaq.client.requests.get", return_value=resp),
            pytest.raises(SourceUnavailableError) as exc,
        ):
            await client._download_zip(_ANTAQ_URL)

        message = str(exc.value)
        assert "not a ZIP (missing PK signature)" in message
        assert f"{len(html)} bytes" in message
        assert "text/html;charset=utf-8" in message
        assert "https://estatistica.antaq.gov.br/redirected" in message

    @pytest.mark.asyncio
    async def test_small_valid_zip_reports_size_not_signature(self):
        tiny = b"PK\x03\x04" + b"\x00" * 10
        resp = _make_requests_response(200, tiny, content_type="application/zip")

        with (
            patch("agrobr.antaq.client.requests.get", return_value=resp),
            pytest.raises(SourceUnavailableError) as exc,
        ):
            await client._download_zip(_ANTAQ_URL)

        message = str(exc.value)
        assert "ZIP too small" in message
        assert "missing PK signature" not in message
        assert "application/zip" in message

    @pytest.mark.asyncio
    async def test_outage_notice_redirect_is_named(self):
        html = b"<!DOCTYPE html>\n" + b"x" * 600
        resp = _make_requests_response(
            200,
            html,
            content_type="text/html",
            url=(
                "https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq"
                "/publicacoes-off/painel-estatistico-aquaviario-indisponivel"
            ),
        )

        with (
            patch("agrobr.antaq.client.requests.get", return_value=resp),
            pytest.raises(SourceUnavailableError, match="official outage notice"),
        ):
            await client._download_zip(_ANTAQ_URL)


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


class TestFetchAnoZip:
    @pytest.mark.asyncio
    async def test_builds_correct_url(self):
        zip_bytes = _make_zip({"test.txt": "ok"})

        with patch.object(
            client, "_download_zip", new_callable=AsyncMock, return_value=zip_bytes
        ) as mock:
            result = await client.fetch_ano_zip(2024)

        mock.assert_called_once_with("https://estatistica.antaq.gov.br/ea/txt/2024.zip")
        assert result == zip_bytes


class TestFetchMercadoriaZip:
    @pytest.mark.asyncio
    async def test_builds_correct_url(self):
        zip_bytes = _make_zip({"Mercadoria.txt": "ok"})

        with patch.object(
            client, "_download_zip", new_callable=AsyncMock, return_value=zip_bytes
        ) as mock:
            result = await client.fetch_mercadoria_zip()

        mock.assert_called_once_with("https://estatistica.antaq.gov.br/ea/txt/Mercadoria.zip")
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
