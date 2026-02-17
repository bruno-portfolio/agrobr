from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.b3 import client
from agrobr.exceptions import SourceUnavailableError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "ajustes_sample"


def _golden_html_bytes() -> bytes:
    html = GOLDEN_DIR.joinpath("response.html").read_text(encoding="utf-8")
    return html.encode("iso-8859-1")


class TestFetchAjustes:
    @pytest.mark.asyncio
    async def test_returns_html_string(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = _golden_html_bytes()
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            html, url = await client.fetch_ajustes("13/02/2025")

        assert isinstance(html, str)
        assert "BGI" in html
        assert "txtData" in url

    @pytest.mark.asyncio
    async def test_decodes_iso_8859_1(self):
        content = "Preço de Ajuste".encode("iso-8859-1")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = content
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            html, _ = await client.fetch_ajustes("13/02/2025")

        assert "Preço" in html

    @pytest.mark.asyncio
    async def test_404_raises_source_unavailable(self):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="b3"),
        ):
            await client.fetch_ajustes("13/02/2025")

    @pytest.mark.asyncio
    async def test_http_error_raises(self):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "Server Error", request=MagicMock(), response=mock_response
            )
        )

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client.fetch_ajustes("13/02/2025")

    def test_base_url_is_correct(self):
        assert "Ajustes1.asp" in client.BASE_URL
        assert "www2.bmf.com.br" in client.BASE_URL

    def test_headers_contain_user_agent(self):
        assert "agrobr" in client.HEADERS["User-Agent"]


class TestFetchPosicoesAbertas:
    @pytest.mark.asyncio
    async def test_returns_csv_bytes(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": "abc123"}
        token_response.raise_for_status = MagicMock()

        csv_content = b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst\n"
        csv_response = MagicMock()
        csv_response.status_code = 200
        csv_response.content = csv_content
        csv_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            side_effect=[token_response, csv_response],
        ):
            result_bytes, url = await client.fetch_posicoes_abertas("2025-12-19")

        assert isinstance(result_bytes, bytes)
        assert result_bytes == csv_content
        assert "requestname" in url

    @pytest.mark.asyncio
    async def test_two_step_flow(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": "test_token_123"}
        token_response.raise_for_status = MagicMock()

        csv_response = MagicMock()
        csv_response.status_code = 200
        csv_response.content = b"header\ndata"
        csv_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            side_effect=[token_response, csv_response],
        ) as mock_retry:
            await client.fetch_posicoes_abertas("2025-12-19")

        assert mock_retry.call_count == 2

    @pytest.mark.asyncio
    async def test_400_raises_source_unavailable(self):
        token_response = MagicMock()
        token_response.status_code = 400
        token_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=token_response,
            ),
            pytest.raises(SourceUnavailableError, match="b3"),
        ):
            await client.fetch_posicoes_abertas("2025-12-19")

    @pytest.mark.asyncio
    async def test_404_raises_source_unavailable(self):
        token_response = MagicMock()
        token_response.status_code = 404
        token_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=token_response,
            ),
            pytest.raises(SourceUnavailableError, match="b3"),
        ):
            await client.fetch_posicoes_abertas("2025-12-19")

    @pytest.mark.asyncio
    async def test_empty_token_raises(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": ""}
        token_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=token_response,
            ),
            pytest.raises(SourceUnavailableError, match="Token vazio"),
        ):
            await client.fetch_posicoes_abertas("2025-12-19")

    @pytest.mark.asyncio
    async def test_csv_download_failure_raises(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": "valid_token"}
        token_response.raise_for_status = MagicMock()

        csv_response = MagicMock()
        csv_response.status_code = 400
        csv_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                side_effect=[token_response, csv_response],
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_posicoes_abertas("2025-12-19")

    def test_base_url_arquivos_is_correct(self):
        assert "arquivos.b3.com.br" in client.BASE_URL_ARQUIVOS
