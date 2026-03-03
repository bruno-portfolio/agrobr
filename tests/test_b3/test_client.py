from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.b3 import client
from agrobr.exceptions import SourceUnavailableError


class TestFetchPosicoesAbertas:
    @pytest.mark.asyncio
    async def test_returns_csv_bytes(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": "abc123"}
        token_response.raise_for_status = MagicMock()

        csv_content = (
            b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst\n"
            b"2025-12-19;BGI;BRBGIDBS006;BGI;J26;FUT;12345;100\n"
        )
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
        csv_response.content = (
            b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst\n"
            b"2025-12-19;BGI;BRBGIDBS006;BGI;J26;FUT;12345;100\n"
        )
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


class TestFetchAjustesZip:
    @pytest.mark.asyncio
    async def test_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"\x00" * 1000
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            content, url = await client.fetch_ajustes_zip("03/03/2026")

        assert isinstance(content, bytes)
        assert len(content) == 1000
        assert "PR260303.zip" in url

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
            await client.fetch_ajustes_zip("03/03/2026")

    @pytest.mark.asyncio
    async def test_too_small_raises_source_unavailable(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"\x00" * 10
        mock_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="ZIP too small"),
        ):
            await client.fetch_ajustes_zip("03/03/2026")

    @pytest.mark.asyncio
    async def test_date_format(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"\x00" * 1000
        mock_response.raise_for_status = MagicMock()

        with patch(
            "agrobr.b3.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            _, url = await client.fetch_ajustes_zip("27/02/2026")

        assert "PR260227.zip" in url

    def test_base_url_zip_is_correct(self):
        assert "pesquisapregao" in client.BASE_URL_ZIP
        assert "b3.com.br" in client.BASE_URL_ZIP

    def test_headers_contain_user_agent(self):
        from agrobr.http.user_agents import UserAgentRotator

        headers = UserAgentRotator.get_bot_headers()
        assert "agrobr" in headers["User-Agent"]
