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
