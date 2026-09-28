"""Testes de resiliência HTTP para agrobr.noticias_agricolas.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.noticias_agricolas import client
from tests.helpers import make_mock_response


class TestNaEncoding:
    @pytest.mark.asyncio
    async def test_encoding_fallback_charset_wrong(self):
        iso_content = "Cotação soja".encode("iso-8859-1")
        resp = make_mock_response(200, content=iso_content, charset_encoding="utf-8")
        decoded_html = "<html><table>Cotação soja</table></html>"

        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.noticias_agricolas.client.decode_content") as mock_decode:
                mock_decode.return_value = (decoded_html, "iso-8859-1")
                result = await client.fetch_indicador_page("soja")

        assert "Cotação" in result
        mock_decode.assert_called_once_with(
            iso_content, declared_encoding="utf-8", source="noticias_agricolas"
        )
