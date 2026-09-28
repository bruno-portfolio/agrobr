from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.b3 import client
from agrobr.exceptions import SourceUnavailableError


class TestFetchPosicoesAbertas:
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


class TestFetchAjustesZip:
    @pytest.mark.asyncio
    async def test_zip_vazio_indica_pregao_nao_publicado(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"\x00" * 22
        mock_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            pytest.raises(SourceUnavailableError, match="ainda não publicado"),
        ):
            await client.fetch_ajustes_zip("12/06/2026")
