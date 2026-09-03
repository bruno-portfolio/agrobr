from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.lista_suja import client


class TestFetchEmpregadores:
    @pytest.mark.asyncio
    async def test_valid_download(self):
        xlsx_bytes = b"%PDF" + b"\x00" * 4996
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = xlsx_bytes
        mock_resp.raise_for_status = MagicMock()

        with patch(
            "agrobr.lista_suja.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            data, url = await client.fetch_empregadores()

        assert len(data) == 5000

    @pytest.mark.asyncio
    async def test_small_file_raises(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"\x00" * 50
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.lista_suja.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_empregadores()
