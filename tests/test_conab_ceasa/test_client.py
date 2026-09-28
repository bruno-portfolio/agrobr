from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.conab.ceasa import client
from agrobr.exceptions import SourceUnavailableError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


class TestFetchPrecos:
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
