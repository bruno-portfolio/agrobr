from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError


@pytest.mark.asyncio
async def test_fetch_unknown_safra_raises():
    from agrobr.rio_verde.client import fetch_ensaio_soja

    with pytest.raises(InvalidParameterError, match="não disponível"):
        await fetch_ensaio_soja("1999/2000")


@pytest.mark.asyncio
async def test_fetch_returns_bytes():
    fake_resp = MagicMock(spec=httpx.Response)
    fake_resp.status_code = 200
    fake_resp.content = b"%PDF" + b"A" * 99_996
    fake_resp.raise_for_status = MagicMock()

    with patch("agrobr.rio_verde.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
        mock_retry.return_value = fake_resp

        from agrobr.rio_verde.client import fetch_ensaio_soja

        content, url = await fetch_ensaio_soja("2025/2026")
        assert len(content) == 100_000
        assert "fundacaorioverde" in url


@pytest.mark.asyncio
async def test_fetch_small_pdf_raises():
    fake_resp = MagicMock(spec=httpx.Response)
    fake_resp.status_code = 200
    fake_resp.content = b"tiny"
    fake_resp.raise_for_status = MagicMock()

    with patch("agrobr.rio_verde.client.retry_on_status", new_callable=AsyncMock) as mock_retry:
        mock_retry.return_value = fake_resp

        from agrobr.rio_verde.client import fetch_ensaio_soja

        with pytest.raises(SourceUnavailableError):
            await fetch_ensaio_soja("2025/2026")
