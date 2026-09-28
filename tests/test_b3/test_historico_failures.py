from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.b3 import api
from agrobr.exceptions import ParseError, SourceUnavailableError


@pytest.mark.asyncio
async def test_historico_all_days_failed_raises_with_last_cause():
    last = ParseError(source="b3", reason="layout", parser_version=1)
    source = AsyncMock(side_effect=[httpx.ConnectError("offline"), last])
    with (
        patch("agrobr.b3.api.ajustes", source),
        pytest.raises(SourceUnavailableError) as caught,
    ):
        await api.historico(contrato="boi", inicio="2026-09-03", fim="2026-09-04")
    assert caught.value.__cause__ is last
    assert "layout" in caught.value.last_error
