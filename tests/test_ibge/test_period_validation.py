from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr import ibge
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import _helpers, client


@pytest.mark.parametrize("period", ["202504", "2025-4", "2025-T4", "2025T4", "2025/4", "2025Q4"])
def test_quarter_aliases(period):
    assert _helpers.resolve_quarter_period(period) == "202504"


@pytest.mark.parametrize("period", ["", "2025-0", "2025-5", "2025", "abcd", 202504, True])
@pytest.mark.parametrize("method", [ibge.abate, ibge.leite_trimestral, ibge.pib_agro])
async def test_invalid_quarter_rejected_before_network(period, method, monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="[Tt]rimestre"):
        args = ("bovino",) if method is ibge.abate else ()
        await method(*args, trimestre=period)
    fetch.assert_not_awaited()
