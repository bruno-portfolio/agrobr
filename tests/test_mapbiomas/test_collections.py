from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr import mapbiomas
from agrobr.exceptions import InvalidParameterError
from agrobr.mapbiomas import client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "collection,period",
    [(10, "1985-2025"), (11, "1985-2026"), (11, "2020-2010"), (11, "2025")],
)
async def test_transicao_periodo_invalido_rejeitado_sem_download(collection: int, period: str):
    with (
        patch.object(client, "fetch_biome_state_bundle", new_callable=AsyncMock) as fetch,
        pytest.raises(InvalidParameterError),
    ):
        await mapbiomas.transicao(colecao=collection, periodo=period)
    fetch.assert_not_awaited()
