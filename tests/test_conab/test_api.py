from __future__ import annotations

from unittest.mock import AsyncMock, patch

from agrobr import constants
from agrobr.conab import api


class TestProdutos:
    async def test_matches_constants(self):
        result = await api.produtos()

        assert set(result) == set(constants.CONAB_PRODUTOS.keys())


class TestUfs:
    async def test_returns_copy_not_reference(self):
        result1 = await api.ufs()
        result2 = await api.ufs()

        assert result1 is not result2


class TestLevantamentos:
    async def test_returns_list(self):
        mock_data = [
            {"safra": "2025/26", "levantamento": 6, "url": "https://conab.gov.br/6.xlsx"},
            {"safra": "2025/26", "levantamento": 5, "url": "https://conab.gov.br/5.xlsx"},
        ]
        with patch(
            "agrobr.conab.api.client.list_levantamentos",
            new_callable=AsyncMock,
            return_value=mock_data,
        ):
            result = await api.levantamentos()

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["levantamento"] == 6
