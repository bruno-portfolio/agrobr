from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr.ibge import client, silvicultura
from tests import helpers


class TestSilviculturaValidation:
    async def test_validacao_silvicultura(self):
        cases = [
            (
                "test_produto_invalido",
                silvicultura,
                ("banana_inexistente",),
                {},
                ValueError,
                "Produto inválido",
            ),
            (
                "test_variavel_invalida",
                silvicultura,
                ("carvao",),
                {"variavel": "peso"},
                ValueError,
                "Variável inválida",
            ),
            (
                "test_especie_invalida_para_area",
                silvicultura,
                ("carvao",),
                {"variavel": "area"},
                ValueError,
                "Produto inválido",
            ),
        ]
        with helpers.collect_failures() as check:
            for case, function, args, kwargs, exception, message in cases:
                with check(case), helpers.isolated_dataset_case((case, kwargs)) as monkeypatch:
                    fetch = AsyncMock()
                    monkeypatch.setattr(client, "fetch_sidra", fetch)
                    with pytest.raises(exception, match=message):
                        await function(*args, **kwargs)
                    fetch.assert_not_awaited()
