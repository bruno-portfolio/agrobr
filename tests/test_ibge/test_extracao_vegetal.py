from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr.ibge import client, extracao_vegetal
from tests import helpers

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestExtracaoVegetalValidation:
    async def test_validacao_extracao_vegetal(self):
        cases = [
            (
                "test_produto_invalido",
                extracao_vegetal,
                ("banana_inexistente",),
                {},
                ValueError,
                "Produto não suportado",
            ),
            (
                "test_variavel_invalida",
                extracao_vegetal,
                ("acai",),
                {"variavel": "peso"},
                ValueError,
                "Variável não suportada",
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


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Mocked
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Golden data
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------
