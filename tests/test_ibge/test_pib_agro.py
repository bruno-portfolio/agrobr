from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr.ibge import client, pib_agro
from tests import helpers

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestPibValidation:
    async def test_validacao_pib_agro(self):
        cases = [
            (
                "test_setor_invalido",
                pib_agro,
                (),
                {"setor": "energia"},
                ValueError,
                "Setor não suportado",
            ),
            (
                "test_precos_invalido",
                pib_agro,
                (),
                {"precos": "constante_2020"},
                ValueError,
                "Tipo de preços não suportado",
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
# Cache
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------
