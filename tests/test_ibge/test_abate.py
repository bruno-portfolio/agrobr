from __future__ import annotations

import pytest


class TestAbateValidation:
    async def test_erro_lista_especies(self):
        from agrobr.ibge.api import abate

        with pytest.raises(ValueError, match="bovino"):
            await abate("lagosta")
