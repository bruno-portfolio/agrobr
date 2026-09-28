"""Testes para os modelos ABIOVE."""

import pytest

from agrobr.abiove.models import resolve_produto


class TestResolveProduto:
    def test_invalid_raises_valueerror(self):
        with pytest.raises(ValueError, match="produto desconhecido"):
            resolve_produto("banana")
