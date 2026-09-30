from __future__ import annotations

import pytest

from agrobr.ibge._helpers import resolve_ibge_code


class TestResolveIbgeCode:
    def test_municipio(self):
        level, code = resolve_ibge_code("MT", "municipio")
        assert level == "6"
        assert code.startswith("in N3")

    def test_invalid_nivel_raises(self):
        assert resolve_ibge_code(None, "Brasil") == ("1", "all")
        with pytest.raises(ValueError, match="nível inválido"):
            resolve_ibge_code(None, "estado")
