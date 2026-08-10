from __future__ import annotations

import pytest

from agrobr.ibge._helpers import NIVEL_MAP_HISTORICO, resolve_ibge_code


class TestResolveIbgeCode:
    def test_brasil(self):
        level, code = resolve_ibge_code(None, "brasil")
        assert level == "1"
        assert code == "all"

    def test_uf(self):
        level, _code = resolve_ibge_code("MT", "uf")
        assert level == "3"

    def test_municipio(self):
        level, code = resolve_ibge_code("MT", "municipio")
        assert level == "6"
        assert code.startswith("in N3")

    def test_invalid_nivel_raises(self):
        with pytest.raises(ValueError, match="nível inválido"):
            resolve_ibge_code(None, "Brasil")
        with pytest.raises(ValueError, match="nível inválido"):
            resolve_ibge_code(None, "estado")

    def test_nivel_map_historico_permite_regiao(self):
        level, _ = resolve_ibge_code(None, "regiao", nivel_map=NIVEL_MAP_HISTORICO)
        assert level == "2"
