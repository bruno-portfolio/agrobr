"""Testes para agrobr.antaq.models."""

from __future__ import annotations

import pytest

from agrobr.antaq.models import (
    MIN_ANO,
    NATUREZA_CARGA,
    TIPO_NAVEGACAO,
    resolve_natureza_carga,
    resolve_tipo_navegacao,
)


class TestResolveTipoNavegacao:
    def test_alias_longo_curso(self):
        assert resolve_tipo_navegacao("longo_curso") == "Longo Curso"

    def test_alias_cabotagem(self):
        assert resolve_tipo_navegacao("cabotagem") == "Cabotagem"

    def test_alias_interior(self):
        assert resolve_tipo_navegacao("interior") == "Interior"

    def test_alias_apoio_maritimo(self):
        assert resolve_tipo_navegacao("apoio_maritimo") == "Apoio Marítimo"

    def test_alias_apoio_portuario(self):
        assert resolve_tipo_navegacao("apoio_portuario") == "Apoio Portuário"

    def test_valor_direto(self):
        assert resolve_tipo_navegacao("Longo Curso") == "Longo Curso"

    def test_none(self):
        assert resolve_tipo_navegacao(None) is None

    def test_invalido(self):
        with pytest.raises(ValueError, match="Tipo de navegação desconhecido"):
            resolve_tipo_navegacao("invalido")

    def test_com_espacos(self):
        assert resolve_tipo_navegacao(" longo_curso ") == "Longo Curso"


class TestResolveNaturezaCarga:
    def test_alias_granel_solido(self):
        assert resolve_natureza_carga("granel_solido") == "Granel Sólido"

    def test_alias_granel_liquido(self):
        assert resolve_natureza_carga("granel_liquido") == "Granel Líquido e Gasoso"

    def test_alias_carga_geral(self):
        assert resolve_natureza_carga("carga_geral") == "Carga Geral"

    def test_alias_conteiner(self):
        assert resolve_natureza_carga("conteiner") == "Carga Conteinerizada"

    def test_valor_direto(self):
        assert resolve_natureza_carga("Granel Sólido") == "Granel Sólido"

    def test_none(self):
        assert resolve_natureza_carga(None) is None

    def test_invalido(self):
        with pytest.raises(ValueError, match="Natureza da carga desconhecida"):
            resolve_natureza_carga("invalido")


class TestConstantes:
    def test_tipo_navegacao_count(self):
        assert len(TIPO_NAVEGACAO) == 5

    def test_natureza_carga_count(self):
        assert len(NATUREZA_CARGA) == 4

    def test_min_ano(self):
        assert MIN_ANO == 2010
