from __future__ import annotations

import pytest

from agrobr.normalize.regions import (
    REGIOES,
    UFS,
    ibge_para_uf,
    listar_regioes,
    listar_ufs,
    normalizar_municipio,
    normalizar_praca,
    normalizar_uf,
    slugificar_praca,
    uf_para_ibge,
    uf_para_nome,
    uf_para_regiao,
    validar_uf,
)
from tests.helpers import collect_failures


@pytest.mark.parametrize("uf", sorted(UFS))
def test_full_state_name_in_phrase(uf):
    assert normalizar_uf(f"Estado de {UFS[uf]['nome']}") == uf


class TestUfParaNome:
    def test_case_insensitive(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_sp", "SP", "São Paulo"),
                ("test_mt", "MT", "Mato Grosso"),
                ("test_case_insensitive", "sp", "São Paulo"),
            ]:
                with check(case):
                    assert uf_para_nome(value) == expected


class TestUfParaRegiao:
    def test_mt_centro_oeste(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_sp_sudeste", "SP", "Sudeste"),
                ("test_mt_centro_oeste", "MT", "Centro-Oeste"),
                ("test_pa_norte", "PA", "Norte"),
            ]:
                with check(case):
                    assert uf_para_regiao(value) == expected


class TestUfParaIbge:
    def test_mt(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_sp", "SP", 35),
                ("test_mt", "MT", 51),
            ]:
                with check(case):
                    assert uf_para_ibge(value) == expected


class TestIbgeParaUf:
    def test_35_sp(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_35_sp", 35, "SP"),
                ("test_51_mt", 51, "MT"),
            ]:
                with check(case):
                    assert ibge_para_uf(value) == expected


class TestListarUfs:
    def test_filtro_sul(self):
        with collect_failures() as check:
            with check("test_sem_filtro_27"):
                assert len(listar_ufs()) == 27
            with check("test_filtro_sul"):
                result = listar_ufs("Sul")
                assert set(result) == {"PR", "RS", "SC"}
            with check("test_regiao_inexistente"):
                assert listar_ufs("Inexistente") == []


class TestListarRegioes:
    def test_5_regioes(self):
        result = listar_regioes()
        assert len(result) == 5
        assert "Norte" in result
        assert "Sudeste" in result


class TestNormalizarMunicipio:
    def test_espacos_extras(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_title_case", "são paulo", "São Paulo"),
                ("test_preposicoes_minusculas", "rio de janeiro", "Rio de Janeiro"),
                ("test_espacos_extras", "  rio   de   janeiro  ", "Rio de Janeiro"),
            ]:
                with check(case):
                    assert normalizar_municipio(value) == expected


class TestValidarUf:
    def test_invalida(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_valida", "SP", True),
                ("test_invalida", "XX", False),
            ]:
                with check(case):
                    assert validar_uf(value) is expected


class TestNormalizarPraca:
    def test_praca_cepea_conhecida(self):
        with collect_failures() as check:
            with check("test_praca_cepea_conhecida"):
                result = normalizar_praca("Paranaguá", produto="soja")
                assert result == "Paranagua"
            with check("test_praca_generica"):
                result = normalizar_praca("  rio verde  ", produto="milho")
                assert result == "Rio Verde"


class TestSlugificarPraca:
    @pytest.mark.parametrize(
        ("praca", "esperado"),
        [
            ("Paranaguá/PR", "paranagua"),
            ("paranagua", "paranagua"),
            (" São Paulo / SP ", "sao_paulo"),
            ("Espírito Santo", "espirito_santo"),
            ("rio_grande_do_sul", "rio_grande_do_sul"),
        ],
    )
    def test_normaliza_rotulo_e_slug(self, praca: str, esperado: str):
        assert slugificar_praca(praca) == esperado


class TestCompletude:
    def test_todas_ufs_em_alguma_regiao(self):
        ufs_em_regioes = set()
        for ufs in REGIOES.values():
            ufs_em_regioes.update(ufs)
        assert ufs_em_regioes == set(UFS.keys())
