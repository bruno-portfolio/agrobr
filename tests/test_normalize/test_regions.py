from __future__ import annotations

import re

import pytest

from agrobr.exceptions import InvalidParameterError, UnknownNameError
from agrobr.normalize.regions import (
    REGIOES,
    UFS,
    ibge_para_uf,
    listar_regioes,
    listar_ufs,
    normalizar_bioma,
    normalizar_municipio,
    normalizar_praca,
    normalizar_uf,
    remover_acentos,
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

    @pytest.mark.parametrize("regiao", ["Inexistente", "", "norte"])
    def test_regiao_invalida(self, regiao):
        with pytest.raises(InvalidParameterError, match="Região inválida.*Norte, Nordeste"):
            listar_ufs(regiao)

    def test_retorno_e_copia(self):
        listar_ufs("Sul").append("XX")
        assert listar_ufs("Sul") == ["PR", "RS", "SC"]


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


@pytest.mark.parametrize("funcao", [uf_para_nome, uf_para_regiao, uf_para_ibge])
@pytest.mark.parametrize("uf", ["ZZ", 51, None])
def test_uf_inexistente_levanta_erro_de_parametro_que_segue_key_error(funcao, uf):
    with pytest.raises(UnknownNameError, match="UF inválida.*Valores válidos: AC, AL, AM") as erro:
        funcao(uf)
    assert isinstance(erro.value, KeyError)


@pytest.mark.parametrize("valor", [51, ["MT"], None], ids=["int", "lista", "none"])
@pytest.mark.parametrize(
    ("funcao", "parametro"),
    [(normalizar_uf, "entrada"), (normalizar_municipio, "nome"), (normalizar_bioma, "bioma")],
    ids=["uf", "municipio", "bioma"],
)
def test_normalizador_recusa_valor_que_nao_e_texto(funcao, parametro, valor):
    with pytest.raises(
        InvalidParameterError, match=re.escape(f"{parametro} deve ser texto, recebeu {valor!r}")
    ):
        funcao(valor)


@pytest.mark.parametrize("valor", [51, ["MT"], None], ids=["int", "lista", "none"])
@pytest.mark.parametrize(
    ("funcao", "parametro"),
    [
        (remover_acentos, "texto"),
        (slugificar_praca, "praca"),
        (validar_uf, "uf"),
        (normalizar_praca, "praca"),
    ],
    ids=["remover_acentos", "slugificar_praca", "validar_uf", "normalizar_praca"],
)
def test_utilitario_recusa_valor_que_nao_e_texto(funcao, parametro, valor):
    with pytest.raises((InvalidParameterError, TypeError, AttributeError)) as erro:
        funcao(valor)
    assert (erro.type, str(erro.value)) == (
        InvalidParameterError,
        f"{parametro} deve ser texto, recebeu {valor!r}",
    )


@pytest.mark.parametrize("produto", [51, ["soja"]], ids=["int", "lista"])
def test_normalizar_praca_recusa_produto_que_nao_e_texto(produto):
    with pytest.raises((InvalidParameterError, TypeError, AttributeError)) as erro:
        normalizar_praca("Paranaguá", produto)
    assert (erro.type, str(erro.value)) == (
        InvalidParameterError,
        f"produto deve ser texto, recebeu {produto!r}",
    )
    assert [normalizar_praca("Paranaguá", p) for p in (None, "", "SOJA")] == [
        "Paranaguá",
        "Paranaguá",
        "Paranagua",
    ]


def test_normalizadores_mantem_o_retorno_do_texto():
    assert (normalizar_uf(" mato grosso "), normalizar_uf("Atlântida")) == ("MT", None)
    assert normalizar_municipio("  são   josé dos campos ") == "São José dos Campos"
    assert (normalizar_bioma(" CERRADO "), normalizar_bioma(" Outro ")) == ("Cerrado", "Outro")
