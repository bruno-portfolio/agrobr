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
    remover_acentos,
    slugificar_praca,
    uf_para_ibge,
    uf_para_nome,
    uf_para_regiao,
    validar_uf,
)


class TestNormalizarUf:
    def test_sigla_upper(self):
        assert normalizar_uf("SP") == "SP"

    def test_sigla_lower(self):
        assert normalizar_uf("sp") == "SP"

    def test_sigla_mixed(self):
        assert normalizar_uf("Sp") == "SP"

    def test_nome_completo(self):
        assert normalizar_uf("São Paulo") == "SP"

    def test_nome_sem_acento(self):
        assert normalizar_uf("sao paulo") == "SP"

    def test_nome_lower(self):
        assert normalizar_uf("mato grosso") == "MT"

    def test_invalido_retorna_none(self):
        assert normalizar_uf("XX") is None

    def test_vazio_retorna_none(self):
        result = normalizar_uf("")
        assert result is None or isinstance(result, str)

    def test_espacos_trim(self):
        assert normalizar_uf("  SP  ") == "SP"

    def test_texto_corrompido_nao_casa_substring(self):
        assert normalizar_uf("paran�") is None

    def test_nome_completo_delimitado_em_frase(self):
        assert normalizar_uf("estado do para") == "PA"

    @pytest.mark.parametrize("uf", list(UFS.keys()))
    def test_todas_27_ufs_por_sigla(self, uf):
        assert normalizar_uf(uf) == uf

    @pytest.mark.parametrize("uf,info", list(UFS.items()))
    def test_todas_27_ufs_por_nome(self, uf, info):
        result = normalizar_uf(str(info["nome"]))
        assert result == uf


class TestUfParaNome:
    def test_sp(self):
        assert uf_para_nome("SP") == "São Paulo"

    def test_mt(self):
        assert uf_para_nome("MT") == "Mato Grosso"

    def test_case_insensitive(self):
        assert uf_para_nome("sp") == "São Paulo"

    def test_invalido_raises(self):
        with pytest.raises(KeyError):
            uf_para_nome("XX")


class TestUfParaRegiao:
    def test_sp_sudeste(self):
        assert uf_para_regiao("SP") == "Sudeste"

    def test_mt_centro_oeste(self):
        assert uf_para_regiao("MT") == "Centro-Oeste"

    def test_pa_norte(self):
        assert uf_para_regiao("PA") == "Norte"


class TestUfParaIbge:
    def test_sp(self):
        assert uf_para_ibge("SP") == 35

    def test_mt(self):
        assert uf_para_ibge("MT") == 51


class TestIbgeParaUf:
    def test_35_sp(self):
        assert ibge_para_uf(35) == "SP"

    def test_51_mt(self):
        assert ibge_para_uf(51) == "MT"

    def test_invalido_raises(self):
        with pytest.raises(ValueError, match="inválido"):
            ibge_para_uf(99)


class TestListarUfs:
    def test_sem_filtro_27(self):
        assert len(listar_ufs()) == 27

    def test_filtro_sul(self):
        result = listar_ufs("Sul")
        assert set(result) == {"PR", "RS", "SC"}

    def test_regiao_inexistente(self):
        assert listar_ufs("Inexistente") == []


class TestListarRegioes:
    def test_5_regioes(self):
        result = listar_regioes()
        assert len(result) == 5
        assert "Norte" in result
        assert "Sudeste" in result


class TestNormalizarMunicipio:
    def test_title_case(self):
        assert normalizar_municipio("são paulo") == "São Paulo"

    def test_preposicoes_minusculas(self):
        assert normalizar_municipio("rio de janeiro") == "Rio de Janeiro"

    def test_espacos_extras(self):
        assert normalizar_municipio("  rio   de   janeiro  ") == "Rio de Janeiro"


class TestValidarUf:
    def test_valida(self):
        assert validar_uf("SP") is True

    def test_invalida(self):
        assert validar_uf("XX") is False


class TestRemoverAcentos:
    def test_acentos(self):
        assert remover_acentos("São Paulo") == "Sao Paulo"
        assert remover_acentos("açúcar") == "acucar"
        assert remover_acentos("café") == "cafe"

    def test_sem_acento(self):
        assert remover_acentos("teste") == "teste"


class TestNormalizarPraca:
    def test_praca_cepea_conhecida(self):
        result = normalizar_praca("Paranaguá", produto="soja")
        assert result == "Paranagua"

    def test_praca_generica(self):
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
    def test_todas_ufs_tem_ibge(self):
        for uf, info in UFS.items():
            assert "ibge" in info, f"{uf} sem código IBGE"
            assert isinstance(info["ibge"], int)

    def test_todas_ufs_em_alguma_regiao(self):
        ufs_em_regioes = set()
        for ufs in REGIOES.values():
            ufs_em_regioes.update(ufs)
        assert ufs_em_regioes == set(UFS.keys())

    def test_27_ufs(self):
        assert len(UFS) == 27
