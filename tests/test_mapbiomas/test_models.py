from __future__ import annotations

from agrobr.mapbiomas.models import (
    ANO_FIM,
    ANO_INICIO,
    BIOMAS,
    BIOMAS_VALIDOS,
    CLASSES_LEGENDA,
    COLECAO_ATUAL,
    COLUNAS_SAIDA_COBERTURA,
    COLUNAS_SAIDA_TRANSICAO,
    ESTADOS_MAPBIOMAS,
    classe_para_nome,
    estado_para_uf,
    normalizar_bioma,
)


class TestNormalizarBioma:
    def test_amazonia_lowercase(self):
        assert normalizar_bioma("amazonia") == "Amazônia"

    def test_amazonia_accented(self):
        assert normalizar_bioma("amazônia") == "Amazônia"

    def test_cerrado(self):
        assert normalizar_bioma("cerrado") == "Cerrado"

    def test_mata_atlantica(self):
        assert normalizar_bioma("mata atlantica") == "Mata Atlântica"

    def test_mata_atlantica_accented(self):
        assert normalizar_bioma("mata atlântica") == "Mata Atlântica"

    def test_caatinga(self):
        assert normalizar_bioma("caatinga") == "Caatinga"

    def test_pampa(self):
        assert normalizar_bioma("pampa") == "Pampa"

    def test_pantanal(self):
        assert normalizar_bioma("pantanal") == "Pantanal"

    def test_unknown_passthrough(self):
        assert normalizar_bioma("desconhecido") == "desconhecido"

    def test_with_spaces(self):
        assert normalizar_bioma("  cerrado  ") == "Cerrado"


class TestEstadoParaUf:
    def test_acre(self):
        assert estado_para_uf("Acre") == "AC"

    def test_goias(self):
        assert estado_para_uf("Goiás") == "GO"

    def test_sao_paulo(self):
        assert estado_para_uf("São Paulo") == "SP"

    def test_all_27_estados(self):
        assert len(ESTADOS_MAPBIOMAS) == 27

    def test_unknown_passthrough(self):
        assert estado_para_uf("Unknown") == "Unknown"

    def test_distrito_federal_with_newline(self):
        assert estado_para_uf("Distrito Federal\n") == "DF"

    def test_with_spaces(self):
        assert estado_para_uf("  Acre  ") == "AC"


class TestClasseParaNome:
    def test_floresta_formacao(self):
        assert classe_para_nome(3) == "Formação Florestal"

    def test_pastagem(self):
        assert classe_para_nome(15) == "Pastagem"

    def test_soja(self):
        assert classe_para_nome(39) == "Soja"

    def test_unknown_class(self):
        assert classe_para_nome(999) == "Classe 999"


class TestConstants:
    def test_colecao_atual(self):
        assert COLECAO_ATUAL >= 10

    def test_ano_range(self):
        assert ANO_INICIO == 1985
        assert ANO_FIM >= 2024

    def test_biomas_validos_has_6(self):
        assert len(BIOMAS_VALIDOS) == 6

    def test_biomas_dict_covers_all(self):
        for bioma in BIOMAS_VALIDOS:
            assert bioma in BIOMAS.values()

    def test_classes_legenda_non_empty(self):
        assert len(CLASSES_LEGENDA) >= 30

    def test_colunas_saida_cobertura(self):
        assert "bioma" in COLUNAS_SAIDA_COBERTURA
        assert "estado" in COLUNAS_SAIDA_COBERTURA
        assert "classe_id" in COLUNAS_SAIDA_COBERTURA
        assert "classe" in COLUNAS_SAIDA_COBERTURA
        assert "ano" in COLUNAS_SAIDA_COBERTURA
        assert "area_ha" in COLUNAS_SAIDA_COBERTURA

    def test_colunas_saida_transicao(self):
        assert "bioma" in COLUNAS_SAIDA_TRANSICAO
        assert "estado" in COLUNAS_SAIDA_TRANSICAO
        assert "classe_de_id" in COLUNAS_SAIDA_TRANSICAO
        assert "classe_para_id" in COLUNAS_SAIDA_TRANSICAO
        assert "periodo" in COLUNAS_SAIDA_TRANSICAO
        assert "area_ha" in COLUNAS_SAIDA_TRANSICAO
