from __future__ import annotations

from agrobr.queimadas.models import (
    BIOMAS_VALIDOS,
    COLUNAS_CSV,
    COLUNAS_SAIDA,
    SATELITES,
    UF_ESTADO,
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
    def test_mato_grosso(self):
        assert estado_para_uf("MATO GROSSO") == "MT"

    def test_sao_paulo(self):
        assert estado_para_uf("SÃO PAULO") == "SP"

    def test_lowercase(self):
        assert estado_para_uf("mato grosso") == "MT"

    def test_amazonas(self):
        assert estado_para_uf("AMAZONAS") == "AM"

    def test_all_27_ufs(self):
        assert len(UF_ESTADO) == 27

    def test_unknown_passthrough(self):
        assert estado_para_uf("UNKNOWN") == "UNKNOWN"


class TestConstants:
    def test_biomas_validos_has_6(self):
        assert len(BIOMAS_VALIDOS) == 6

    def test_colunas_csv_has_16(self):
        assert len(COLUNAS_CSV) == 16

    def test_colunas_saida_non_empty(self):
        assert len(COLUNAS_SAIDA) > 0

    def test_satelites_non_empty(self):
        assert len(SATELITES) > 0
        assert "AQUA_M-T" in SATELITES
        assert "NOAA-20" in SATELITES
