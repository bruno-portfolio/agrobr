"""Testes para os modelos Pydantic INMET."""

from datetime import date, datetime

import pytest

from agrobr.inmet.models import Estacao, ObservacaoHoraria


class TestEstacao:
    def test_from_api_dict(self):
        data = {
            "CD_ESTACAO": "A001",
            "DC_NOME": "BRASILIA",
            "SG_ESTADO": "DF",
            "CD_SITUACAO": "Operante",
            "TP_ESTACAO": "Automatica",
            "VL_LATITUDE": "-15.789343",
            "VL_LONGITUDE": "-47.925756",
            "VL_ALTITUDE": "1160.96",
            "DT_INICIO_OPERACAO": "2000-05-07",
            "DT_FIM_OPERACAO": None,
            "SG_ENTIDADE": "INMET",
            "CD_OSCAR": "0-2000-0-86716",
        }

        estacao = Estacao.model_validate(data)

        assert estacao.codigo == "A001"
        assert estacao.nome == "BRASILIA"
        assert estacao.uf == "DF"
        assert estacao.latitude == pytest.approx(-15.789343)
        assert estacao.longitude == pytest.approx(-47.925756)
        assert estacao.altitude == pytest.approx(1160.96)
        assert estacao.operante is True

    def test_estacao_desativada(self):
        data = {
            "CD_ESTACAO": "A999",
            "DC_NOME": "TESTE",
            "SG_ESTADO": "SP",
            "CD_SITUACAO": "Desativada",
            "TP_ESTACAO": "Automatica",
            "VL_LATITUDE": "-23.0",
            "VL_LONGITUDE": "-46.0",
            "VL_ALTITUDE": "700.0",
            "DT_INICIO_OPERACAO": "2005-01-01",
            "DT_FIM_OPERACAO": "2020-12-31",
        }

        estacao = Estacao.model_validate(data)

        assert estacao.operante is False
        assert estacao.fim_operacao == date(2020, 12, 31)


class TestObservacaoHoraria:
    def test_from_api_dict(self):
        data = {
            "CD_ESTACAO": "A001",
            "DT_MEDICAO": "2024-01-15",
            "HR_MEDICAO": "1200 UTC",
            "UF": "DF",
            "TEM_INS": "25.3",
            "TEM_MAX": "28.0",
            "TEM_MIN": "22.1",
            "UMD_INS": "65.0",
            "UMD_MAX": "70.0",
            "UMD_MIN": "60.0",
            "CHUVA": "0.0",
            "RAD_GLO": "500.0",
            "PRE_INS": "886.2",
            "PRE_MAX": "887.0",
            "PRE_MIN": "885.0",
            "VEN_VEL": "2.5",
            "VEN_DIR": "180",
            "VEN_RAJ": "5.0",
            "PTO_INS": "15.0",
            "PTO_MAX": "16.0",
            "PTO_MIN": "14.0",
        }

        obs = ObservacaoHoraria.model_validate(data)

        assert obs.estacao == "A001"
        assert obs.temperatura == pytest.approx(25.3)
        assert obs.precipitacao == pytest.approx(0.0)
        assert obs.umidade == pytest.approx(65.0)

    def test_sentinel_becomes_none(self):
        data = {
            "CD_ESTACAO": "A001",
            "DT_MEDICAO": "2024-01-15",
            "HR_MEDICAO": "0000 UTC",
            "TEM_INS": "-9999",
            "CHUVA": "-9999",
        }

        obs = ObservacaoHoraria.model_validate(data)

        assert obs.temperatura is None
        assert obs.precipitacao is None

    def test_empty_string_becomes_none(self):
        data = {
            "CD_ESTACAO": "A001",
            "DT_MEDICAO": "2024-01-15",
            "HR_MEDICAO": "0600 UTC",
            "TEM_INS": "",
            "CHUVA": "",
        }

        obs = ObservacaoHoraria.model_validate(data)

        assert obs.temperatura is None
        assert obs.precipitacao is None

    def test_null_becomes_none(self):
        data = {
            "CD_ESTACAO": "A001",
            "DT_MEDICAO": "2024-01-15",
            "HR_MEDICAO": "0600 UTC",
            "TEM_INS": None,
        }

        obs = ObservacaoHoraria.model_validate(data)

        assert obs.temperatura is None

    def test_datetime_utc_property(self):
        data = {
            "CD_ESTACAO": "A001",
            "DT_MEDICAO": "2024-01-15",
            "HR_MEDICAO": "1430 UTC",
        }

        obs = ObservacaoHoraria.model_validate(data)
        dt = obs.datetime_utc

        assert dt == datetime(2024, 1, 15, 14, 30)
