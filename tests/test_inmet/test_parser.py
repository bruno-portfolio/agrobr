"""Testes para o parser INMET."""

import pytest

from agrobr.exceptions import ParseError
from agrobr.inmet.parser import (
    PARSER_VERSION,
    agregar_diario,
    agregar_mensal_uf,
    parse_observacoes,
)
from tests.helpers import levanta_exatamente


def _obs(
    estacao="A001",
    data="2024-01-15",
    hora="1200 UTC",
    uf="DF",
    tem="25.3",
    tem_max="28.0",
    tem_min="22.1",
    umd="65.0",
    chuva="0.0",
    rad="500.0",
    pre="886.2",
    ven_vel="2.5",
    ven_dir="180",
    ven_raj="5.0",
    pto="15.0",
):
    return {
        "CD_ESTACAO": estacao,
        "DT_MEDICAO": data,
        "HR_MEDICAO": hora,
        "UF": uf,
        "TEM_INS": tem,
        "TEM_MAX": tem_max,
        "TEM_MIN": tem_min,
        "UMD_INS": umd,
        "UMD_MAX": "70.0",
        "UMD_MIN": "60.0",
        "CHUVA": chuva,
        "RAD_GLO": rad,
        "PRE_INS": pre,
        "PRE_MAX": "887.0",
        "PRE_MIN": "885.0",
        "VEN_VEL": ven_vel,
        "VEN_DIR": ven_dir,
        "VEN_RAJ": ven_raj,
        "PTO_INS": pto,
        "PTO_MAX": "16.0",
        "PTO_MIN": "14.0",
        "VL_LATITUDE": "-15.789",
        "VL_LONGITUDE": "-47.925",
        "VL_ALTITUDE": "1160.96",
    }


class TestParseObservacoes:
    def test_parse_null_becomes_nan(self):
        dados = [_obs(tem=None)]
        df = parse_observacoes(dados)

        assert df.iloc[0]["temperatura"] != df.iloc[0]["temperatura"]

    def test_parse_empty_raises(self):
        with pytest.raises(ParseError) as exc_info:
            parse_observacoes([])
        assert "vazia" in str(exc_info.value)
        assert exc_info.value.parser_version == PARSER_VERSION

    def test_parse_sorts_by_station_and_date(self):
        dados = [
            _obs(estacao="A002", data="2024-01-15"),
            _obs(estacao="A001", data="2024-01-16"),
            _obs(estacao="A001", data="2024-01-15"),
        ]
        df = parse_observacoes(dados)

        assert df.iloc[0]["estacao"] == "A001"
        assert df.iloc[2]["estacao"] == "A002"


class TestAgregarMensalUf:
    def test_agrega_diario_para_mensal(self):
        dados = []
        for day in range(1, 4):
            dados.append(_obs(data=f"2024-01-{day:02d}", chuva="10.0"))
        for day in range(1, 3):
            dados.append(_obs(data=f"2024-02-{day:02d}", chuva="20.0"))

        df_horario = parse_observacoes(dados)
        df_diario = agregar_diario(df_horario)
        df_mensal = agregar_mensal_uf(df_diario)

        assert len(df_mensal) == 2
        assert "precip_acum_mm" in df_mensal.columns
        assert "temp_media" in df_mensal.columns
        assert "num_estacoes" in df_mensal.columns

    def test_agrega_empty(self):
        import pandas as pd

        df = pd.DataFrame()
        result = agregar_mensal_uf(df)
        assert result.empty


def test_parse_observacoes_sem_coluna_conhecida_recusa():
    with levanta_exatamente(ParseError, "Nenhuma coluna esperada"):
        parse_observacoes([{"COLUNA_NOVA": "1"}])


def test_parse_observacoes_sem_hora_recusa():
    with levanta_exatamente(ParseError, "Hora UTC ausente"):
        parse_observacoes([{"CD_ESTACAO": "A001", "DT_MEDICAO": "2024-01-01"}])
