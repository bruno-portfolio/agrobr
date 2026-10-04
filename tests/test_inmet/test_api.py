"""Testes para a API pública INMET."""

import json
import re
import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.inmet import api

ESTACOES_T = (
    Path(__file__).resolve().parents[1] / "golden_data/inmet/estacoes_20260924/estacoes_T.json"
)


def _mock_obs(
    estacao="A001",
    data="2024-01-15",
    hora="1200 UTC",
    uf="DF",
    chuva="5.0",
):
    return {
        "CD_ESTACAO": estacao,
        "DT_MEDICAO": data,
        "HR_MEDICAO": hora,
        "UF": uf,
        "TEM_INS": "25.3",
        "TEM_MAX": "28.0",
        "TEM_MIN": "22.1",
        "UMD_INS": "65.0",
        "UMD_MAX": "70.0",
        "UMD_MIN": "60.0",
        "CHUVA": chuva,
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
        "VL_LATITUDE": "-15.789",
        "VL_LONGITUDE": "-47.925",
        "VL_ALTITUDE": "1160.96",
    }


def _mock_estacao(
    codigo="A001",
    nome="BRASILIA",
    uf="DF",
    situacao="Operante",
    tipo="Automatica",
    lat="-15.789",
    lon="-47.925",
    alt="1160.96",
    inicio="2000-05-07",
):
    return {
        "CD_ESTACAO": codigo,
        "DC_NOME": nome,
        "SG_ESTADO": uf,
        "CD_SITUACAO": situacao,
        "TP_ESTACAO": tipo,
        "VL_LATITUDE": lat,
        "VL_LONGITUDE": lon,
        "VL_ALTITUDE": alt,
        "DT_INICIO_OPERACAO": inicio,
    }


class TestEstacoes:
    @pytest.mark.asyncio
    async def test_estacoes_filter_uf(self):
        mock_data = [
            _mock_estacao(),
            _mock_estacao(
                codigo="A002",
                nome="SAO PAULO",
                uf="SP",
                lat="-23.5",
                lon="-46.6",
                alt="800.0",
                inicio="2001-01-01",
            ),
        ]

        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacoes(uf="SP")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "SP"


class TestEstacao:
    @pytest.mark.asyncio
    async def test_estacao_horario(self):
        mock_data = [_mock_obs(hora="1200 UTC"), _mock_obs(hora="1300 UTC")]

        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacao("A001", "2024-01-15", "2024-01-15")

        assert len(df) == 2
        assert "temperatura" in df.columns

    @pytest.mark.asyncio
    async def test_estacao_leva_o_descarte_de_data_ao_meta(self):
        mock_data = [_mock_obs(data="1899-12-31"), _mock_obs(data="1900-01-01")]
        aviso = (
            "inmet: 1 valor(es) de data viraram NaT (data ilegível ou com ano fora de "
            "1900–2099). As observações sem data saíram do resultado."
        )

        with (
            patch.object(
                api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
            ),
            warnings.catch_warnings(record=True) as emitidos,
        ):
            warnings.simplefilter("always")
            df, meta = await api.estacao("A001", "1899-12-31", "1900-01-01", return_meta=True)

        assert df["data"].tolist() == [pd.Timestamp("1900-01-01")]
        assert meta.validation_warnings == [aviso]
        assert [str(w.message) for w in emitidos if "NaT" in str(w.message)] == [aviso]

    @pytest.mark.asyncio
    async def test_estacao_diario(self):
        mock_data = [
            _mock_obs(hora="0000 UTC", chuva="5.0"),
            _mock_obs(hora="0600 UTC", chuva="3.0"),
            _mock_obs(hora="1200 UTC", chuva="0.0"),
            _mock_obs(hora="1800 UTC", chuva="2.0"),
        ]

        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacao("A001", "2024-01-15", "2024-01-15", agregacao="diario")

        assert len(df) == 1
        assert "precipitacao_mm" in df.columns
        assert df.iloc[0]["precipitacao_mm"] == pytest.approx(10.0)


class TestClimaUf:
    @pytest.mark.asyncio
    async def test_clima_uf_rejects_invalid_uf_before_request(self):
        with (
            patch.object(api.client, "fetch_dados_estacoes_uf", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="UF inválida"),
        ):
            await api.clima_uf("XX", 2024)

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ano", [0, 1999, 9999, True, "2024"])
    async def test_clima_uf_recusa_ano_fora_do_dominio_antes_da_rede(self, ano):
        with (
            patch.object(api.client, "fetch_dados_estacoes_uf", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="ano deve estar entre 2000 e"),
        ):
            await api.clima_uf("SP", ano)

        fetch.assert_not_awaited()


class TestSelecaoSemObservacoes:
    @pytest.mark.asyncio
    async def test_estacao_sem_observacoes_no_periodo_e_fonte_sem_dado(self):
        with (
            patch.object(
                api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=[]
            ),
            pytest.raises(SourceUnavailableError, match="sem observações da estação A001"),
        ):
            await api.estacao("A001", "2024-01-15", "2024-01-16")

    @pytest.mark.asyncio
    async def test_clima_uf_sem_observacoes_no_ano_e_fonte_sem_dado(self):
        with (
            patch.object(
                api.client, "fetch_dados_estacoes_uf", new_callable=AsyncMock, return_value=[]
            ),
            pytest.raises(SourceUnavailableError, match="sem observações das estações de SP"),
        ):
            await api.clima_uf("SP", 2024)


class TestEstacoesEntrada:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("argumentos", "mensagem"),
        [
            ({"uf": "ZZ"}, "UF inválida: 'ZZ'. Valores válidos: AC"),
            ({"uf": 51}, "UF inválida: 51. Valores válidos: AC"),
            ({"tipo": "X"}, "tipo inválido: 'X'. Valores válidos"),
        ],
    )
    async def test_filtro_invalido_recusado_antes_da_rede(self, argumentos, mensagem):
        with (
            patch.object(api.client, "_get_json", new_callable=AsyncMock) as rede,
            pytest.raises(InvalidParameterError, match=re.escape(mensagem)),
        ):
            await api.estacoes(**argumentos)

        rede.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_catalogo_vazio_e_layout_e_nao_tabela_vazia(self):
        with (
            patch.object(api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[]),
            pytest.raises(ParseError, match="Catálogo de estações do INMET vazio"),
        ):
            await api.estacoes()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("campo", "filtro", "sem_filtro", "esperado"),
        [
            ("SG_ESTADO", "o filtro uf='SP'", {}, lambda r: r["CD_SITUACAO"] == "Operante"),
            (
                "CD_SITUACAO",
                "o filtro apenas_operantes=True",
                {"uf": "SP", "apenas_operantes": False},
                lambda r: r["SG_ESTADO"] == "SP",
            ),
        ],
        ids=["sem_uf", "sem_situacao"],
    )
    async def test_filtro_sem_a_coluna_no_catalogo_levanta(
        self, campo, filtro, sem_filtro, esperado
    ):
        oficial = json.loads(ESTACOES_T.read_bytes())
        sem_coluna = [{k: v for k, v in r.items() if k != campo} for r in oficial]
        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=sem_coluna
        ):
            with pytest.raises(ParseError, match=re.escape(filtro)):
                await api.estacoes(uf="SP")
            df = await api.estacoes(**sem_filtro)
        assert sorted(df["codigo"]) == sorted(r["CD_ESTACAO"] for r in oficial if esperado(r))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "inicio", ["08/05/2026", "2026-05-08Tlixo", "2026-05-08T21:00:00.000-03:00lixo"]
    )
    async def test_data_de_operacao_fora_do_formato_iso_e_layout(self, inicio):
        estacao = _mock_estacao(inicio=inicio)
        with (
            patch.object(
                api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[estacao]
            ),
            pytest.raises(ParseError, match="inicio_operacao fora do formato ISO"),
        ):
            await api.estacoes()

    @pytest.mark.asyncio
    async def test_data_de_operacao_inexistente_vira_nat_com_aviso_no_meta(self):
        estacoes = [
            _mock_estacao(),
            _mock_estacao(codigo="A002", inicio="2026-02-30T21:00:00.000-03:00"),
        ]
        with (
            patch.object(
                api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes
            ),
            pytest.warns(UserWarning, match="1 valor"),
        ):
            df, meta = await api.estacoes(return_meta=True)

        assert df["inicio_operacao"].isna().tolist() == [False, True]
        assert any("inicio_operacao viraram NaT" in aviso for aviso in meta.validation_warnings)


class TestEstacoesReturnMeta:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[_mock_estacao()]
        ):
            result = await api.estacoes(as_polars=True)

        assert isinstance(result, pl.DataFrame)


class TestEstacaoAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        mock_data = [_mock_obs(hora="1200 UTC"), _mock_obs(hora="1300 UTC")]
        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            result = await api.estacao("A001", "2024-01-15", "2024-01-15", as_polars=True)
        assert isinstance(result, pl.DataFrame)
