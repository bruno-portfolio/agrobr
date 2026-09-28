"""Testes para agrobr.alt.anp_diesel.api."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.alt.anp_diesel import api
from agrobr.exceptions import InvalidParameterError, ParseError


def _make_precos_xlsx_bytes(**kwargs) -> bytes:
    """Gera XLSX sintetico de precos."""
    rows = kwargs.get(
        "rows",
        [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "6.45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "6.20",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.60",
                "NÚMERO DE POSTOS PESQUISADOS": "80",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "5.95",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.30",
                "NÚMERO DE POSTOS PESQUISADOS": "120",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "22/01/2024",
                "DATA FINAL": "28/01/2024",
                "PREÇO MÉDIO REVENDA": "6.50",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.85",
                "NÚMERO DE POSTOS PESQUISADOS": "155",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/02/2024",
                "DATA FINAL": "07/02/2024",
                "PREÇO MÉDIO REVENDA": "6.25",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.62",
                "NÚMERO DE POSTOS PESQUISADOS": "81",
            },
        ],
    )
    df = pd.DataFrame(rows)
    df["UNIDADE DE MEDIDA"] = "R$/l"
    nivel = kwargs.get("nivel", "uf")
    if nivel == "brasil":
        df = df[df["ESTADO - SIGLA"] == "SP"].drop(columns=["ESTADO - SIGLA"])
    if nivel != "municipio":
        df = df.drop(columns=["MUNICÍPIO"])
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _make_vendas_csv_bytes() -> bytes:
    """Gera CSV sintetico de vendas diesel (formato dados abertos ANP)."""
    rows = [
        {
            "ANO": "2024",
            "MES": "JAN",
            "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
            "UNIDADE DA FEDERACAO": "MATO GROSSO",
            "PRODUTO": "OLEO DIESEL",
            "VENDAS": "500000",
        },
        {
            "ANO": "2024",
            "MES": "JAN",
            "GRANDE REGIAO": "REGIAO SUDESTE",
            "UNIDADE DA FEDERACAO": "SAO PAULO",
            "PRODUTO": "OLEO DIESEL",
            "VENDAS": "800000",
        },
        {
            "ANO": "2024",
            "MES": "FEV",
            "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
            "UNIDADE DA FEDERACAO": "MATO GROSSO",
            "PRODUTO": "OLEO DIESEL S-10",
            "VENDAS": "520000",
        },
    ]
    df = pd.DataFrame(rows)
    return df.to_csv(index=False, sep=";").encode("utf-8")


class TestPrecosDiesel:
    @pytest.mark.asyncio
    async def test_nivel_invalido(self):
        with pytest.raises(ValueError, match="invalido"):
            await api.precos_diesel(nivel="bairro")

    @pytest.mark.asyncio
    async def test_agregacao_invalida(self):
        with pytest.raises(ValueError, match="invalida"):
            await api.precos_diesel(agregacao="diario")

    @pytest.mark.asyncio
    async def test_produto_invalido(self):
        with pytest.raises(ValueError, match="invalido"):
            await api.precos_diesel(produto="GASOLINA")


class TestVendasDiesel:
    @pytest.mark.parametrize("header", [None, "CAMPO DESCONHECIDO"])
    async def test_filtro_uf_rejeita_layout_sem_estado(self, header):
        frame = pd.read_csv(io.BytesIO(_make_vendas_csv_bytes()), sep=";", dtype=str)
        if header is None:
            frame = frame.drop(columns=["UNIDADE DA FEDERACAO"])
        else:
            frame = frame.rename(columns={"UNIDADE DA FEDERACAO": header})
        content = frame.to_csv(index=False, sep=";").encode("utf-8")
        with (
            patch.object(api.client, "fetch_vendas_m3", AsyncMock(return_value=content)),
            pytest.raises(ParseError, match="UF") as caught,
        ):
            await api.vendas_diesel(uf="MT")
        assert caught.value.source == "anp_diesel"

    @pytest.mark.parametrize(
        "column,value",
        [
            ("VENDAS", "invalido"),
            ("VENDAS", "inf"),
            ("VENDAS", "-inf"),
            ("VENDAS", "NaN"),
            ("VENDAS", "1e309"),
            ("ANO", "2024.5"),
            ("ANO", "2024.00000000000000000001"),
            ("ANO", ""),
            ("ANO", "9999"),
            ("MES", "1.9"),
            ("MES", "1.00000000000000000001"),
            ("MES", "0"),
            ("MES", "13"),
            ("MES", "inf"),
            ("MES", "desconhecido"),
        ],
    )
    async def test_linha_invalida_rejeita_resultado_parcial(self, column, value):
        frame = pd.read_csv(io.BytesIO(_make_vendas_csv_bytes()), sep=";", dtype=str)
        frame.loc[2, column] = value
        content = frame.to_csv(index=False, sep=";").encode("utf-8")
        with (
            patch.object(api.client, "fetch_vendas_m3", AsyncMock(return_value=content)),
            pytest.raises(ParseError, match="registro 3") as caught,
        ):
            await api.vendas_diesel(uf="MT", return_meta=True)
        assert caught.value.source == "anp_diesel"

    @pytest.mark.parametrize("missing", ["", "-"])
    async def test_volume_ausente_preserva_observacao(self, missing):
        frame = pd.read_csv(io.BytesIO(_make_vendas_csv_bytes()), sep=";", dtype=str)
        frame.loc[0, "VENDAS"] = missing
        content = frame.to_csv(index=False, sep=";").encode("utf-8")
        with patch.object(api.client, "fetch_vendas_m3", AsyncMock(return_value=content)):
            result, meta = await api.vendas_diesel(uf="MT", return_meta=True)
        assert len(result) == meta.records_count == 2
        assert result.data.dt.strftime("%Y-%m-%d").tolist() == ["2024-01-01", "2024-02-01"]
        assert pd.isna(result.volume_m3.iloc[0])
        assert result.volume_m3.iloc[1] == 520000.0
        assert result.volume_m3.dtype == "float64"

    @pytest.mark.asyncio
    async def test_filtro_data(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            df = await api.vendas_diesel(
                inicio="2024-02-01",
                fim="2024-12-31",
            )
            assert all(df["data"] >= pd.Timestamp("2024-02-01"))


class TestSyncWrapper:
    def test_sync_anp_diesel_accessible(self):
        from agrobr import sync

        alt = sync.alt
        assert hasattr(alt, "anp_diesel")

    def test_sync_has_precos_vendas(self):
        from agrobr import sync

        ad = sync.alt.anp_diesel
        assert hasattr(ad, "precos_diesel")
        assert hasattr(ad, "vendas_diesel")


class TestDateValidation:
    @pytest.mark.parametrize("function_name", ["precos_diesel", "vendas_diesel"])
    @pytest.mark.asyncio
    async def test_formato_invalido(self, function_name):
        function = getattr(api, function_name)

        with pytest.raises(InvalidParameterError, match="YYYY-MM-DD"):
            await function(inicio="01/01/2026")

    @pytest.mark.parametrize("function_name", ["precos_diesel", "vendas_diesel"])
    @pytest.mark.asyncio
    async def test_periodo_invertido(self, function_name):
        function = getattr(api, function_name)

        with pytest.raises(InvalidParameterError, match="inicio deve ser anterior"):
            await function(inicio="2026-02-01", fim="2026-01-01")


class TestPrecosDieselAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_resource", new_callable=AsyncMock) as mock:
            mock.return_value = api.client.PrecosResource(
                xlsx,
                "https://www.gov.br/anp/data.xlsx",
                "https://www.gov.br/anp/data.xlsx",
                datetime(2026, 9, 8, tzinfo=UTC),
            )
            result = await api.precos_diesel(nivel="uf", as_polars=True)
        assert isinstance(result, pl.DataFrame)
